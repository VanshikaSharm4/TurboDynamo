package lsm

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VanshikaSharm4/TurboDynamo.git/internal/metrics"
	storage "github.com/VanshikaSharm4/TurboDynamo.git/internal/store"
)

type LSMEngine struct {
	mu           sync.RWMutex
	active       *MemTable
	immutable    []*MemTable // queue of memtables waiting to flush
	wal          *storage.WAL
	levels       [][]*SSTableReader // kept for backward compat, but used as tiers
	version      atomic.Uint64
	dir          string
	closed       atomic.Bool
	flushCh      chan struct{}      // async flush signal
	directFlushCh chan struct{}    // sync flush request (blocks caller)
	compCh      chan struct{}
	flushCond   *sync.Cond // signaled when a flush completes, used for backpressure
	wg          sync.WaitGroup
	config      LSMConfig
	cache       *KeyCache
	nodeID      string // node identifier for vector clock
	writeCounter uint32 // atomic counter to reduce IsFull check frequency
	flushing   atomic.Bool // true when flushLoop is processing (prevents race with flushMemTable)
	compactionStrategy string // "leveled" or "tiered"
}

type LSMConfig struct {
	MemTableSize          int64         // soft limit for memtable
	MaxMemtableBytes     int64         // total memory for all memtables
	WALSyncInterval     time.Duration // background sync interval (0 = sync every write)
	WALCheckpointBytes  int64         // bytes written before checkpoint sync (0 = use default)
	WALMaxBufferedBytes int64         // max buffered before forced flush (0 = use default)
	CompactionThreshold int           // SSTables per level to trigger compaction
	L0SizeThreshold     int64         // hard limit for memtable
	BloomFPRate         float64
	LevelRatio          float64 // size ratio between levels
	KeyCacheSize        int     // number of entries in key cache
	NodeID              string  // node identifier for vector clock
	MaxImmutable       int     // max immutable memtables in queue (prevent memory leak)
	Metrics             *metrics.Metrics // optional metrics recorder
	CompactionStrategy  string  // "leveled" or "tiered" (default: "tiered" for write-heavy)
}

const (
	DefaultKeyCacheSize    = 1000000           // 1M entries (increased from 10K)
	DefaultLevelRatio      = 10.0              // 10x ratio (fewer levels = faster)
	DefaultL0SizeThreshold = 256 * 1024 * 1024 // 256MB (2x memtable)
	DefaultMaxImmutable   = 10                 // Max immutable memtables (100MB each = 1GB max)
)

func (e *LSMEngine) PutEntry(entry storage.Entry) error {
	e.active.Put(entry)
	return nil
}

func (e *LSMEngine) SetVersion(version uint64) {
	e.version.Store(version)
}

func NewLSMEngine(dir string) (*LSMEngine, error) {
	return NewLSMEngineWithConfig(dir, LSMConfig{})
}

func NewLSMEngineWithConfig(dir string, cfg LSMConfig) (*LSMEngine, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	// Apply defaults optimized for high throughput
	if cfg.MemTableSize <= 0 {
		cfg.MemTableSize = 256 * 1024 * 1024 // 256MB (increased from 64MB)
	}
	if cfg.MaxMemtableBytes <= 0 {
		cfg.MaxMemtableBytes = 1024 * 1024 * 1024 // 1GB (4x memtable)
	}
	if cfg.CompactionThreshold <= 0 {
		cfg.CompactionThreshold = 8 // increased from 4
	}
	if cfg.L0SizeThreshold <= 0 {
		cfg.L0SizeThreshold = DefaultL0SizeThreshold // 256MB
	}
	if cfg.BloomFPRate <= 0 {
		cfg.BloomFPRate = 0.01
	}
	if cfg.LevelRatio <= 0 {
		cfg.LevelRatio = DefaultLevelRatio // 10 (fewer levels = faster)
	}
	if cfg.KeyCacheSize <= 0 {
		cfg.KeyCacheSize = DefaultKeyCacheSize // 1M entries
	}
	if cfg.CompactionStrategy == "" {
		cfg.CompactionStrategy = "tiered" // default to tiered for write-heavy workloads
	}

	wal, err := storage.OpenWALWithConfig(filepath.Join(dir, "wal.log"), storage.WALConfig{
		SyncInterval:     cfg.WALSyncInterval,
		CheckpointBytes:  cfg.WALCheckpointBytes,
		MaxBufferedBytes: cfg.WALMaxBufferedBytes,
	})
	if err != nil {
		return nil, err
	}

e := &LSMEngine{
		active:        NewMemTable(cfg.MemTableSize),
		wal:         wal,
		dir:         dir,
		flushCh:     make(chan struct{}, 1),
		directFlushCh: make(chan struct{}, 1),
		compCh:     make(chan struct{}, 1),
		config:      cfg,
		cache:      newKeyCache(cfg.KeyCacheSize),
		nodeID:     cfg.NodeID,
		compactionStrategy: cfg.CompactionStrategy,
	}
	e.flushCond = sync.NewCond(&e.mu)

	if err := e.loadSSTables(); err != nil {
		return nil, err
	}

	// Load persisted version counter before WAL replay
	if err := e.loadVersion(); err != nil {
		return nil, err
	}

	if err := e.replayWAL(); err != nil {
		return nil, err
	}

	// Flush any replayed WAL entries to SSTables and reset the WAL.
	// This ensures clean state and prevents WAL from growing across restarts.
	if err := e.flushMemTable(); err != nil {
		return nil, err
	}

	// Persist the version counter so it survives WAL reset
	if err := e.saveVersion(); err != nil {
		return nil, err
	}

	e.wg.Add(2)
	go e.flushLoop()
	go e.compactLoop()

	return e, nil
}

func (e *LSMEngine) Put(key string, value []byte) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	if len(key) > storage.MaxKeyLen {
		return storage.ErrKeyTooLong
	}
	if len(value) > storage.MaxValueLen {
		return storage.ErrValueTooLarge
	}

	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMPut(time.Since(start))
		}
	}()

	entry := storage.Entry{
		Key:         key,
		Value:       value,
		Version:     e.version.Add(1),
		TimeStamp:   time.Now(),
		VectorClock: storage.NewVectorClock().Increment(e.nodeID),
	}
	//
	if err := e.wal.Append(entry); err != nil {
		return err
	}

	e.mu.Lock()
	// Rotate when active is full OR when immutable queue is too full
	if e.active.IsFull() {
		// Limit immutable queue to prevent memory leak
		maxImm := e.config.MaxImmutable
		if maxImm <= 0 {
			maxImm = DefaultMaxImmutable
		}
		// Drop oldest immutable if at capacity
		if len(e.immutable) >= maxImm {
			if len(e.immutable) > 0 {
				// Just discard oldest - data is in WAL for recovery
				e.immutable = e.immutable[1:]
			}
		}
		e.immutable = append(e.immutable, e.active)
		e.active = NewMemTable(e.config.MemTableSize)
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	e.active.Put(entry)
	e.cache.Invalidate(key)
	e.mu.Unlock()

	return nil
}

// BatchPut writes multiple key-value pairs in a single WAL lock + single memtable lock.
// This is significantly faster than calling Put in a loop for batch replication workloads.
func (e *LSMEngine) BatchPut(pairs []storage.Entry) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMPut(time.Since(start))
		}
	}()

	// Initialize entry metadata before writing
	now := time.Now()
	for i := range pairs {
		if len(pairs[i].Key) > storage.MaxKeyLen {
			return storage.ErrKeyTooLong
		}
		if len(pairs[i].Value) > storage.MaxValueLen {
			return storage.ErrValueTooLarge
		}
		if pairs[i].Version == 0 {
			pairs[i].Version = e.version.Add(1)
		}
		if pairs[i].TimeStamp.IsZero() {
			pairs[i].TimeStamp = now
		}
		if pairs[i].VectorClock == nil {
			pairs[i].VectorClock = storage.NewVectorClock().Increment(e.nodeID)
		}
	}

	// Single WAL write for the whole batch
	if err := e.wal.BatchAppend(pairs); err != nil {
		return err
	}

	e.mu.Lock()
	// Rotate when active is full or immutable queue is full
	if e.active.IsFull() {
		// Limit immutable queue
		maxImm := e.config.MaxImmutable
		if maxImm <= 0 {
			maxImm = DefaultMaxImmutable
		}
		if len(e.immutable) >= maxImm {
			if len(e.immutable) > 0 {
				e.immutable = e.immutable[1:]
			}
		}
		e.immutable = append(e.immutable, e.active)
		e.active = NewMemTable(e.config.MemTableSize)
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	for i := range pairs {
		e.active.Put(pairs[i])
		e.cache.Invalidate(pairs[i].Key)
	}
	e.mu.Unlock()

	return nil
}

func (e *LSMEngine) PutWithVectorClock(key string, value []byte, vc storage.VectorClock) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	if len(key) > storage.MaxKeyLen {
		return storage.ErrKeyTooLong
	}
	if len(value) > storage.MaxValueLen {
		return storage.ErrValueTooLarge
	}

	entry := storage.Entry{
		Key:         key,
		Value:       value,
		Version:     e.version.Add(1),
		TimeStamp:   time.Now(),
		VectorClock: vc,
	}

	if err := e.wal.Append(entry); err != nil {
		return err
	}

	e.mu.Lock()
	// Limit immutable queue
	if e.active.IsFull() {
		maxImm := e.config.MaxImmutable
		if maxImm <= 0 {
			maxImm = DefaultMaxImmutable
		}
		if len(e.immutable) >= maxImm {
			if len(e.immutable) > 0 {
				e.immutable = e.immutable[1:]
			}
		}
		e.immutable = append(e.immutable, e.active)
		e.active = NewMemTable(e.config.MemTableSize)
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	e.active.Put(entry)
	e.cache.Invalidate(key)
	e.mu.Unlock()

	return nil
}

// PutAsync writes to memtable only, skips WAL - for fire-and-forget replication
// Data may be lost on crash but much faster for W=1 eventual consistency
func (e *LSMEngine) PutAsync(key string, value []byte) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	if len(key) > storage.MaxKeyLen {
		return storage.ErrKeyTooLong
	}
	if len(value) > storage.MaxValueLen {
		return storage.ErrValueTooLarge
	}

	entry := storage.Entry{
		Key:         key,
		Value:       value,
		Version:     e.version.Add(1),
		TimeStamp:   time.Now(),
		VectorClock: storage.NewVectorClock().Increment(e.nodeID),
	}

	e.mu.Lock()
	if e.active.IsFull() {
		maxImm := e.config.MaxImmutable
		if maxImm <= 0 {
			maxImm = DefaultMaxImmutable
		}
		if len(e.immutable) >= maxImm {
			if len(e.immutable) > 0 {
				e.immutable = e.immutable[1:]
			}
		}
		e.immutable = append(e.immutable, e.active)
		e.active = NewMemTable(e.config.MemTableSize)
		select {
		case e.flushCh <- struct{}{}:
		default:
		}
	}

	e.active.Put(entry)
	e.cache.Invalidate(key)
	e.mu.Unlock()

	return nil
}

// BatchPutAsync writes multiple entries without WAL - fire-and-forget mode
func (e *LSMEngine) BatchPutAsync(pairs []storage.Entry) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	// Initialize entry metadata
	now := time.Now()
	for i := range pairs {
		if pairs[i].Version == 0 {
			pairs[i].Version = e.version.Add(1)
		}
		if pairs[i].TimeStamp.IsZero() {
			pairs[i].TimeStamp = now
		}
		if pairs[i].VectorClock == nil {
			pairs[i].VectorClock = storage.NewVectorClock().Increment(e.nodeID)
		}
	}

	e.mu.Lock()
	for _, entry := range pairs {
		if e.active.IsFull() {
			maxImm := e.config.MaxImmutable
			if maxImm <= 0 {
				maxImm = DefaultMaxImmutable
			}
			if len(e.immutable) >= maxImm {
				if len(e.immutable) > 0 {
					e.immutable = e.immutable[1:]
				}
			}
			e.immutable = append(e.immutable, e.active)
			e.active = NewMemTable(e.config.MemTableSize)
			select {
			case e.flushCh <- struct{}{}:
			default:
			}
		}
		e.active.Put(entry)
		e.cache.Invalidate(entry.Key)
	}
	e.mu.Unlock()

	return nil
}

func (e *LSMEngine) Get(key string) (storage.Entry, error) {
	if e.closed.Load() {
		return storage.Entry{}, storage.ErrEngineClosed
	}

	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMGet("total", time.Since(start))
		}
	}()

	// 1. Check active memtable (lock-free - memtable is concurrent-safe)
	memStart := time.Now()
	if entry, ok := e.active.Get(key); ok {
		if entry.Tombstone {
			e.cache.Put(key, storage.Entry{}, false)
			if e.config.Metrics != nil {
				e.config.Metrics.RecordLSMGet("memtable", time.Since(memStart))
			}
			return storage.Entry{}, storage.ErrKeyNotFound
		}
		e.cache.Put(key, entry, true)
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMGet("memtable", time.Since(memStart))
		}
		return entry, nil
	}

	// 2. Check key cache
	cacheStart := time.Now()
	if item, ok := e.cache.Get(key); ok {
		if item.found {
			if e.config.Metrics != nil {
				e.config.Metrics.RecordLSMGet("cache", time.Since(cacheStart))
			}
			return item.entry, nil
		}
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMGet("cache", time.Since(cacheStart))
		}
		return storage.Entry{}, storage.ErrKeyNotFound
	}

	// 3. Check immutable memtables (safe iteration)
	e.mu.RLock()
	immunes := make([]*MemTable, len(e.immutable))
	copy(immunes, e.immutable)
	e.mu.RUnlock()

	for i := len(immunes) - 1; i >= 0; i-- {
		if entry, ok := immunes[i].Get(key); ok {
			if entry.Tombstone {
				e.cache.Put(key, storage.Entry{}, false)
				if e.config.Metrics != nil {
					e.config.Metrics.RecordLSMGet("memtable", time.Since(memStart))
				}
				return storage.Entry{}, storage.ErrKeyNotFound
			}
			e.cache.Put(key, entry, true)
			if e.config.Metrics != nil {
				e.config.Metrics.RecordLSMGet("memtable", time.Since(memStart))
			}
			return entry, nil
		}
	}

	// 4. Take brief snapshot of levels for SSTable iteration
	e.mu.RLock()
	levelSnapshot := make([][]*SSTableReader, len(e.levels))
	copy(levelSnapshot, e.levels)
	e.mu.RUnlock()

	// 5. Check SSTables (lock-free iteration)
	sstStart := time.Now()
	for _, level := range levelSnapshot {
		for _, sst := range level {
			if !sst.filter.MightContain([]byte(key)) {
				continue
			}

			entry, err := sst.Get(key)
			if err == storage.ErrKeyNotFound {
				continue
			}
			if err != nil {
				if e.config.Metrics != nil {
					e.config.Metrics.RecordLSMGet("sstable", time.Since(sstStart))
				}
				return storage.Entry{}, err
			}

			if entry.Tombstone {
				e.cache.Put(key, storage.Entry{}, false)
				if e.config.Metrics != nil {
					e.config.Metrics.RecordLSMGet("sstable", time.Since(sstStart))
				}
				return storage.Entry{}, storage.ErrKeyNotFound
			}

			e.cache.Put(key, entry, true)
			if e.config.Metrics != nil {
				e.config.Metrics.RecordLSMGet("sstable", time.Since(sstStart))
			}
			return entry, nil
		}
	}

	e.cache.Put(key, storage.Entry{}, false)
	if e.config.Metrics != nil {
		e.config.Metrics.RecordLSMGet("sstable", time.Since(sstStart))
	}
	return storage.Entry{}, storage.ErrKeyNotFound
}

// MultiGet retrieves multiple keys in a single pass, optimizing for throughput
// by reducing locking overhead and batching Bloom filter checks.
// Lock-free reads from memtables; takes brief lock snapshot of levels for SSTable iteration.
func (e *LSMEngine) MultiGet(keys []string) (map[string]storage.Entry, error) {
	if e.closed.Load() {
		return nil, storage.ErrEngineClosed
	}

	results := make(map[string]storage.Entry, len(keys))
	pendingKeys := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		pendingKeys[k] = struct{}{}
	}

	// 1. Check active memtable (lock-free)
	for k := range pendingKeys {
		if entry, ok := e.active.Get(k); ok {
			if !entry.Tombstone {
				results[k] = entry
			}
			delete(pendingKeys, k)
		}
	}
	if len(pendingKeys) == 0 {
		return results, nil
	}

	// 2. Check immutable memtables (safe iteration)
	e.mu.RLock()
	immunes := make([]*MemTable, len(e.immutable))
	copy(immunes, e.immutable)
	e.mu.RUnlock()

	for i := len(immunes) - 1; i >= 0; i-- {
		mem := immunes[i]
		for k := range pendingKeys {
			if entry, ok := mem.Get(k); ok {
				if !entry.Tombstone {
					results[k] = entry
				}
				delete(pendingKeys, k)
			}
		}
		if len(pendingKeys) == 0 {
			return results, nil
		}
	}

	// 3. Take brief snapshot of levels for SSTable iteration
	e.mu.RLock()
	levelSnapshot := make([][]*SSTableReader, len(e.levels))
	copy(levelSnapshot, e.levels)
	e.mu.RUnlock()

	// 4. Check SSTables level by level (lock-free iteration)
	keysToSearch := make([]string, 0, len(pendingKeys))
	for k := range pendingKeys {
		keysToSearch = append(keysToSearch, k)
	}

	for _, level := range levelSnapshot {
		for _, sst := range level {
			found, missing := sst.MultiGet(keysToSearch)
			for k, entry := range found {
				if !entry.Tombstone {
					results[k] = entry
				}
				delete(pendingKeys, k)
			}

			if len(pendingKeys) == 0 {
				return results, nil
			}
			keysToSearch = missing
		}
	}

	return results, nil
}

func (e *LSMEngine) processDirectFlush() {
	e.mu.Lock()
	e.flushing.Store(true)
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		e.flushing.Store(false)
		e.mu.Unlock()
	}()

	for {
		e.mu.Lock()
		if len(e.immutable) == 0 {
			e.mu.Unlock()
			break
		}
		mem := e.immutable[0]
		e.immutable = e.immutable[1:]
		e.mu.Unlock()

		entries := mem.Entries()
		if len(entries) == 0 {
			continue
		}

		sstPath := filepath.Join(e.dir, fmt.Sprintf("L0_%d.sst", time.Now().UnixNano()))

		writer, err := NewSSTableWriter(sstPath, len(entries), e.config.BloomFPRate)
		if err != nil {
			slog.Error("direct flush: sstable writer error", "error", err)
			e.mu.Lock()
			e.immutable = append([]*MemTable{mem}, e.immutable...)
			e.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		writeErr := false
		for _, entry := range entries {
			if err := writer.WriteEntry(entry); err != nil {
				slog.Error("direct flush: write entry error", "error", err)
				writeErr = true
				break
			}
		}
		if writeErr {
			e.mu.Lock()
			e.immutable = append([]*MemTable{mem}, e.immutable...)
			e.mu.Unlock()
			os.Remove(sstPath)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err := writer.Finalize(); err != nil {
			slog.Error("direct flush: finalize error", "error", err)
			e.mu.Lock()
			e.immutable = append([]*MemTable{mem}, e.immutable...)
			e.mu.Unlock()
			os.Remove(sstPath)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		reader, err := OpenSSTable(sstPath)
		if err != nil {
			slog.Error("direct flush: open sstable error", "error", err)
			e.mu.Lock()
			e.immutable = append([]*MemTable{mem}, e.immutable...)
			e.mu.Unlock()
			os.Remove(sstPath)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		e.mu.Lock()
		if len(e.levels) == 0 {
			e.levels = append(e.levels, nil)
		}
		e.levels[0] = append([]*SSTableReader{reader}, e.levels[0]...)
		e.mu.Unlock()

		e.flushCond.Broadcast()
	}

	e.flushCond.Broadcast()
}

func (e *LSMEngine) flushLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.flushCh:
		case <-e.directFlushCh:
			e.processDirectFlush()
		case <-ticker.C:
		}

		if e.closed.Load() {
			return
		}

		// Drain all pending flushes then go back to wait
		for {
			e.mu.Lock()
			if len(e.immutable) == 0 {
				e.mu.Unlock()
				break
			}
			mem := e.immutable[0]
			e.immutable = e.immutable[1:]
			e.mu.Unlock()

			entries := mem.Entries()
			if len(entries) == 0 {
				continue
			}

			sstPath := filepath.Join(e.dir, fmt.Sprintf("L0_%d.sst", time.Now().UnixNano()))

			writer, err := NewSSTableWriter(sstPath, len(entries), e.config.BloomFPRate)
			if err != nil {
				slog.Error("sstable writer error", "error", err)
				e.mu.Lock()
				e.immutable = append([]*MemTable{mem}, e.immutable...)
				e.mu.Unlock()
				time.Sleep(100 * time.Millisecond)
				continue
			}

			writeErr := false
			for _, entry := range entries {
				if err := writer.WriteEntry(entry); err != nil {
					slog.Error("write entry error", "error", err)
					writeErr = true
					break
				}
			}
			if writeErr {
				e.mu.Lock()
				e.immutable = append([]*MemTable{mem}, e.immutable...)
				e.mu.Unlock()
				os.Remove(sstPath)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if err := writer.Finalize(); err != nil {
				slog.Error("finalize error", "error", err)
				e.mu.Lock()
				e.immutable = append([]*MemTable{mem}, e.immutable...)
				e.mu.Unlock()
				os.Remove(sstPath)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			reader, err := OpenSSTable(sstPath)
			if err != nil {
				slog.Error("open sstable error", "error", err)
				e.mu.Lock()
				e.immutable = append([]*MemTable{mem}, e.immutable...)
				e.mu.Unlock()
				os.Remove(sstPath)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			e.mu.Lock()
			if len(e.levels) == 0 {
				e.levels = append(e.levels, nil)
			}
			e.levels[0] = append([]*SSTableReader{reader}, e.levels[0]...)
			e.mu.Unlock()

			e.flushCond.Broadcast()
		}

		// Signal any waiters that all flushes are complete
		e.flushCond.Broadcast()
	}
}

// maxFilesForLevel returns the max SSTables allowed at a given level.
// Creates a pyramid using level_ratio: L0=4, L1=4*ratio, L2=4*ratio^2, ...
func (e *LSMEngine) maxFilesForLevel(level int) int {
	ratio := e.config.LevelRatio
	result := float64(e.config.CompactionThreshold)
	for range level {
		result *= ratio
	}
	return int(result)
}

func (e *LSMEngine) compactLoop() {
	defer e.wg.Done()

	for range e.compCh {
		if e.closed.Load() {
			return
		}

		if e.compactionStrategy == "tiered" {
			e.compactTiered()
		} else {
			e.compactLeveled()
		}
	}
}

// compactLeveled is the original leveled compaction (kept for backward compat)
func (e *LSMEngine) compactLeveled() {
	// Collect levels that need compaction
	e.mu.RLock()
	var levelsToCompact []int
	for level := 0; level < len(e.levels); level++ {
		if len(e.levels[level]) >= e.maxFilesForLevel(level) {
			levelsToCompact = append(levelsToCompact, level)
		} else {
			break
		}
	}
	e.mu.RUnlock()

	if len(levelsToCompact) == 0 {
		return
	}

	// Compact levels sequentially to prevent data corruption.
	for _, level := range levelsToCompact {
		e.compactLevel(level)
	}
}

// tierSize returns the size category for an SSTable
func (e *LSMEngine) tierSize(sst *SSTableReader) int64 {
	info, err := os.Stat(sst.path)
	if err != nil {
		return 0
	}
	size := info.Size()
	// Tier boundaries: 1MB, 10MB, 100MB, 1GB, etc.
	// Group by powers of 10
	if size < 1024*1024 {
		return 0 // <1MB
	}
	if size < 10*1024*1024 {
		return 1 // 1-10MB
	}
	if size < 100*1024*1024 {
		return 2 // 10-100MB
	}
	if size < 1024*1024*1024 {
		return 3 // 100MB-1GB
	}
	return 4 // >1GB
}

// compactTiered implements size-tiered compaction (STCS) like Cassandra
// Groups SSTables by size, merges when threshold reached
func (e *LSMEngine) compactTiered() {
	e.mu.RLock()
	// Flatten all SSTables from all levels
	var allSSTs []*SSTableReader
	for _, level := range e.levels {
		allSSTs = append(allSSTs, level...)
	}
	e.mu.RUnlock()

	if len(allSSTs) == 0 {
		return
	}

	// Group by size tier
	type tierGroup struct {
		tier    int64
		ssts    []*SSTableReader
	}
	tierMap := make(map[int64][]*SSTableReader)
	for _, sst := range allSSTs {
		tier := e.tierSize(sst)
		tierMap[tier] = append(tierMap[tier], sst)
	}

	// Find the smallest tier that has enough files to compact
	var groups []tierGroup
	for tier, ssts := range tierMap {
		groups = append(groups, tierGroup{tier: tier, ssts: ssts})
	}
	// Sort by tier (smallest first)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].tier < groups[j].tier
	})

	// Find first tier with enough files
	var toCompact *tierGroup
	for i := range groups {
		if len(groups[i].ssts) >= e.config.CompactionThreshold {
			toCompact = &groups[i]
			break
		}
	}

	if toCompact == nil {
		return
	}

	slog.Info("[TIERED-COMPACTION] merging", "tier", toCompact.tier, "count", len(toCompact.ssts))

	// Collect all entries from all SSTables using merge sort
	merged := mergeSSTables(toCompact.ssts)

	// Deduplicate: keep only the latest version of each key
	const tombstoneTTL = 24 * time.Hour
	deduped := deduplicateEntries(merged, tombstoneTTL)

	if len(deduped) == 0 {
		// All entries were old tombstones — atomically verify and clean up
		e.mu.Lock()
		compactedPaths := make(map[string]bool)
		for _, sst := range toCompact.ssts {
			compactedPaths[sst.path] = true
		}
		for level := range e.levels {
			for _, sst := range e.levels[level] {
				if compactedPaths[sst.path] {
					delete(compactedPaths, sst.path)
				}
			}
		}
		if len(compactedPaths) > 0 {
			e.mu.Unlock()
			slog.Info("[TIERED-COMPACTION] aborted, source SSTables already removed", "tier", toCompact.tier)
			return
		}
		compactedPaths = make(map[string]bool)
		for _, sst := range toCompact.ssts {
			compactedPaths[sst.path] = true
		}
		for level := range e.levels {
			var kept []*SSTableReader
			for _, sst := range e.levels[level] {
				if !compactedPaths[sst.path] {
					kept = append(kept, sst)
				}
			}
			e.levels[level] = kept
		}
		e.mu.Unlock()
		for _, sst := range toCompact.ssts {
			sst.Close()
			os.Remove(sst.path)
		}
		slog.Info("[TIERED-COMPACTION] all tombstones expired, cleaned up", "tier", toCompact.tier)
		return
	}

	// Write merged result as new SSTable in next tier
	nextTier := toCompact.tier + 1
	sstPath := filepath.Join(e.dir,
		fmt.Sprintf("L%d_%d.sst", nextTier, time.Now().UnixNano()))
	writer, err := NewSSTableWriter(sstPath, len(deduped), e.config.BloomFPRate)
	if err != nil {
		slog.Error("[TIERED-COMPACTION] error creating writer", "error", err)
		return
	}

	for _, entry := range deduped {
		if err := writer.WriteEntry(entry); err != nil {
			slog.Error("[TIERED-COMPACTION] error writing entry", "error", err)
			os.Remove(sstPath)
			return
		}
	}

	if err := writer.Finalize(); err != nil {
		slog.Error("[TIERED-COMPACTION] error finalizing", "error", err)
		os.Remove(sstPath)
		return
	}

	// Open new SSTable
	reader, err := OpenSSTable(sstPath)
	if err != nil {
		slog.Error("[TIERED-COMPACTION] error opening new SSTable", "error", err)
		os.Remove(sstPath)
		return
	}

	// ATOMIC SWAP: verify source SSTables still exist, then remove and install new one
	e.mu.Lock()
	compactedPaths := make(map[string]bool)
	for _, sst := range toCompact.ssts {
		compactedPaths[sst.path] = true
	}
	// Verify all source SSTables are still present in levels
	for level := range e.levels {
		for _, sst := range e.levels[level] {
			if compactedPaths[sst.path] {
				delete(compactedPaths, sst.path)
			}
		}
	}
	if len(compactedPaths) > 0 {
		// Some source SSTables were already removed by concurrent compaction/flush
		e.mu.Unlock()
		reader.Close()
		os.Remove(sstPath)
		slog.Info("[TIERED-COMPACTION] aborted, source SSTables already removed")
		return
	}
	// Reset and rebuild compactedPaths for actual removal
	compactedPaths = make(map[string]bool)
	for _, sst := range toCompact.ssts {
		compactedPaths[sst.path] = true
	}
	for level := range e.levels {
		var kept []*SSTableReader
		for _, sst := range e.levels[level] {
			if !compactedPaths[sst.path] {
				kept = append(kept, sst)
			}
		}
		e.levels[level] = kept
	}
	// Add new SSTable to next level (create if needed)
	for len(e.levels) <= int(nextTier) {
		e.levels = append(e.levels, nil)
	}
	e.levels[nextTier] = append(e.levels[nextTier], reader)
	e.mu.Unlock()

	// Now safe to close and delete old files
	for _, sst := range toCompact.ssts {
		sst.Close()
		os.Remove(sst.path)
	}

	slog.Info("[TIERED-COMPACTION] done", "src_tier", toCompact.tier, "dst_tier", nextTier, "entries", len(deduped))
}

func (e *LSMEngine) compactLevel(level int) {
	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMCompaction(strconv.Itoa(level), time.Since(start))
		}
	}()

	e.mu.Lock()
	if level >= len(e.levels) || len(e.levels[level]) < e.maxFilesForLevel(level) {
		e.mu.Unlock()
		return
	}

	// Take a snapshot of SSTables to compact, but DO NOT remove them yet.
	// They must remain visible to Get() during the entire merge process.
	toMerge := make([]*SSTableReader, len(e.levels[level]))
	copy(toMerge, e.levels[level])
	e.mu.Unlock()

	slog.Info("[COMPACTION] merging", "level", level, "count", len(toMerge))

	// Collect all entries from all SSTables using merge sort
	merged := mergeSSTables(toMerge)

	// Deduplicate: keep only the latest version of each key
	// Remove old tombstones
	const tombstoneTTL = 24 * time.Hour
	deduped := deduplicateEntries(merged, tombstoneTTL)

	if len(deduped) == 0 {
		// All entries were old tombstones — atomically remove, then delete files
		e.mu.Lock()
		e.levels[level] = nil
		e.mu.Unlock()
		for _, sst := range toMerge {
			sst.Close()
			os.Remove(sst.path)
		}
		slog.Info("[COMPACTION] all tombstones expired, cleaned up", "level", level)
		return
	}

	// Write merged result as new SSTable
	nextLevel := level + 1
	sstPath := filepath.Join(e.dir,
		fmt.Sprintf("L%d_%d.sst", nextLevel, time.Now().UnixNano()))
	writer, err := NewSSTableWriter(sstPath, len(deduped), e.config.BloomFPRate)
	if err != nil {
		slog.Error("[COMPACTION] error creating writer", "error", err)
		return
	}

	for _, entry := range deduped {
		if err := writer.WriteEntry(entry); err != nil {
			slog.Error("[COMPACTION] error writing entry", "error", err)
			os.Remove(sstPath)
			return
		}
	}

	if err := writer.Finalize(); err != nil {
		slog.Error("[COMPACTION] error finalizing", "error", err)
		os.Remove(sstPath)
		return
	}

	// Open new SSTable
	reader, err := OpenSSTable(sstPath)
	if err != nil {
		slog.Error("[COMPACTION] error opening new SSTable", "error", err)
		os.Remove(sstPath)
		return
	}

	// ATOMIC SWAP: remove old SSTables and install new one in a single lock
	e.mu.Lock()

	// Build a set of SSTables that were in the original compaction snapshot
	oldSSTables := make(map[*SSTableReader]bool)
	for _, sst := range toMerge {
		oldSSTables[sst] = true
	}

	// Keep any SSTables that were added to this level AFTER the compaction snapshot
	// These are the "new" SSTables from flushes that happened during compaction
	var newSSTables []*SSTableReader
	if level < len(e.levels) && e.levels[level] != nil {
		for _, sst := range e.levels[level] {
			if !oldSSTables[sst] {
				newSSTables = append(newSSTables, sst)
			}
		}
	}

	e.levels[level] = nil // remove old SSTables from source level
	for len(e.levels) <= nextLevel {
		e.levels = append(e.levels, nil)
	}
	e.levels[nextLevel] = append(e.levels[nextLevel], reader)
	// Prepend new SSTables back to the level so they're not lost
	if len(newSSTables) > 0 {
		e.levels[level] = append(newSSTables, e.levels[level]...)
	}
	e.mu.Unlock()

	// Now safe to close and delete old files (no longer referenced by e.levels)
	for _, sst := range toMerge {
		sst.Close()
		os.Remove(sst.path)
	}

	slog.Info("[COMPACTION] done", "src_level", level, "dst_level", nextLevel, "entries", len(deduped))
}

func (e *LSMEngine) Flush() error {
	return e.flushMemTable()
}

func (e *LSMEngine) flushMemTable() error {
	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMFlush(time.Since(start))
		}
	}()

	e.mu.Lock()
	if e.active.Len() > 0 {
		maxImm := e.config.MaxImmutable
		if maxImm <= 0 {
			maxImm = DefaultMaxImmutable
		}
		if len(e.immutable) >= maxImm {
			if len(e.immutable) > 0 {
				e.immutable = e.immutable[1:]
			}
		}
		e.immutable = append(e.immutable, e.active)
		e.active = NewMemTable(e.config.MemTableSize)
	}
	e.mu.Unlock()

	select {
	case e.directFlushCh <- struct{}{}:
		for {
			e.mu.Lock()
			drained := len(e.immutable) == 0
			e.mu.Unlock()
			if drained {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	default:
		for {
			e.mu.Lock()
			drained := len(e.immutable) == 0
			e.mu.Unlock()
			if drained {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if err := e.wal.Sync(); err != nil {
		return err
	}

	if !e.closed.Load() {
		e.mu.RLock()
		totalSSTs := 0
		for _, level := range e.levels {
			totalSSTs += len(level)
		}
		e.mu.RUnlock()
		
		shouldCompact := false
		if e.compactionStrategy == "tiered" {
			shouldCompact = totalSSTs >= e.config.CompactionThreshold
		} else {
			l0Count := 0
			if len(e.levels) > 0 {
				l0Count = len(e.levels[0])
			}
			shouldCompact = l0Count >= e.maxFilesForLevel(0)
		}
		
		if shouldCompact {
			select {
			case e.compCh <- struct{}{}:
			default:
			}
		}
	}

	return nil
}

func (e *LSMEngine) loadSSTables() error {
	files, err := os.ReadDir(e.dir)
	if err != nil {
		return err
	}

	levelMap := make(map[int][]*SSTableReader)

	for _, f := range files {
		if f.IsDir() {
			continue
		}

		name := f.Name()

		if filepath.Ext(name) != ".sst" {
			continue
		}

		var level int
		_, err := fmt.Sscanf(name, "L%d_", &level)
		if err != nil {
			continue
		}

		path := filepath.Join(e.dir, name)

		reader, err := OpenSSTable(path)
		if err != nil {
			return err
		}

		levelMap[level] = append(levelMap[level], reader)
	}

	if len(levelMap) == 0 {
		return nil
	}

	maxLevel := 0
	for lvl := range levelMap {
		if lvl > maxLevel {
			maxLevel = lvl
		}
	}

	e.levels = make([][]*SSTableReader, maxLevel+1)

	for lvl, readers := range levelMap {
		sort.Slice(readers, func(i, j int) bool {
			return readers[i].path > readers[j].path
		})
		e.levels[lvl] = readers
	}

	return nil
}

func (e *LSMEngine) replayWAL() error {

	return e.wal.Replay(e)
}

func (e *LSMEngine) Delete(key string) error {
	if e.closed.Load() {
		return storage.ErrEngineClosed
	}

	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMPut(time.Since(start))
		}
	}()

	entry := storage.Entry{
		Key:         key,
		Value:       nil,
		Version:     e.version.Add(1),
		TimeStamp:   time.Now(),
		Tombstone:   true,
		VectorClock: storage.NewVectorClock().Increment(e.nodeID),
	}

	if err := e.wal.Append(entry); err != nil {
		return err
	}

	e.mu.Lock()
	e.active.Put(entry)
	e.cache.Invalidate(key)
	e.mu.Unlock()

	return nil
}

func (e *LSMEngine) Scan(prefix string) ([]storage.Entry, error) {
	if e.closed.Load() {
		return nil, storage.ErrEngineClosed
	}

	start := time.Now()
	defer func() {
		if e.config.Metrics != nil {
			e.config.Metrics.RecordLSMScan(time.Since(start))
		}
	}()

	e.mu.RLock()
	levelSnapshot := make([][]*SSTableReader, len(e.levels))
	for i, level := range e.levels {
		levelSnapshot[i] = make([]*SSTableReader, len(level))
		copy(levelSnapshot[i], level)
	}
	immutableSnapshot := make([]*MemTable, len(e.immutable))
	copy(immutableSnapshot, e.immutable)
	activeSnapshot := e.active
	e.mu.RUnlock()

	result := make(map[string]storage.Entry)

	for _, level := range levelSnapshot {
		for i := len(level) - 1; i >= 0; i-- {
			sst := level[i]
			entries, err := sst.Scan(prefix)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if !entry.Tombstone {
					result[entry.Key] = entry
				} else {
					delete(result, entry.Key)
				}
			}
		}
	}

	for i := len(immutableSnapshot) - 1; i >= 0; i-- {
		for _, entry := range immutableSnapshot[i].Scan(prefix) {
			if !entry.Tombstone {
				result[entry.Key] = entry
			} else {
				delete(result, entry.Key)
			}
		}
	}

	for _, entry := range activeSnapshot.Scan(prefix) {
		if !entry.Tombstone {
			result[entry.Key] = entry
		} else {
			delete(result, entry.Key)
		}
	}

	// Convert map to sorted slice
	entries := make([]storage.Entry, 0, len(result))
	for _, entry := range result {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

func (e *LSMEngine) Keys() ([]string, error) {
	entries, err := e.Scan("")
	if err != nil {
		return nil, err
	}

	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys, nil
}

// Iter creates a new iterator for range scans with cursor
// If prefix is empty, iterates over all keys
func (e *LSMEngine) Iter(prefix string) (*Iterator, error) {
	if e.closed.Load() {
		return nil, storage.ErrEngineClosed
	}

	// Take a snapshot of current state to avoid race with concurrent flush/compaction
	entries, err := e.snapshotScan(prefix)
	if err != nil {
		return nil, err
	}

	return NewIterator(entries, prefix), nil
}

// snapshotScan takes a consistent snapshot of current entries
func (e *LSMEngine) snapshotScan(prefix string) ([]storage.Entry, error) {
	e.mu.RLock()
	var snapshotLevels [][]*SSTableReader
	for _, level := range e.levels {
		levelCopy := make([]*SSTableReader, len(level))
		copy(levelCopy, level)
		snapshotLevels = append(snapshotLevels, levelCopy)
	}
	activeCopy := e.active
	immutableCopy := make([]*MemTable, len(e.immutable))
	copy(immutableCopy, e.immutable)
	e.mu.RUnlock()

	result := make(map[string]storage.Entry)

	for _, level := range snapshotLevels {
		for i := len(level) - 1; i >= 0; i-- {
			sst := level[i]
			entries, err := sst.Scan(prefix)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if !entry.Tombstone {
					result[entry.Key] = entry
				} else {
					delete(result, entry.Key)
				}
			}
		}
	}

	for i := len(immutableCopy) - 1; i >= 0; i-- {
		for _, entry := range immutableCopy[i].Scan(prefix) {
			if !entry.Tombstone {
				result[entry.Key] = entry
			} else {
				delete(result, entry.Key)
			}
		}
	}

	for _, entry := range activeCopy.Scan(prefix) {
		if !entry.Tombstone {
			result[entry.Key] = entry
		} else {
			delete(result, entry.Key)
		}
	}

	// Convert map to sorted slice
	entries := make([]storage.Entry, 0, len(result))
	for _, entry := range result {
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries, nil
}

func (e *LSMEngine) Stats() storage.EngineStats {
	// Get unique key count via Scan (handles tombstones and versioning correctly)
	// We can't hold the lock while calling Scan (it also acquires the lock),
	// so we do a lock-free snapshot approach.
	uniqueKeys := e.countUniqueKeys()

	e.mu.RLock()
	var memBytes int64

	if e.active != nil {
		memBytes += e.active.Size()
	}
	for _, mem := range e.immutable {
		memBytes += mem.Size()
	}

	// Count both SSTable files and WAL for actual disk usage
	var diskBytes int64

	for _, level := range e.levels {
		for _, sst := range level {
			if sst.path != "" {
				info, err := os.Stat(sst.path)
				if err == nil {
					diskBytes += info.Size()
				}
			}
		}
	}

	// Add WAL file size (it's part of persisted data)
	if e.wal != nil {
		if walSize, err := e.wal.Size(); err == nil {
			diskBytes += walSize
		}
	}
	e.mu.RUnlock()

	return storage.EngineStats{
		KeyCount:    uniqueKeys,
		DiskBytes:   diskBytes,
		MemBytes:    memBytes,
		BloomFPRate: 0.01,
	}
}

func (e *LSMEngine) countUniqueKeys() int64 {
	if e.closed.Load() {
		return 0
	}

	// Use Scan() which properly handles tombstones across all levels and memtables
	entries, err := e.Scan("")
	if err != nil {
		return 0
	}

	return int64(len(entries))
}

// RecordLevelMetrics updates Prometheus gauges for SSTable counts per level
func (e *LSMEngine) RecordLevelMetrics() {
	if e.config.Metrics == nil {
		return
	}
	e.mu.RLock()
	for level, ssts := range e.levels {
		e.config.Metrics.SetLSMLevelSSTables(strconv.Itoa(level), len(ssts))
	}
	e.mu.RUnlock()
}

func (e *LSMEngine) Close() error {
	if e.closed.Swap(true) {
		return nil
	}

	// Flush remaining data BEFORE closing channels.
	// flushMemTable() may send on compCh to trigger compaction —
	// doing this after close(compCh) causes a "send on closed channel" panic.
	if err := e.flushMemTable(); err != nil {
		slog.Error("final flush error", "error", err)
	}

	// Persist version counter before closing
	if err := e.saveVersion(); err != nil {
		slog.Error("save version error", "error", err)
	}

	close(e.flushCh)
	close(e.directFlushCh)
	close(e.compCh)
	e.wg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, level := range e.levels {
		for _, sst := range level {
			sst.Close()
		}
	}

	return e.wal.Close()
}

func (e *LSMEngine) TriggerCompaction() {
	select {
	case e.compCh <- struct{}{}:
	default:
	}
}

func (e *LSMEngine) saveVersion() error {
	versionPath := filepath.Join(e.dir, "VERSION")
	tmpPath := versionPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d", e.version.Load())
	if err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, versionPath)
}

func (e *LSMEngine) loadVersion() error {
	versionPath := filepath.Join(e.dir, "VERSION")
	data, err := os.ReadFile(versionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No version file yet, start from 0
		}
		return err
	}
	var version uint64
	_, err = fmt.Sscanf(string(data), "%d", &version)
	if err != nil {
		return err
	}
	e.version.Store(version)
	return nil
}

// mergeSSTables merges multiple SSTables using merge sort
// Returns all entries sorted by key, with all versions preserved
func mergeSSTables(ssts []*SSTableReader) []storage.Entry {
	if len(ssts) == 0 {
		return nil
	}

	// Collect all entries from all SSTables
	var allEntries []storage.Entry
	for _, sst := range ssts {
		entries, err := sst.Scan("")
		if err != nil {
			slog.Warn("mergeSSTables: scan error", "path", sst.path, "error", err)
			continue
		}
		allEntries = append(allEntries, entries...)
	}

	if len(allEntries) == 0 {
		return nil
	}

	// Sort by key, then by version (descending - newest first)
	sort.Slice(allEntries, func(i, j int) bool {
		if allEntries[i].Key != allEntries[j].Key {
			return allEntries[i].Key < allEntries[j].Key
		}
		return allEntries[i].Version > allEntries[j].Version
	})

	return allEntries
}

// deduplicateEntries keeps the latest version of each key
// and removes tombstones older than ttl
func deduplicateEntries(entries []storage.Entry, tombstoneTTL time.Duration) []storage.Entry {
	if len(entries) == 0 {
		return nil
	}

	var result []storage.Entry
	i := 0
	for i < len(entries) {
		// Find all versions of this key
		j := i + 1
		for j < len(entries) && entries[j].Key == entries[i].Key {
			j++
		}
		// entries[i:j] are all versions of the same key
		// They are sorted by version descending - first one is latest
		latest := entries[i]

		// Skip tombstones that are old enough
		if latest.Tombstone && time.Since(latest.TimeStamp) > tombstoneTTL {
			i = j
			continue // garbage collect this tombstone
		}

		result = append(result, latest)
		i = j
	}

	return result
}

func (e *LSMEngine) InternalStats() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	l0Count := 0
	if len(e.levels) > 0 {
		l0Count = len(e.levels[0])
	}

	return map[string]interface{}{
		"active_keys":     e.active.Len(),
		"immutable_count": len(e.immutable),
		"l0_sstables":     l0Count,
		"levels":          len(e.levels),
		"version":         e.version.Load(),
	}
}
