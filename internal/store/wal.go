package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type WALRecord struct {
	Op          string            `json:"op"`
	Key         string            `json:"key"`
	Value       []byte            `json:"value,omitempty"`
	Version     uint64            `json:"ver"`
	TimeStamp   int64             `json:"ts"`
	VectorClock map[string]uint64 `json:"vc,omitempty"`
}

type WALReplayHandler interface {
	PutEntry(entry Entry) error
	SetVersion(version uint64)
}

type WALReplayHandlerFuncs struct {
	PutEntryFunc   func(Entry) error
	SetVersionFunc func(uint64)
}

func (h WALReplayHandlerFuncs) PutEntry(entry Entry) error {
	if h.PutEntryFunc != nil {
		return h.PutEntryFunc(entry)
	}
	return nil
}

func (h WALReplayHandlerFuncs) SetVersion(version uint64) {
	if h.SetVersionFunc != nil {
		h.SetVersionFunc(version)
	}
}

type WALConfig struct {
	SyncInterval     time.Duration // 0 = sync every write, >0 = background sync interval
	CheckpointBytes  int64         // checkpoint after N bytes (default 1MB)
	MaxBufferedBytes int64         // max buffered before forced flush (default 4MB)
	OnSyncError      func(error)
}

// Default WAL settings optimized for throughput while maintaining durability
const (
	DefaultWALSyncInterval     = 100 * time.Millisecond // async background sync
	DefaultWALCheckpointBytes  = 1024 * 1024            // 1MB checkpoint
	DefaultWALMaxBufferedBytes = 4 * 1024 * 1024        // 4MB max buffered
)

const (
	walOpPut = byte(0)
	walOpDel = byte(1)
)

type WAL struct {
	mu           sync.Mutex
	file         *os.File
	wbuf         *bufio.Writer
	path         string
	stopCh       chan struct{}
	stopOnce     sync.Once
	wg           sync.WaitGroup
	config       WALConfig
	bytesWritten int64 // track bytes written for checkpoint
	dirty        bool  // data not yet synced to disk
	closed       bool  // WAL has been closed
}

func OpenWAL(path string) (*WAL, error) {
	return OpenWALWithConfig(path, WALConfig{
		SyncInterval:     0, // Legacy behavior: sync on every write
		CheckpointBytes:  0,
		MaxBufferedBytes: 0,
	})
}

func OpenWALWithConfig(path string, cfg WALConfig) (*WAL, error) {
	if cfg.SyncInterval < 0 {
		return nil, fmt.Errorf("WAL SyncInterval cannot be negative")
	}

	// Apply optimized defaults only when values are explicitly set
	// SyncInterval = 0 means sync on every write (legacy behavior)
	if cfg.SyncInterval > 0 {
		if cfg.CheckpointBytes <= 0 {
			cfg.CheckpointBytes = DefaultWALCheckpointBytes // 1MB
		}
		if cfg.MaxBufferedBytes <= 0 {
			cfg.MaxBufferedBytes = DefaultWALMaxBufferedBytes // 4MB
		}
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL %s: %w", path, err)
	}

	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, err
	}

	// Use larger buffer for throughput (256KB)
	w := &WAL{
		file:         f,
		wbuf:         bufio.NewWriterSize(f, 256*1024),
		path:         path,
		config:       cfg,
		bytesWritten: 0,
		dirty:        false,
	}

	// Always create stop channel for Close() safety
	w.stopCh = make(chan struct{})

	// Start background sync goroutine only if async mode is enabled
	if cfg.SyncInterval > 0 {
		w.wg.Add(1)
		go w.backgroundSync()
	} else {
		// For sync mode, Close the stopCh immediately since no goroutine is running
		close(w.stopCh)
		w.stopCh = nil // Mark as nil so Close() knows not to wait
	}

	return w, nil
}

func (w *WAL) backgroundSync() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.config.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if !w.dirty {
				w.mu.Unlock()
				continue
			}
			w.wbuf.Flush()
			w.dirty = false
			f := w.file
			w.mu.Unlock()

			if err := f.Sync(); err != nil {
				if w.config.OnSyncError != nil {
					w.config.OnSyncError(err)
				} else {
					fmt.Fprintf(os.Stderr, "WAL fsync error: %v\n", err)
				}
			}
		case <-w.stopCh:
			return
		}
	}
}

func (w *WAL) Append(entry Entry) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return fmt.Errorf("WAL is closed")
	}
	defer w.mu.Unlock()
	keyLen := len(entry.Key)
	valLen := len(entry.Value)

	vcData, err := json.Marshal(entry.VectorClock)
	if err != nil {
		vcData = nil
	}
	vcLen := len(vcData)
	recordSize := 4 + keyLen + 4 + valLen + 8 + 8 + 1 + 4 + vcLen

	if w.wbuf.Available() < recordSize {
		if err := w.wbuf.Flush(); err != nil {
			return fmt.Errorf("WAL flush: %w", err)
		}
	}

	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(keyLen))
	w.wbuf.Write(hdr[:])
	w.wbuf.WriteString(entry.Key)
	binary.LittleEndian.PutUint32(hdr[:], uint32(valLen))
	w.wbuf.Write(hdr[:])
	if valLen > 0 {
		w.wbuf.Write(entry.Value)
	}
	var verBuf [8]byte
	binary.LittleEndian.PutUint64(verBuf[:], entry.Version)
	w.wbuf.Write(verBuf[:])
	var tsBuf [8]byte
	binary.LittleEndian.PutUint64(tsBuf[:], uint64(entry.TimeStamp.UnixNano()))
	w.wbuf.Write(tsBuf[:])
	op := walOpPut
	if entry.Tombstone {
		op = walOpDel
	}
	w.wbuf.WriteByte(op)
	binary.LittleEndian.PutUint32(hdr[:], uint32(vcLen))
	w.wbuf.Write(hdr[:])
	if vcLen > 0 {
		w.wbuf.Write(vcData)
	}

	w.bytesWritten += int64(recordSize)
	w.dirty = true

	shouldCheckpoint := w.config.SyncInterval == 0 || (w.config.CheckpointBytes > 0 && w.bytesWritten >= w.config.CheckpointBytes)
	if shouldCheckpoint {
		w.wbuf.Flush()
		if err := w.file.Sync(); err != nil {
			if w.config.OnSyncError != nil {
				w.config.OnSyncError(err)
			}
		}
		w.bytesWritten = 0
		w.dirty = false
	}

	return nil
}

// Sync forces a synchronous flush to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dirty {
		w.wbuf.Flush()
		if err := w.file.Sync(); err != nil {
			return err
		}
		w.bytesWritten = 0
		w.dirty = false
	}
	return nil
}

func (w *WAL) File() *os.File {
	return w.file
}

func (w *WAL) Replay(handler WALReplayHandler) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wbuf.Flush()
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	defer func() { _, _ = w.file.Seek(0, io.SeekEnd) }()

	var firstByte [1]byte
	if _, err := w.file.Read(firstByte[:]); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	isJSON := firstByte[0] == '{'
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	var maxVersion uint64
	if isJSON {
		scanner := bufio.NewScanner(w.file)
		scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
		for scanner.Scan() {
			var rec WALRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				continue
			}
			entry := Entry{
				Key: rec.Key, Value: rec.Value, Version: rec.Version,
				TimeStamp: time.Unix(0, rec.TimeStamp), Tombstone: rec.Op == "DEL",
			}
			if err := handler.PutEntry(entry); err != nil {
				return err
			}
			if rec.Version > maxVersion {
				maxVersion = rec.Version
			}
		}
	} else {
		for {
			start, err := w.file.Seek(0, io.SeekCurrent)
			if err != nil {
				return err
			}
			rec, err := w.readBinaryRecord()
			if err == io.EOF {
				break
			}
			if err != nil {
				// A corrupt or incomplete tail must not hide already durable records.
				if _, err := w.file.Seek(start+1, io.SeekStart); err != nil {
					return err
				}
				continue
			}
			entry := Entry{Key: rec.Key, Value: rec.Value, Version: rec.Version, TimeStamp: time.Unix(0, rec.TimeStamp), Tombstone: rec.Op == "DEL", VectorClock: rec.VectorClock}
			if err := handler.PutEntry(entry); err != nil {
				return err
			}
			if rec.Version > maxVersion {
				maxVersion = rec.Version
			}
		}
	}
	handler.SetVersion(maxVersion)
	return nil
}

func (w *WAL) Close() error {
	w.stopOnce.Do(func() {
		if w.stopCh != nil {
			close(w.stopCh)
		}
	})
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.wbuf.Flush()
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("WAL sync on close: %w", err)
	}
	return w.file.Close()
}

// BatchAppend writes multiple entries in a single lock acquisition.
// This is significantly faster than calling Append in a loop because it
// eliminates per-entry mutex lock/unlock overhead for batch workloads.
func (w *WAL) BatchAppend(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return fmt.Errorf("WAL is closed")
	}

	for i := range entries {
		entry := &entries[i]
		keyLen := len(entry.Key)
		valLen := len(entry.Value)

		vcData, err := json.Marshal(entry.VectorClock)
		if err != nil {
			vcData = nil
		}
		vcLen := len(vcData)
		recordSize := 4 + keyLen + 4 + valLen + 8 + 8 + 1 + 4 + vcLen

		if w.wbuf.Available() < recordSize {
			if err := w.wbuf.Flush(); err != nil {
				return fmt.Errorf("WAL flush: %w", err)
			}
		}

		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(keyLen))
		w.wbuf.Write(hdr[:])
		w.wbuf.WriteString(entry.Key)
		binary.LittleEndian.PutUint32(hdr[:], uint32(valLen))
		w.wbuf.Write(hdr[:])
		if valLen > 0 {
			w.wbuf.Write(entry.Value)
		}
		var verBuf [8]byte
		binary.LittleEndian.PutUint64(verBuf[:], entry.Version)
		w.wbuf.Write(verBuf[:])
		var tsBuf [8]byte
		binary.LittleEndian.PutUint64(tsBuf[:], uint64(entry.TimeStamp.UnixNano()))
		w.wbuf.Write(tsBuf[:])
		op := walOpPut
		if entry.Tombstone {
			op = walOpDel
		}
		w.wbuf.WriteByte(op)
		binary.LittleEndian.PutUint32(hdr[:], uint32(vcLen))
		w.wbuf.Write(hdr[:])
		if vcLen > 0 {
			w.wbuf.Write(vcData)
		}

		w.bytesWritten += int64(recordSize)
		w.dirty = true
	}

	// Flush buffered data to kernel once per batch (cheap, no fsync)
	// Background sync goroutine handles periodic fsync every SyncInterval.
	if err := w.wbuf.Flush(); err != nil {
		return fmt.Errorf("WAL flush: %w", err)
	}
	// Reset checkpoint counter — actual fsync is handled by backgroundSync
	// In legacy mode (SyncInterval=0), reset after every batch
	// In async mode, only reset when threshold is reached
	shouldReset := w.config.SyncInterval == 0 || (w.config.CheckpointBytes > 0 && w.bytesWritten >= w.config.CheckpointBytes)
	if shouldReset {
		w.bytesWritten = 0
		w.dirty = false
	}

	return nil
}

func (w *WAL) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.wbuf.Flush(); err != nil {
		return err
	}
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.wbuf.Reset(w.file)

	return nil
}

func (w *WAL) Size() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (w *WAL) Checkpoint() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.wbuf.Flush(); err != nil {
		return 0, err
	}
	if err := w.file.Sync(); err != nil {
		return 0, err
	}
	pos, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	checkpoint := uint64(pos)
	if err := w.saveCheckpoint(checkpoint); err != nil {
		return 0, fmt.Errorf("save checkpoint: %w", err)
	}
	return checkpoint, nil
}

func (w *WAL) saveCheckpoint(checkpoint uint64) error {
	tmpPath := w.path + ".checkpoint.tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d", checkpoint)
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
	return os.Rename(tmpPath, w.path+".checkpoint")
}

func (w *WAL) LoadCheckpoint() (uint64, error) {
	data, err := os.ReadFile(w.path + ".checkpoint")
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var checkpoint uint64
	_, err = fmt.Sscanf(string(data), "%d", &checkpoint)
	if err != nil {
		return 0, fmt.Errorf("parse checkpoint: %w", err)
	}
	return checkpoint, nil
}

func (w *WAL) TruncateBefore(checkpoint uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.truncateBeforeUnlocked(checkpoint)
}

func (w *WAL) truncateBeforeUnlocked(checkpoint uint64) error {
	if err := w.wbuf.Flush(); err != nil {
		return err
	}
	if _, err := w.file.Seek(int64(checkpoint), io.SeekStart); err != nil {
		return err
	}
	tmpPath := w.path + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpPath)

	for {
		rec, readErr := w.readBinaryRecord()
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			if !w.resyncForward(tmpFile) {
				break
			}
			continue
		}
		if encErr := writeBinaryWALRecord(tmpFile, rec); encErr != nil {
			tmpFile.Close()
			return fmt.Errorf("write temp file: %w", encErr)
		}
	}

	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close original file: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("reopen WAL: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.wbuf.Reset(f)
	return nil
}

// readBinaryRecord reads a single binary record from the current file position.
// Returns io.EOF when no more data, or an error if the record is corrupt.
func (w *WAL) readBinaryRecord() (WALRecord, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
		return WALRecord{}, err
	}
	keyLen := binary.LittleEndian.Uint32(hdr[:])
	// Sanity check: key length shouldn't exceed reasonable bounds
	if keyLen > 1024*1024 {
		return WALRecord{}, fmt.Errorf("key too large: %d", keyLen)
	}
	keyBuf := make([]byte, keyLen)
	if _, err := io.ReadFull(w.file, keyBuf); err != nil {
		return WALRecord{}, err
	}
	if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
		return WALRecord{}, err
	}
	valLen := binary.LittleEndian.Uint32(hdr[:])
	if valLen > 10*1024*1024 {
		return WALRecord{}, fmt.Errorf("value too large: %d", valLen)
	}
	var value []byte
	if valLen > 0 {
		value = make([]byte, valLen)
		if _, err := io.ReadFull(w.file, value); err != nil {
			return WALRecord{}, err
		}
	}
	var verBuf [8]byte
	if _, err := io.ReadFull(w.file, verBuf[:]); err != nil {
		return WALRecord{}, err
	}
	version := binary.LittleEndian.Uint64(verBuf[:])
	var tsBuf [8]byte
	if _, err := io.ReadFull(w.file, tsBuf[:]); err != nil {
		return WALRecord{}, err
	}
	timestamp := int64(binary.LittleEndian.Uint64(tsBuf[:]))
	var opBuf [1]byte
	if _, err := io.ReadFull(w.file, opBuf[:]); err != nil {
		return WALRecord{}, err
	}
	if _, err := io.ReadFull(w.file, hdr[:]); err != nil {
		return WALRecord{}, err
	}
	vcLen := binary.LittleEndian.Uint32(hdr[:])
	if vcLen > 10*1024*1024 {
		return WALRecord{}, fmt.Errorf("vector clock too large: %d", vcLen)
	}
	vcData := make([]byte, vcLen)
	if _, err := io.ReadFull(w.file, vcData); err != nil {
		return WALRecord{}, err
	}
	var vc VectorClock
	if vcLen > 0 {
		if err := json.Unmarshal(vcData, &vc); err != nil {
			return WALRecord{}, err
		}
	}
	if opBuf[0] != walOpPut && opBuf[0] != walOpDel {
		return WALRecord{}, fmt.Errorf("invalid op: %d", opBuf[0])
	}
	rec := WALRecord{
		Op:          "PUT",
		Key:         string(keyBuf),
		Value:       value,
		Version:     version,
		TimeStamp:   timestamp,
		VectorClock: vc,
	}
	if opBuf[0] == walOpDel {
		rec.Op = "DEL"
	}
	return rec, nil
}

// resyncForward scans the file forward looking for valid binary records.
// It reads the remaining file content and writes any valid records to tmpFile.
// Returns true if at least one valid record was found and written.
func (w *WAL) resyncForward(tmpFile *os.File) bool {
	// Read remaining content
	remaining, err := io.ReadAll(w.file)
	if err != nil || len(remaining) == 0 {
		return false
	}

	found := false
	// Try to find valid records starting from each byte offset
	for offset := 0; offset < len(remaining); {
		rec, size, ok := tryParseBinaryRecord(remaining[offset:])
		if ok {
			if encErr := writeBinaryWALRecord(tmpFile, rec); encErr == nil {
				found = true
			}
			offset += size
		} else {
			offset++
		}
	}
	return found
}

// tryParseBinaryRecord attempts to parse a binary record from the given buffer.
// Returns the record, the number of bytes consumed, and whether parsing succeeded.
func tryParseBinaryRecord(data []byte) (WALRecord, int, bool) {
	minRecordSize := 4 + 0 + 4 + 8 + 8 + 1 // keyLen + key + valLen + version + timestamp + op
	if len(data) < minRecordSize {
		return WALRecord{}, 0, false
	}

	keyLen := binary.LittleEndian.Uint32(data[0:4])
	if keyLen > 1024*1024 {
		return WALRecord{}, 0, false
	}

	pos := 4
	if uint32(len(data)) < keyLen+4+4+8+8+1 {
		return WALRecord{}, 0, false
	}
	key := string(data[pos : pos+int(keyLen)])
	pos += int(keyLen)

	valLen := binary.LittleEndian.Uint32(data[pos : pos+4])
	if valLen > 10*1024*1024 {
		return WALRecord{}, 0, false
	}
	pos += 4

	var value []byte
	if valLen > 0 {
		if uint32(len(data)) < uint32(pos)+valLen {
			return WALRecord{}, 0, false
		}
		value = data[pos : pos+int(valLen)]
		pos += int(valLen)
	}

	if uint32(len(data)) < uint32(pos)+8+8+1 {
		return WALRecord{}, 0, false
	}
	version := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	timestamp := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8
	op := data[pos]
	pos++

	if op != walOpPut && op != walOpDel {
		return WALRecord{}, 0, false
	}

	if len(data)-pos < 4 {
		return WALRecord{}, 0, false
	}
	vcLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if vcLen > 10*1024*1024 || len(data)-pos < vcLen {
		return WALRecord{}, 0, false
	}
	var vc VectorClock
	if vcLen > 0 {
		if json.Unmarshal(data[pos:pos+vcLen], &vc) != nil {
			return WALRecord{}, 0, false
		}
	}
	pos += vcLen
	rec := WALRecord{
		Op:          "PUT",
		Key:         key,
		Value:       value,
		Version:     version,
		TimeStamp:   timestamp,
		VectorClock: vc,
	}
	if op == walOpDel {
		rec.Op = "DEL"
	}
	return rec, pos, true
}

func (w *WAL) Compact() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.wbuf.Flush(); err != nil {
		return 0, err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	tmpPath := w.path + ".compact.tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpPath)

	// Detect format
	var firstByte [1]byte
	if _, err := w.file.Read(firstByte[:]); err != nil {
		if err == io.EOF {
			// Empty WAL, just rename empty temp
		} else {
			return 0, err
		}
	}
	isJSON := firstByte[0] == '{'
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	entryCount := 0
	if isJSON {
		scanner := bufio.NewScanner(w.file)
		scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
		for scanner.Scan() {
			var rec WALRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				continue
			}
			if encErr := writeBinaryWALRecord(tmpFile, rec); encErr != nil {
				return 0, fmt.Errorf("write temp file: %w", encErr)
			}
			entryCount++
		}
		if err := scanner.Err(); err != nil {
			return 0, fmt.Errorf("scan WAL: %w", err)
		}
	} else {
		for {
			rec, err := w.readBinaryRecord()
			if err == io.EOF {
				break
			}
			if err != nil {
				return 0, fmt.Errorf("compact WAL: %w", err)
			}
			if err := writeBinaryWALRecord(tmpFile, rec); err != nil {
				return 0, err
			}
			entryCount++
		}
	}

	if err := tmpFile.Sync(); err != nil {
		return 0, fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return 0, fmt.Errorf("close temp file: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return 0, fmt.Errorf("close original file: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return 0, fmt.Errorf("rename temp file: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("reopen WAL: %w", err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return 0, err
	}
	w.file = f
	w.wbuf.Reset(f)
	return entryCount, nil
}

func (w *WAL) Count() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.wbuf.Flush()
	if _, err := w.file.Seek(0, 0); err != nil {
		return 0, err
	}
	var firstByte [1]byte
	if _, err := w.file.Read(firstByte[:]); err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, err
	}
	isJSON := firstByte[0] == '{'
	if _, err := w.file.Seek(0, 0); err != nil {
		return 0, err
	}
	count := 0
	if isJSON {
		scanner := bufio.NewScanner(w.file)
		scanner.Buffer(make([]byte, 2*1024*1024), 2*1024*1024)
		for scanner.Scan() {
			var rec WALRecord
			if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
				continue
			}
			count++
		}
		if err := scanner.Err(); err != nil {
			return 0, err
		}
	} else {
		for {
			_, err := w.readBinaryRecord()
			if err == io.EOF {
				break
			}
			if err != nil {
				return count, err
			}
			count++
		}
	}
	_, err := w.file.Seek(0, io.SeekEnd)
	return count, err
}

// writeBinaryWALRecord keeps compaction and truncation compatible with Append.
func writeBinaryWALRecord(dst io.Writer, rec WALRecord) error {
	vc, err := json.Marshal(rec.VectorClock)
	if err != nil {
		return err
	}
	for _, field := range []any{uint32(len(rec.Key)), []byte(rec.Key), uint32(len(rec.Value)), rec.Value, rec.Version, rec.TimeStamp} {
		if err := binary.Write(dst, binary.LittleEndian, field); err != nil {
			return err
		}
	}
	op := walOpPut
	if rec.Op == "DEL" {
		op = walOpDel
	}
	if err := binary.Write(dst, binary.LittleEndian, op); err != nil {
		return err
	}
	if err := binary.Write(dst, binary.LittleEndian, uint32(len(vc))); err != nil {
		return err
	}
	_, err = dst.Write(vc)
	return err
}
