package lsm

import (
	"strings"
	"sync"

	storage "github.com/VanshikaSharm4/TurboDynamo.git/internal/store"
)

const DefaultMemTableSize = 64 * 1024 * 1024 // 64MB

type MemTable struct {
	mu        sync.RWMutex
	list      *SkipList
	sizeBytes int64
	maxBytes  int64
}

func NewMemTable(maxBytes int64) *MemTable {
	if maxBytes <= 0 {
		maxBytes = DefaultMemTableSize
	}
	return &MemTable{
		list:     NewSkipList(16, 0.5),
		maxBytes: maxBytes,
	}
}

func (m *MemTable) Put(entry storage.Entry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldSize := m.list.Put(entry)
	newSize := int64(len(entry.Key) + len(entry.Value))
	if oldSize > 0 {
		// oldSize is only the old value length; we must subtract old key+value and add new key+value
		m.sizeBytes -= int64(len(entry.Key)) + oldSize
	}
	m.sizeBytes += newSize
}

func (m *MemTable) Get(key string) (storage.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.Get(key)
}

func (m *MemTable) Scan(prefix string) []storage.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []storage.Entry
	curr := m.list.Seek(prefix)

	for curr != nil {
		if !strings.HasPrefix(curr.entry.Key, prefix) {
			// since it's sorted and we seeked to prefix, we can break as soon as it doesn't match
			break
		}
		result = append(result, curr.entry)
		curr = curr.forward[0]
	}

	return result
}

func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes >= m.maxBytes
}

func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}

func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.Size()
}

func (m *MemTable) Entries() []storage.Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.Entries()
}
