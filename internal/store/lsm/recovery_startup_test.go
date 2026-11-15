package lsm

import (
	"path/filepath"
	"testing"
	"time"

	storage "github.com/VanshikaSharm4/TurboDynamo/internal/store"
	"github.com/stretchr/testify/require"
)

func TestStartupRecoversNonemptyWAL(t *testing.T) {
	dir := t.TempDir()
	wal, err := storage.OpenWAL(filepath.Join(dir, "wal.log"))
	require.NoError(t, err)
	require.NoError(t, wal.Append(storage.Entry{Key: "recovered", Value: []byte("saved"), Version: 1, TimeStamp: time.Now()}))
	require.NoError(t, wal.Close())

	engine, err := NewLSMEngine(dir)
	require.NoError(t, err)
	defer engine.Close()
	entry, err := engine.Get("recovered")
	require.NoError(t, err)
	require.Equal(t, []byte("saved"), entry.Value)
}
