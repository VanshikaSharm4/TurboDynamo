package storage

import (
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestWALRewritePreservesVectorClockAndLaterAppends(t *testing.T) {
	for _, rewrite := range []string{"compact", "truncate"} {
		t.Run(rewrite, func(t *testing.T) {
			wal, err := OpenWAL(filepath.Join(t.TempDir(), "wal.log"))
			require.NoError(t, err)
			defer wal.Close()
			require.NoError(t, wal.Append(Entry{Key: "old", Value: []byte("before"), Version: 1, TimeStamp: time.Now()}))
			checkpoint, err := wal.Checkpoint()
			require.NoError(t, err)
			clock := VectorClock{"node-1": 2, "node-2": 1}
			require.NoError(t, wal.Append(Entry{Key: "keep", Value: []byte("after"), Version: 2, TimeStamp: time.Now(), VectorClock: clock}))
			if rewrite == "compact" {
				_, err = wal.Compact()
			} else {
				err = wal.TruncateBefore(checkpoint)
			}
			require.NoError(t, err)
			require.NoError(t, wal.Append(Entry{Key: "later", Value: []byte("new"), Version: 3, TimeStamp: time.Now()}))
			handler := &MockWALHandler{}
			require.NoError(t, wal.Replay(handler))
			expected := 3
			if rewrite == "truncate" {
				expected = 2
			}
			require.Len(t, handler.entries, expected)
			require.Equal(t, clock, handler.entries[expected-2].VectorClock)
			require.Equal(t, "later", handler.entries[expected-1].Key)
			count, err := wal.Count()
			require.NoError(t, err)
			require.Equal(t, expected, count)
		})
	}
}
