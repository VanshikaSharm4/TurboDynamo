package engine

import (
	"fmt"

	"github.com/VanshikaSharm4/TurboDynamo/internal/config"
	storage "github.com/VanshikaSharm4/TurboDynamo/internal/store"
	"github.com/VanshikaSharm4/TurboDynamo/internal/store/lsm"
)

type KVEngine interface {
	storage.StorageEngine
	Flush() error
	TriggerCompaction()
}

var (
	cfg        *config.Config
	walSyncMs  int
	remoteAddr string
	useRemote  bool
)

func SetConfig(c *config.Config) {
	cfg = c
}

func SetWALSyncMs(ms int) {
	walSyncMs = ms
}

func SetRemoteAddr(addr string) {
	remoteAddr = addr
	useRemote = true
}

func GetEngine() (KVEngine, error) {
	if useRemote && remoteAddr != "" {
		return NewRemoteEngine(remoteAddr), nil
	}

	if cfg == nil {
		return nil, fmt.Errorf("config not loaded (specify --addr for remote or --dir for local)")
	}

	lsme, err := lsm.NewLSMEngine(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	return lsme, nil
}
