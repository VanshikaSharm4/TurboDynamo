package commands

import (
	"github.com/VanshikaSharm4/TurboDynamo/cmd/kvctl/engine"
	"github.com/VanshikaSharm4/TurboDynamo/internal/config"
	"github.com/spf13/cobra"
)

var (
	cfgFile    string
	dataDir    string
	serverAddr string
	outputFmt  string
	verbose    bool
	walSyncMs  int
	cfg        *config.Config
)

var RootCmd = &cobra.Command{
	Use:   "kvctl",
	Short: "TurboDynamo KV Store CLI",
	Long: `TurboDynamo - High Performance Key-Value Store

A modern LSM-tree based storage engine with:
  • Write-Ahead Logging (WAL)
  • Bloom Filters
  • Automatic Compaction
  • Crash Recovery

Use with a running server (like Redis CLI):
  kvctl --addr http://localhost:9000 put user:1 "Alice"
  kvctl --addr http://localhost:9000 get user:1
  kvctl --addr http://localhost:9000 scan user:

Or offline directly on disk:
  kvctl put user:1 "Alice"
  kvctl get user:1`,
	Example: `  kvctl --addr http://localhost:9000 put user:1 "Alice"
  kvctl --addr http://localhost:9000 get user:1
  kvctl --addr http://localhost:9000 scan user:
  kvctl --addr http://localhost:9000 keys
  kvctl shell`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if serverAddr != "" {
			engine.SetRemoteAddr(serverAddr)
			return nil
		}
		return initEngine()
	},
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() error {
	return RootCmd.Execute()
}

func initEngine() error {
	if err := loadConfig(); err != nil {
		return err
	}

	engine.SetConfig(cfg)
	engine.SetWALSyncMs(walSyncMs)

	return nil
}

func loadConfig() error {
	var err error
	cfg, err = config.Load(cfgFile)
	if err != nil {
		return err
	}

	if dataDir != "" {
		cfg.DataDir = dataDir
	}

	return nil
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&serverAddr, "addr", "a", "", "Server address (e.g. http://localhost:9000) — like redis-cli -h")
	RootCmd.PersistentFlags().StringVarP(&dataDir, "dir", "d", "data", "Data directory for local mode (default: ./data)")
	RootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "Configuration file (YAML)")
	RootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "text", "Output format (text, json, table)")
	RootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "Verbose output")
	RootCmd.PersistentFlags().IntVar(&walSyncMs, "wal-sync", 0, "WAL fsync interval in ms")
}
