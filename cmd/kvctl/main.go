package main

import (
	"os"

	"github.com/VanshikaSharm4/TurboDynamo/cmd/kvctl/commands"
	"github.com/VanshikaSharm4/TurboDynamo/cmd/kvctl/output"
)

func main() {
	// Setup colors
	output.SetupColors()

	// Execute root command
	if err := commands.RootCmd.Execute(); err != nil {
		output.ErrorColor.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
