package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	version   = "dev"
	gitCommit = ""
	buildDate = ""
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print certslurpd version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("certslurpd version: %s\n", version)
		if gitCommit != "" {
			fmt.Printf("git commit: %s\n", gitCommit)
		}
		if buildDate != "" {
			fmt.Printf("build date: %s\n", buildDate)
		}
	},
}

func init() {
	// ... existing
	rootCmd.AddCommand(versionCmd)
}
