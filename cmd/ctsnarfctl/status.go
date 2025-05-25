package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	Run: func(cmd *cobra.Command, args []string) {
		// TODO: Implement real status querying logic (etcd, etc.)
		fmt.Println("Cluster status: (stub)")
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
