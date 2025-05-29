package main

import (
	"context"

	"github.com/spf13/cobra"
)

func clusterStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cluster status",
		Short: "Show cluster status",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			status, err := client.GetClusterStatus(ctx)
			if err != nil {
				return err
			}
			outResult(status, printClusterStatusTable)
			return nil
		},
	}
}
