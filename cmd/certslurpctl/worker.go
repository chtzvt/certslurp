package main

import (
	"context"

	"github.com/spf13/cobra"
)

func workerListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			workers, err := client.ListWorkers(ctx)
			if err != nil {
				return err
			}
			outResult(workers, printWorkersTable)
			return nil
		},
	}
}

func workerMetricsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "metrics <workerID>",
		Short: "Show metrics for a worker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			metrics, err := client.GetWorkerMetrics(ctx, args[0])
			if err != nil {
				return err
			}
			outResult(metrics, printWorkerMetricsTable)
			return nil
		},
	}
}
