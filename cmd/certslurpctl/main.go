package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/job"
	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var (
	apiURL   string
	apiToken string
	timeout  time.Duration
)

func main() {
	root := &cobra.Command{
		Use:   "certslurpctl",
		Short: "certslurp control/admin CLI",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if apiURL == "" || apiToken == "" {
				return fmt.Errorf("--api-url and --api-token are required")
			}
			return nil
		},
	}

	root.PersistentFlags().StringVar(&apiURL, "api-url", os.Getenv("CERTSLURP_API_URL"), "API URL (or $CERTSLURP_API_URL)")
	root.PersistentFlags().StringVar(&apiToken, "api-token", os.Getenv("CERTSLURP_API_TOKEN"), "API token (or $CERTSLURP_API_TOKEN)")
	root.PersistentFlags().DurationVar(&timeout, "timeout", 15*time.Second, "API request timeout")

	// Jobs
	jobs := &cobra.Command{Use: "job", Short: "Manage jobs"}
	jobs.AddCommand(
		jobSubmitCmd(),
		jobListCmd(),
		jobStatusCmd(),
	)
	root.AddCommand(jobs)

	// Cluster status
	root.AddCommand(clusterStatusCmd())

	// Workers
	workers := &cobra.Command{Use: "worker", Short: "Worker nodes"}
	workers.AddCommand(
		workerListCmd(),
		workerInfoCmd(),
		workerMetricsCmd(),
	)
	root.AddCommand(workers)

	// Secrets
	secrets := &cobra.Command{Use: "secrets", Short: "Secret store"}
	secrets.AddCommand(
		secretsGenClusterKeyCmd(),
		secretsPendingCmd(),
		secretsApprovalCmd(),
		secretsListCmd(),
		secretsAddCmd(),
		secretsRemoveCmd(),
	)
	root.AddCommand(secrets)

	_ = root.Execute()
}

// Utility: API client for this CLI
func cliClient() *api.Client {
	c := api.NewClient(apiURL, apiToken)
	c.Client.Timeout = timeout
	return c
}

func outJSON(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}

// --- Job commands ---

func jobSubmitCmd() *cobra.Command {
	var (
		logURI  string
		start   int64
		end     int64
		shardSz int
	)
	cmd := &cobra.Command{
		Use:   "submit [spec.yaml]",
		Short: "Submit a new job (YAML/JSON spec or via flags)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()

			var spec job.JobSpec

			if len(args) > 0 && args[0] != "" {
				f, err := os.Open(args[0])
				if err != nil {
					return err
				}
				defer f.Close()
				dec := yaml.NewDecoder(f)
				if err := dec.Decode(&spec); err != nil {
					return fmt.Errorf("decode spec: %w", err)
				}
			} else {
				if logURI == "" {
					return fmt.Errorf("--log-uri required if not using spec file")
				}
				spec.Version = "1.0.0"
				spec.LogURI = logURI
				spec.Options.Fetch.IndexStart = start
				spec.Options.Fetch.IndexEnd = end
				spec.Options.Fetch.ShardSize = shardSz
				spec.Options.Fetch.FetchSize = 10
				spec.Options.Fetch.FetchWorkers = 1
				spec.Options.Output.Extractor = "raw"
				spec.Options.Output.Transformer = "passthrough"
				spec.Options.Output.Sink = "null"
			}

			jobID, err := client.SubmitJob(ctx, &spec)
			if err != nil {
				return err
			}
			fmt.Printf("Job submitted: %s\n", jobID)
			return nil
		},
	}
	cmd.Flags().StringVar(&logURI, "log-uri", "", "CT log URI")
	cmd.Flags().Int64Var(&start, "start", 0, "Index start")
	cmd.Flags().Int64Var(&end, "end", 0, "Index end (0=auto)")
	cmd.Flags().IntVar(&shardSz, "shard-size", 0, "Shard size (0=auto)")
	return cmd
}

func jobListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			jobs, err := client.ListJobs(ctx)
			if err != nil {
				return err
			}
			outJSON(jobs)
			return nil
		},
	}
}

func jobStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <jobID>",
		Short: "Show job status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			info, err := client.GetJob(ctx, args[0])
			if err != nil {
				return err
			}
			outJSON(info)
			return nil
		},
	}
}

// --- Cluster status ---

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
			outJSON(status)
			return nil
		},
	}
}

// --- Worker commands ---

func workerListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List all workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			workers, err := client.ListWorkers(ctx)
			if err != nil {
				return err
			}
			outJSON(workers)
			return nil
		},
	}
}

func workerInfoCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "info <workerID>",
		Short: "Show worker info",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			metrics, err := client.GetWorkerMetrics(ctx, args[0])
			if err != nil {
				return err
			}
			outJSON(metrics)
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
			outJSON(metrics)
			return nil
		},
	}
}

// --- Secrets management ---

func secretsPendingCmd() *cobra.Command {
	pendingCmd := &cobra.Command{
		Use: "pending",
		RunE: func(cmd *cobra.Command, args []string) error {
			client := api.NewClient(apiURL, apiToken)
			nodes, err := client.ListPendingNodes(context.Background())
			if err != nil {
				return err
			}
			for _, n := range nodes {
				fmt.Printf("Pending node: %s\n", n.NodeID)
			}
			return nil
		},
	}

	return pendingCmd
}

func secretsGenClusterKeyCmd() *cobra.Command {
	genKeyCmd := &cobra.Command{
		Use:   "genkey",
		Short: "Generate a new base64-encoded cluster key",
		RunE: func(cmd *cobra.Command, args []string) error {
			keyFile, _ := cmd.Flags().GetString("key-file")

			rawKey, err := secrets.GenerateClusterKey()
			if err != nil {
				return fmt.Errorf("failed to generate key: %w", err)
			}

			encodedKey := base64.StdEncoding.EncodeToString(rawKey[:]) + "\n"

			err = os.WriteFile(keyFile, []byte(encodedKey), 0o600)
			if err != nil {
				return fmt.Errorf("failed to write key file: %w", err)
			}

			fmt.Printf("Cluster key written to %s\n", keyFile)
			return nil
		},
	}

	genKeyCmd.Flags().String("key-file", "cluster.key", "Base64 cluster key file")
	_ = genKeyCmd.MarkFlagRequired("key-file")

	return genKeyCmd
}

func secretsApprovalCmd() *cobra.Command {
	approveCmd := &cobra.Command{
		Use: "approve",
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID, _ := cmd.Flags().GetString("node-id")
			keyFile, _ := cmd.Flags().GetString("key-file")
			// Load cluster key (32 bytes, base64)
			keyB64, err := os.ReadFile(keyFile)
			if err != nil {
				return err
			}
			client := api.NewClient(apiURL, apiToken)
			return client.ApproveNode(context.Background(), nodeID, string(keyB64))
		},
	}

	approveCmd.Flags().String("node-id", "", "Node ID to approve")
	approveCmd.Flags().String("key-file", "cluster.key", "Base64 cluster key file")
	approveCmd.MarkFlagRequired("node-id")

	return approveCmd
}

func secretsListCmd() *cobra.Command {
	var prefix string
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List secrets",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			keys, err := client.ListSecrets(ctx, prefix)
			if err != nil {
				return err
			}
			outJSON(keys)
			return nil
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "Prefix filter")
	return cmd
}

func secretsAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <key>",
		Short: "Add or update a secret (reads value from stdin)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			ctx := context.Background()
			client := cliClient()
			if err := client.PutSecret(ctx, args[0], val); err != nil {
				return err
			}
			fmt.Printf("Secret %q set\n", args[0])
			return nil
		},
	}
}

func secretsRemoveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <key>",
		Short: "Delete a secret",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			if err := client.DeleteSecret(ctx, args[0]); err != nil {
				return err
			}
			fmt.Printf("Secret %q deleted\n", args[0])
			return nil
		},
	}
}
