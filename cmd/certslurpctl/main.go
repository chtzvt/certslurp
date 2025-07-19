package main

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

var (
	apiURL     string
	apiToken   string
	keyFile    string
	outputJSON bool
	timeout    time.Duration
)

func main() {
	root := &cobra.Command{
		Use:   "certslurpctl",
		Short: "certslurp control/admin CLI",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if len(os.Args) > 1 && os.Args[1] != "secrets" && os.Args[1] != "worker" {
				if apiURL == "" || apiToken == "" {
					return fmt.Errorf("--api-url and --api-token are required")
				}
			}
			return nil
		},
	}

	root.PersistentFlags().StringVar(&apiURL, "api-url", os.Getenv("CERTSLURP_API_URL"), "API URL (or $CERTSLURP_API_URL)")
	root.PersistentFlags().StringVar(&apiToken, "api-token", os.Getenv("CERTSLURP_API_TOKEN"), "API token (or $CERTSLURP_API_TOKEN)")
	root.PersistentFlags().StringVar(&keyFile, "cluster-key-file", os.Getenv("CERTSLURP_CLUSTER_KEY_FILE"), "API token (or $CERTSLURP_CLUSTER_KEY_FILE)")
	root.PersistentFlags().DurationVar(&timeout, "timeout", 15*time.Second, "API request timeout")
	root.PersistentFlags().BoolVar(&outputJSON, "json", false, "Output as JSON")

	// Jobs
	jobs := &cobra.Command{Use: "job", Short: "Manage jobs"}
	jobs.AddCommand(
		jobSubmitCmd(),
		jobTemplateCmd(),
		jobListCmd(),
		jobStatusCmd(),
		jobStartCmd(),
		jobCancelCmd(),
		jobCompleteCmd(),
		jobShardsCmd(),
		jobResetFailedCmd(),
	)
	root.AddCommand(jobs)

	root.AddCommand(shardCmd())

	// Cluster status
	root.AddCommand(clusterStatusCmd())

	// Workers
	workers := &cobra.Command{Use: "worker", Short: "Worker nodes"}
	workers.AddCommand(
		workerListCmd(),
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
		secretsGetCmd(),
	)
	root.AddCommand(secrets)

	root.AddCommand(&cobra.Command{
		Use:   "completion",
		Short: "Generate shell completion scripts",
		Run: func(cmd *cobra.Command, args []string) {
			root.GenBashCompletion(os.Stdout)
		},
	})

	_ = root.Execute()
}
