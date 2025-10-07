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
	clusterKey string
	outputJSON bool
	timeout    time.Duration
)

const noAPICreds = "no-api-creds"

func main() {
	root := &cobra.Command{
		Use:   "certslurpctl",
		Short: "certslurp control/admin CLI",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Annotations[noAPICreds] == "1" {
				return nil
			}
			if apiURL == "" || apiToken == "" {
				return fmt.Errorf("--api-url and --api-token are required")
			}
			return nil
		},
	}

	root.PersistentFlags().StringVar(&apiURL, "api-url", os.Getenv("CERTSLURP_API_URL"), "API URL (or $CERTSLURP_API_URL)")
	root.PersistentFlags().StringVar(&apiToken, "api-token", os.Getenv("CERTSLURP_API_TOKEN"), "API token (or $CERTSLURP_API_TOKEN)")
	root.PersistentFlags().StringVar(&clusterKey, "cluster-key", os.Getenv("CERTSLURP_CLUSTER_KEY"), "Cluster key (or $CERTSLURP_CLUSTER_KEY)")
	root.PersistentFlags().StringVar(&keyFile, "cluster-key-file", os.Getenv("CERTSLURP_CLUSTER_KEY_FILE"), "Cluster key file path (or $CERTSLURP_CLUSTER_KEY_FILE)")
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
	gen := secretsGenClusterKeyCmd()
	if gen.Annotations == nil {
		gen.Annotations = map[string]string{}
	}
	gen.Annotations[noAPICreds] = "1"

	secrets := &cobra.Command{Use: "secrets", Short: "Secret store"}
	secrets.AddCommand(
		gen,
		secretsPendingCmd(),
		secretsApprovalCmd(),
		secretsListCmd(),
		secretsAddCmd(),
		secretsRemoveCmd(),
		secretsGetCmd(),
	)
	root.AddCommand(secrets)

	// Completion
	completion := &cobra.Command{
		Use:   "completion",
		Short: "Generate shell completion scripts",
		Run: func(cmd *cobra.Command, args []string) {
			root.GenBashCompletion(os.Stdout)
		},
	}

	if completion.Annotations == nil {
		completion.Annotations = map[string]string{}
	}

	completion.Annotations[noAPICreds] = "1"

	root.AddCommand(completion)

	_ = root.Execute()
}
