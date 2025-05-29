package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/spf13/cobra"
)

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

	approveCmd.Flags().String("key-file", "cluster.key", "Base64 cluster key file")
	_ = approveCmd.MarkFlagRequired("key-file")

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
			outResult(keys, printSecretsTable)
			return nil
		},
	}
	cmd.Flags().StringVar(&prefix, "prefix", "", "Prefix filter")
	return cmd
}

func secretsAddCmd() *cobra.Command {
	addCmd := &cobra.Command{
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

	addCmd.Flags().String("key-file", "cluster.key", "Base64 cluster key file")
	_ = addCmd.MarkFlagRequired("key-file")

	return addCmd
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

func secretsGetCmd() *cobra.Command {
	getCmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Get (encrypted) secret value",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := cliClient()
			ctx := context.Background()
			val, err := client.GetSecret(ctx, args[0])
			if err != nil {
				return err
			}
			// Just write the bytes to stdout
			os.Stdout.Write(val)
			return nil
		},
	}

	getCmd.Flags().String("key-file", "cluster.key", "Base64 cluster key file")
	_ = getCmd.MarkFlagRequired("key-file")

	return getCmd
}
