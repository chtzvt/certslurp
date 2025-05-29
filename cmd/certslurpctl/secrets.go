package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/nacl/secretbox"
)

func loadClusterKey(path string) ([32]byte, error) {
	var clusterKey [32]byte
	b64, err := os.ReadFile(path)
	if err != nil {
		return clusterKey, err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b64)))
	if err != nil {
		return clusterKey, err
	}
	if len(raw) != 32 {
		return clusterKey, fmt.Errorf("invalid cluster key length: got %d, want 32", len(raw))
	}
	copy(clusterKey[:], raw)
	return clusterKey, nil
}

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

			client := api.NewClient(apiURL, apiToken)
			return client.ApproveNode(context.Background(), nodeID)
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
			keyFile, _ := cmd.Flags().GetString("key-file")

			clusterKey, err := loadClusterKey(keyFile)
			if err != nil {
				return fmt.Errorf("invalid cluster key (base64): %w", err)
			}

			// Encrypt the value
			enc := secrets.EncryptValue(clusterKey, val)

			ctx := context.Background()
			client := cliClient()
			if err := client.PutSecret(ctx, args[0], enc); err != nil {
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
		Short: "Get (decrypted) secret value",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			keyFile, _ := cmd.Flags().GetString("key-file")

			clusterKey, err := loadClusterKey(keyFile)
			if err != nil {
				return fmt.Errorf("invalid cluster key (base64): %w", err)
			}

			client := cliClient()
			ctx := context.Background()
			ciphertext, err := client.GetSecret(ctx, args[0])
			if err != nil {
				return err
			}
			if len(ciphertext) < 24 {
				return fmt.Errorf("ciphertext too short")
			}
			var nonce [24]byte
			copy(nonce[:], ciphertext[:24])
			plaintext, ok := secretbox.Open(nil, ciphertext[24:], &nonce, &clusterKey)
			if !ok {
				return fmt.Errorf("decryption failed")
			}
			os.Stdout.Write(plaintext)
			return nil
		},
	}

	getCmd.Flags().String("key-file", "cluster.key", "Base64 cluster key file")
	_ = getCmd.MarkFlagRequired("key-file")

	return getCmd
}
