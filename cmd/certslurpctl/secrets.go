package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/chtzvt/certslurp/internal/api"
	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/nacl/secretbox"
)

func loadClusterKey(path string, keyFromEnv string) ([32]byte, error) {
	var out [32]byte
	var b64 []byte
	if keyFromEnv != "" {
		b64 = []byte(keyFromEnv)
	} else {
		if path == "" {
			return out, fmt.Errorf("no cluster key provided: set --cluster-key (or $CERTSLURP_CLUSTER_KEY) or --cluster-key-file")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return out, err
		}
		b64 = data
	}

	s := strings.TrimSpace(string(b64))
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		if raw, err = base64.RawStdEncoding.DecodeString(s); err != nil {
			return out, fmt.Errorf("cluster key must be base64 of 32 bytes: %w", err)
		}
	}
	if len(raw) != 32 {
		return out, fmt.Errorf("invalid cluster key length: got %d, want 32", len(raw))
	}
	copy(out[:], raw)
	for i := range raw {
		raw[i] = 0
	}
	return out, nil
}

func secretsPendingCmd() *cobra.Command {
	pendingCmd := &cobra.Command{
		Use:   "pending",
		Short: "List nodes pending approval",
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
			rawKey, err := secrets.GenerateClusterKey()
			if err != nil {
				return fmt.Errorf("failed to generate key: %w", err)
			}

			encodedKey := base64.StdEncoding.EncodeToString(rawKey[:]) + "\n"

			if keyFile == "" {
				fmt.Printf("%s", string(encodedKey))
				return nil
			}

			if err := os.MkdirAll(filepath.Dir(keyFile), 0o700); err != nil {
				return fmt.Errorf("failed to create key directory: %w", err)
			}

			err = os.WriteFile(keyFile, []byte(encodedKey), 0o600)
			if err != nil {
				return fmt.Errorf("failed to write key file: %w", err)
			}

			fmt.Printf("Cluster key written to %s\n", keyFile)
			return nil
		},
	}

	return genKeyCmd
}

func secretsApprovalCmd() *cobra.Command {
	approveCmd := &cobra.Command{
		Use:   "approve",
		Short: "Approve a node for secret store access",
		RunE: func(cmd *cobra.Command, args []string) error {
			nodeID, _ := cmd.Flags().GetString("node-id")

			client := api.NewClient(apiURL, apiToken)
			return client.ApproveNode(context.Background(), nodeID)
		},
	}

	approveCmd.Flags().String("node-id", "", "Node ID to approve")
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
			if keyFile == "" && clusterKey == "" {
				return fmt.Errorf("missing required --cluster-key (or $CERTSLURP_CLUSTER_KEY) or --cluster-key-file (or $CERTSLURP_CLUSTER_KEY_FILE)")
			}

			val, err := io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}

			ck, err := loadClusterKey(keyFile, clusterKey)
			if err != nil {
				return fmt.Errorf("failed to load cluster key (env/file): %w", err)
			}

			// Encrypt the value
			enc := secrets.EncryptValue(ck, val)

			ctx := context.Background()
			client := cliClient()
			if err := client.PutSecret(ctx, args[0], enc); err != nil {
				return err
			}
			fmt.Printf("Secret %q set\n", args[0])
			return nil
		},
	}

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
			if keyFile == "" && clusterKey == "" {
				return fmt.Errorf("missing required --cluster-key (or $CERTSLURP_CLUSTER_KEY) or --cluster-key-file (or $CERTSLURP_CLUSTER_KEY_FILE)")
			}

			ck, err := loadClusterKey(keyFile, clusterKey)
			if err != nil {
				return fmt.Errorf("failed to load cluster key (env/file): %w", err)
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
			plaintext, ok := secretbox.Open(nil, ciphertext[24:], &nonce, &ck)
			if !ok {
				return fmt.Errorf("decryption failed")
			}
			os.Stdout.Write(plaintext)
			return nil
		},
	}

	return getCmd
}
