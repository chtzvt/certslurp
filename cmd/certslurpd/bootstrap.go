package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/chtzvt/certslurp/cmd/certslurpd/config"
	"github.com/chtzvt/certslurp/internal/cluster"
)

func selfBootstrap(ctx context.Context, cl cluster.Cluster, cfg *config.ClusterConfig, logger *log.Logger) error {
	registrationDone := make(chan struct{})
	registrationFailed := make(chan error, 2)

	go func() {
		if err := cl.Secrets().RegisterAndWaitForClusterKey(context.Background()); err != nil {
			registrationFailed <- err
			return
		}
		logger.Println("Self-bootstrap registration complete.")
		close(registrationDone)
	}()

	approved := false
	for i := 0; i < 10; i++ {
		select {
		case err := <-registrationFailed:
			return fmt.Errorf("Self-bootstrap registration failed: %v", err)
		case <-registrationDone:
			approved = true
			break
		default:
			pending, err := cl.Secrets().ListPendingRegistrations(ctx)
			if err != nil {
				logger.Printf("Self-bootstrap: could not query pending registrations: %v", err)
				break
			}
			for _, reg := range pending {
				if reg.NodeID == cl.Secrets().NodeId() {
					if err := approveSelf(cl, ctx, cfg.Secrets.ClusterKey); err != nil {
						logger.Fatalf("Self-bootstrap failed: %v", err)
					}
					logger.Println("Self-bootstrap successful")
					approved = true
					goto done
				}
			}
		}
		if approved {
			goto done
		}
		time.Sleep(300 * time.Millisecond)
	}
done:

	if !approved {
		return fmt.Errorf("Self-bootstrap failed to register after 3s")
	}

	return nil
}

func approveSelf(cl cluster.Cluster, ctx context.Context, clusterKeyB64 string) error {
	var clusterKey [32]byte

	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(clusterKeyB64)))
	if err != nil {
		return err
	}

	if len(raw) != 32 {
		return fmt.Errorf("invalid cluster key length: got %d, want 32", len(raw))
	}

	copy(clusterKey[:], raw)
	cl.Secrets().SetClusterKey(clusterKey)

	return cl.Secrets().ApproveNode(ctx, cl.Secrets().NodeId())
}
