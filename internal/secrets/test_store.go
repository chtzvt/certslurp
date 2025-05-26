package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"path/filepath"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/testutil"
	"golang.org/x/crypto/nacl/box"
)

func SetupTestStore(t *testing.T) *Store {
	t.Helper()

	// Start embedded etcd and get cleanup
	cluster, cleanup := testutil.SetupEtcdCluster(t)
	t.Cleanup(cleanup)

	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)

	keyPath := filepath.Join(tempDir, "test_node_key")
	store, err := NewStore(cluster.Client(), keyPath)
	if err != nil {
		t.Fatalf("Failed to create Store: %v", err)
	}
	// Simulate admin approval for bootstrap (direct cluster key approval)
	var clusterKey [32]byte
	_, _ = rand.Read(clusterKey[:])
	sealed, _ := box.SealAnonymous(nil, clusterKey[:], &store.keys.Public, rand.Reader)
	_, err = cluster.Client().Put(context.TODO(), "/ctsnarf/secrets/keys/"+store.nodeID, base64.StdEncoding.EncodeToString(sealed))
	if err != nil {
		t.Fatalf("Failed to put cluster key: %v", err)
	}
	store.clusterK = clusterKey
	return store
}
