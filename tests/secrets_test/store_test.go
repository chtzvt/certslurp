package secrets_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"path/filepath"
	"testing"

	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"golang.org/x/crypto/nacl/box"
)

func SetupTestStore(t *testing.T) *secrets.Store {
	t.Helper()

	// Start embedded etcd and get cleanup
	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	t.Cleanup(cleanup)

	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)

	keyPath := filepath.Join(tempDir, "test_node_key")
	store, err := secrets.NewStore(cluster.Client(), keyPath, "/certslurp")
	if err != nil {
		t.Fatalf("Failed to create Store: %v", err)
	}
	// Simulate admin approval for bootstrap (direct cluster key approval)
	var clusterKey [32]byte
	_, _ = rand.Read(clusterKey[:])
	pubKey := store.PublicKey()
	sealed, _ := box.SealAnonymous(nil, clusterKey[:], &pubKey, rand.Reader)
	_, err = cluster.Client().Put(context.TODO(), "/certslurp/secrets/keys/"+store.NodeId(), base64.StdEncoding.EncodeToString(sealed))
	if err != nil {
		t.Fatalf("Failed to put cluster key: %v", err)
	}
	store.SetClusterKey(clusterKey)
	return store
}
