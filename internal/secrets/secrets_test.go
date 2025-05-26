package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/ctsnarf/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
)

func TestSetAndGet(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()

	testKey := "test-secret"
	testValue := []byte("supersecret")

	// Set secret
	if err := store.Set(ctx, testKey, testValue); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// Get secret
	got, err := store.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(got) != string(testValue) {
		t.Errorf("Secret mismatch: got %q, want %q", got, testValue)
	}
}

func TestGetNotFound(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()
	_, err := store.Get(ctx, "not-a-real-key")
	if err == nil {
		t.Error("Expected error for non-existent secret, got nil")
	}
}

func TestSecretOverwrite(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()
	key := "overwrite"
	val1 := []byte("v1")
	val2 := []byte("v2")
	require.NoError(t, store.Set(ctx, key, val1))
	got, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val1, got)

	require.NoError(t, store.Set(ctx, key, val2))
	got, err = store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val2, got)
}

func TestSecretDelete(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()
	key := "del"
	val := []byte("gone")
	require.NoError(t, store.Set(ctx, key, val))
	got, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, val, got)

	// Simulate delete
	_, err = store.etcd.Delete(ctx, "/ctsnarf/secrets/store/"+key)
	require.NoError(t, err)
	_, err = store.Get(ctx, key)
	require.Error(t, err)
}

func TestSecretList(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()
	keys := []string{"a", "b", "c/d", "d"}
	for _, k := range keys {
		require.NoError(t, store.Set(ctx, k, []byte(k+"-val")))
	}
	listed, err := store.List(ctx, "")
	require.NoError(t, err)
	// Should contain at least the keys just written (may also have others if other tests run in same process)
	for _, k := range keys {
		found := false
		for _, got := range listed {
			if got == k {
				found = true
			}
		}
		if !found {
			t.Errorf("Expected key %q in list", k)
		}
	}
}

func TestSecretConcurrency(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()
	n := 20
	keys := make([]string, n)
	for i := range keys {
		keys[i] = base64.StdEncoding.EncodeToString([]byte{byte(i)})
	}
	var wg sync.WaitGroup
	for i, k := range keys {
		wg.Add(1)
		go func(k string, i int) {
			defer wg.Done()
			data := []byte{byte(i), 42}
			for j := 0; j < 3; j++ {
				require.NoError(t, store.Set(ctx, k, data))
				got, err := store.Get(ctx, k)
				require.NoError(t, err)
				require.Equal(t, data, got)
			}
		}(k, i)
	}
	wg.Wait()
}

func TestSecretEmptyValue(t *testing.T) {
	store := SetupTestStore(t)
	ctx := context.TODO()
	key := "empty"
	require.NoError(t, store.Set(ctx, key, []byte{}))
	got, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, 0, len(got))
}

func TestBootstrapRegistrationFlow(t *testing.T) {
	cluster, cleanup := testutil.SetupEtcdCluster(t)
	t.Cleanup(cleanup)

	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)
	keyPath := tempDir + "/node_key"

	// Node will register and block waiting for admin approval
	store, err := NewStore(cluster.Client(), keyPath)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error)
	go func() {
		done <- store.RegisterAndWaitForClusterKey(ctx)
	}()

	// Admin approves after a short delay
	time.Sleep(300 * time.Millisecond)
	// Simulate admin creating cluster key
	var clusterKey [32]byte
	_, _ = rand.Read(clusterKey[:])
	sealed, _ := box.SealAnonymous(nil, clusterKey[:], &store.keys.Public, rand.Reader)
	_, err = cluster.Client().Put(context.TODO(), "/ctsnarf/secrets/keys/"+store.nodeID, base64.StdEncoding.EncodeToString(sealed))
	require.NoError(t, err)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for bootstrap registration")
	}

	// Should now be able to set/get secrets
	require.NoError(t, store.Set(context.TODO(), "bootstrap", []byte("foo")))
	got, err := store.Get(context.TODO(), "bootstrap")
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), got)
}

func TestBootstrapRegistrationTimeout(t *testing.T) {
	cluster, cleanup := testutil.SetupEtcdCluster(t)
	t.Cleanup(cleanup)
	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)
	keyPath := tempDir + "/node_key"

	store, err := NewStore(cluster.Client(), keyPath)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = store.RegisterAndWaitForClusterKey(ctx)
	require.Error(t, err) // Should be context.DeadlineExceeded
}
