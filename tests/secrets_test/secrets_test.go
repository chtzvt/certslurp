package secrets_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
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
	_, err = store.Client().Delete(ctx, "/certslurp/secrets/store/"+key)
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
	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	t.Cleanup(cleanup)

	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)
	keyPath := tempDir + "/node_key"

	// Node will register and block waiting for admin approval
	store, err := secrets.NewStore(cluster.Client(), keyPath, "/certslurp")
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
	pubKey := store.PublicKey()
	sealed, _ := box.SealAnonymous(nil, clusterKey[:], &pubKey, rand.Reader)
	_, err = cluster.Client().Put(context.TODO(), "/certslurp/secrets/keys/"+store.NodeId(), base64.StdEncoding.EncodeToString(sealed))
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
	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	t.Cleanup(cleanup)
	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)
	keyPath := tempDir + "/node_key"

	store, err := secrets.NewStore(cluster.Client(), keyPath, "/certslurp")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err = store.RegisterAndWaitForClusterKey(ctx)
	require.Error(t, err) // Should be context.DeadlineExceeded
}

func TestGenerateClusterKey(t *testing.T) {
	key, err := secrets.GenerateClusterKey()
	require.NoError(t, err)
	require.Len(t, key, 32)
	// Should be nonzero
	var zero [32]byte
	require.NotEqual(t, zero, key)
}

func TestListPendingRegistrationsAndApproveNode(t *testing.T) {
	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	t.Cleanup(cleanup)

	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)
	keyPath := tempDir + "/node_key"

	// Register a new Store (node), which should register itself as pending
	store, err := secrets.NewStore(cluster.Client(), keyPath, cluster.Prefix())
	require.NoError(t, err)
	ctx := context.TODO()
	// Register node (does not block, for test we just do the put directly)
	pubKey := store.PublicKey()
	privKey := store.PrivateKey()

	pubB64 := base64.StdEncoding.EncodeToString(pubKey[:])
	_, err = cluster.Client().Put(ctx, cluster.Prefix()+"/registration/pending/"+store.NodeId(), pubB64)
	require.NoError(t, err)

	// Now list pending registrations
	pending, err := store.ListPendingRegistrations(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, pending)
	found := false
	for _, reg := range pending {
		if reg.NodeID == store.NodeId() && reg.PubKeyB64 == pubB64 {
			found = true
		}
	}
	require.True(t, found, "pending registration for this node should exist")

	// Generate cluster key and approve node
	clusterKey, err := secrets.GenerateClusterKey()
	require.NoError(t, err)
	require.Len(t, clusterKey, 32)

	store.SetClusterKey(clusterKey)

	require.NoError(t, store.ApproveNode(ctx, store.NodeId()))

	// Pending registration should be gone
	pending2, err := store.ListPendingRegistrations(ctx)
	require.NoError(t, err)
	for _, reg := range pending2 {
		require.NotEqual(t, store.NodeId(), reg.NodeID, "pending registration should have been removed")
	}

	// Should be able to fetch the encrypted key in /secrets/keys/
	resp, err := cluster.Client().Get(ctx, cluster.Prefix()+"/secrets/keys/"+store.NodeId())
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs)

	// Node should be able to retrieve and decrypt the cluster key (simulate full join)
	// Simulate RegisterAndWaitForClusterKey (call only the key-read/decrypt logic)
	sealed, _ := base64.StdEncoding.DecodeString(string(resp.Kvs[0].Value))

	plain, ok := box.OpenAnonymous(nil, sealed, &pubKey, &privKey)
	require.True(t, ok, "should be able to decrypt cluster key")
	require.Equal(t, clusterKey[:], plain)
}

func TestSetSealed_Success(t *testing.T) {
	cluster, cleanup := testcluster.SetupEtcdCluster(t)
	t.Cleanup(cleanup)
	tempDir, cleanup2 := testutil.SetupTempDir(t)
	t.Cleanup(cleanup2)
	keyPath := tempDir + "/node_key"

	store, err := secrets.NewStore(cluster.Client(), keyPath, "/certslurp")
	require.NoError(t, err)

	key := "foo"
	val := []byte("topsecret")
	ctx := context.Background()

	err = store.SetSealed(ctx, key, val)
	require.NoError(t, err)

	// Verify directly in etcd
	resp, err := cluster.Client().Get(ctx, "/certslurp/secrets/store/"+key)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Kvs)
	got := string(resp.Kvs[0].Value)
	want := base64.StdEncoding.EncodeToString(val)
	assert.Equal(t, want, got)
}

func TestSetSealed_ErrorFromEtcd(t *testing.T) {
	mock := &fakeEtcdClient{putErr: errors.New("nope")}
	store := &testStore{etcd: mock, prefix: "p"}
	err := store.SetSealed(context.Background(), "foo", []byte("bar"))
	assert.ErrorContains(t, err, "nope")
}

func TestEncryptValue_DecryptsToOriginal(t *testing.T) {
	var key [32]byte
	copy(key[:], []byte("12345678901234567890123456789012"))

	val := []byte("sensitive-data")
	cipher := secrets.EncryptValue(key, val)

	require.Len(t, cipher, 24+secretbox.Overhead+len(val), "should prepend 24-byte nonce and 16-byte MAC")

	var nonce [24]byte
	copy(nonce[:], cipher[:24])
	opened, ok := secretbox.Open(nil, cipher[24:], &nonce, &key)
	require.True(t, ok, "should decrypt with correct key")
	assert.Equal(t, val, opened)
}

func TestEncryptValue_DifferentCiphertexts(t *testing.T) {
	var key [32]byte
	copy(key[:], []byte("12345678901234567890123456789012"))

	val := []byte("repeatme")
	c1 := secrets.EncryptValue(key, val)
	c2 := secrets.EncryptValue(key, val)
	assert.NotEqual(t, c1, c2, "nonce should make ciphertexts different each time")
}
