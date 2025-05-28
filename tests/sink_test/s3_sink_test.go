package sink_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/chtzvt/certslurp/internal/secrets"
	"github.com/chtzvt/certslurp/internal/sink"
	"github.com/chtzvt/certslurp/internal/testcluster"
	"github.com/chtzvt/certslurp/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
)

type mockPutObjectAPI struct {
	called    bool
	lastKey   string
	lastBody  []byte
	returnErr error
	wg        *sync.WaitGroup
}

func (m *mockPutObjectAPI) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	defer m.wg.Done() // signal done
	m.called = true
	m.lastKey = *params.Key
	body, _ := io.ReadAll(params.Body)
	m.lastBody = body
	return &s3.PutObjectOutput{}, m.returnErr
}

func setupTestStore(t *testing.T) *secrets.Store {
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

func TestS3Sink_PutObject(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "AWS_ACCESS_KEY_ID", []byte("fake-access")))
	require.NoError(t, store.Set(ctx, "AWS_SECRET_ACCESS_KEY", []byte("fake-secret")))

	wg := &sync.WaitGroup{}
	wg.Add(1)
	mock := &mockPutObjectAPI{wg: wg}

	opts := map[string]interface{}{
		"bucket": "mybucket",
		"region": "us-west-2",
		"prefix": "prefix/",
	}

	sinkIface, err := sink.NewS3Sink(opts, store)
	require.NoError(t, err)
	sink := sinkIface.(*sink.S3Sink)
	sink.Client = mock

	w, err := sink.Open(ctx, "testfile.txt")
	require.NoError(t, err)
	payload := []byte("s3 test payload")
	n, err := w.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.NoError(t, w.Close())

	// Wait for the PutObject goroutine to finish
	wg.Wait()

	require.True(t, mock.called)
	require.Equal(t, "prefix/testfile.txt", mock.lastKey)
	require.Equal(t, payload, mock.lastBody)
}
