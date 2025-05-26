package sink

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/chtzvt/ctsnarf/internal/secrets"
	"github.com/stretchr/testify/require"
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

func TestS3Sink_PutObject(t *testing.T) {
	store := secrets.SetupTestStore(t)
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

	sinkIface, err := NewS3Sink(opts, store)
	require.NoError(t, err)
	sink := sinkIface.(*S3Sink)
	sink.client = mock

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
