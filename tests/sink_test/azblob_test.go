package sink_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/chtzvt/certslurp/internal/sink"
	"github.com/stretchr/testify/require"
)

type mockBlockBlobAPI struct {
	called     bool
	lastKey    string
	lastBody   []byte
	returnErr  error
	uploadLock *sync.WaitGroup
}

func (m *mockBlockBlobAPI) UploadStream(ctx context.Context, body io.Reader, options *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error) {
	defer m.uploadLock.Done()
	m.called = true
	content, _ := io.ReadAll(body)
	m.lastBody = content
	return azblob.UploadStreamResponse{}, m.returnErr
}

func TestAzureBlobSink_UploadStream_MemoryAndDisk(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "TEST_AZURE_KEY", []byte("LPgXjG4nUZAeO7BIAXtgrYitCq9fyEOWDzC320EJEz2bBPJyF9db5kpaKHplyL6CR90H8dRmtumL+AStK+vO3Q==")))

	for _, bufferType := range []string{"memory", "disk"} {
		t.Run(bufferType, func(t *testing.T) {
			wg := &sync.WaitGroup{}
			wg.Add(1)
			mock := &mockBlockBlobAPI{uploadLock: wg}

			opts := map[string]interface{}{
				"account":           "faketestaccount",
				"container":         "testcontainer",
				"prefix":            "testing/",
				"compression":       "none",
				"access_key_secret": "TEST_AZURE_KEY",
				"buffer_type":       bufferType,
			}

			sinkIface, err := sink.NewAzureBlobSink(opts, store)
			require.NoError(t, err)
			azSink := sinkIface.(*sink.AzureBlobSink)
			azSink.Client = mock

			writer, err := azSink.Open(ctx, "testblob.txt")
			require.NoError(t, err)

			payload := []byte("azureblob test payload")
			n, err := writer.Write(payload)
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
			require.NoError(t, writer.Close())

			wg.Wait()

			require.True(t, mock.called)
			require.Equal(t, payload, mock.lastBody)
		})
	}
}

func TestBuildBlobKey(t *testing.T) {
	cases := []struct {
		prefix, name, want string
	}{
		{"foo/", "bar.txt", "foo/bar.txt"},
		{"foo", "bar.txt", "foo/bar.txt"},
		{"foo//", "/bar.txt", "foo/bar.txt"},
		{"", "bar.txt", "bar.txt"},
		{"/", "bar.txt", "bar.txt"},
		{"foo/bar", "baz.txt", "foo/bar/baz.txt"},
	}
	for _, c := range cases {
		got := sink.BuildBlobKey(c.prefix, c.name)
		require.Equal(t, c.want, got)
	}
}

func TestAzureBlobSink_Compression_Gzip(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "TEST_AZURE_KEY", []byte("LPgXjG4nUZAeO7BIAXtgrYitCq9fyEOWDzC320EJEz2bBPJyF9db5kpaKHplyL6CR90H8dRmtumL+AStK+vO3Q==")))

	wg := &sync.WaitGroup{}
	wg.Add(1)
	mock := &mockBlockBlobAPI{uploadLock: wg}

	opts := map[string]interface{}{
		"account":           "faketestaccount",
		"container":         "testcontainer",
		"prefix":            "testing/",
		"compression":       "gzip",
		"access_key_secret": "TEST_AZURE_KEY",
	}

	sinkIface, err := sink.NewAzureBlobSink(opts, store)
	require.NoError(t, err)
	azSink := sinkIface.(*sink.AzureBlobSink)
	azSink.Client = mock

	writer, err := azSink.Open(ctx, "gzipped.txt")
	require.NoError(t, err)

	payload := []byte("gzip payload for azureblob")
	_, err = writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	wg.Wait()

	// Decompress and verify content
	require.True(t, mock.called)
	r, err := gzip.NewReader(bytes.NewReader(mock.lastBody))
	require.NoError(t, err)
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, payload, out)
}
