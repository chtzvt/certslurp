package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/chtzvt/certslurp/internal/secrets"
)

type AzureBlobSink struct {
	account       string
	container     string
	prefix        string
	accessKeyName string
	secrets       *secrets.Store
	bufferType    string
	Client        BlockBlobAPI
}

type azureBlobSinkWriter struct {
	ctx        context.Context
	blobClient BlockBlobAPI
	buf        *bytes.Buffer // for memory
	file       *os.File      // for disk
	closer     io.Closer
	diskMode   bool
}

func (w *azureBlobSinkWriter) Write(p []byte) (int, error) {
	if w.diskMode {
		return w.file.Write(p)
	}
	return w.buf.Write(p)
}

func (w *azureBlobSinkWriter) Close() error {
	defer w.closer.Close()
	var reader io.ReadSeeker
	if w.diskMode {
		defer os.Remove(w.file.Name())
		if _, err := w.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		reader = w.file
	} else {
		reader = bytes.NewReader(w.buf.Bytes())
	}
	_, err := w.blobClient.UploadStream(w.ctx, reader, nil)
	return err
}

type BlockBlobAPI interface {
	UploadStream(ctx context.Context, body io.Reader, options *azblob.UploadStreamOptions) (azblob.UploadStreamResponse, error)
}

func NewAzureBlobSink(opts map[string]interface{}, secrets *secrets.Store) (Sink, error) {
	account, _ := opts["account"].(string)
	container, _ := opts["container"].(string)
	if account == "" || container == "" {
		return nil, fmt.Errorf("azureblob sink requires 'account' and 'container' options")
	}
	prefix, _ := opts["prefix"].(string)
	accessKeyName, _ := opts["access_key_secret"].(string)
	bufferType := "memory"
	if v, ok := opts["buffer_type"].(string); ok && v == "disk" {
		bufferType = "disk"
	}

	return &AzureBlobSink{
		account:       account,
		container:     container,
		prefix:        prefix,
		accessKeyName: accessKeyName,
		secrets:       secrets,
		bufferType:    bufferType,
	}, nil
}

func (a *AzureBlobSink) Open(ctx context.Context, name string) (SinkWriter, error) {
	key, err := a.secrets.Get(ctx, a.accessKeyName)
	if err != nil {
		return nil, fmt.Errorf("missing Azure Blob Storage access key '%s' in secrets: %w", a.accessKeyName, err)
	}
	cred, err := azblob.NewSharedKeyCredential(a.account, strings.TrimSpace(string(key)))
	if err != nil {
		return nil, fmt.Errorf("azure shared key credential error: %w", err)
	}
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", a.account)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure blob client init error: %w", err)
	}

	blobName := BuildBlobKey(a.prefix, name)

	var blobClient BlockBlobAPI
	if a.Client != nil {
		blobClient = a.Client
	} else {
		blobClient = client.ServiceClient().NewContainerClient(a.container).NewBlockBlobClient(blobName)
	}

	var file *os.File
	var buf *bytes.Buffer
	var closer io.Closer

	diskMode := a.bufferType == "disk"
	if diskMode {
		f, err := os.CreateTemp("", "certslurp-azblob-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}
		file = f
		closer = file
	} else {
		b := &bytes.Buffer{}
		buf = b
		closer = nopCloser{}
	}

	return &azureBlobSinkWriter{
		ctx:        ctx,
		blobClient: blobClient,
		buf:        buf,
		file:       file,
		closer:     closer,
		diskMode:   diskMode,
	}, nil
}

func BuildBlobKey(prefix, name string) string {
	prefix = strings.Trim(prefix, "/")
	name = strings.TrimLeft(name, "/")
	if prefix == "" {
		return name
	}
	return path.Join(prefix, name)
}

func init() {
	Register("azureblob", NewAzureBlobSink)
}
