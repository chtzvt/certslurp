package sink

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/chtzvt/certslurp/internal/compression"
	"github.com/chtzvt/certslurp/internal/secrets"
)

type AzureBlobSink struct {
	account     string
	container   string
	prefix      string
	compression string
	secrets     *secrets.Store
}

func NewAzureBlobSink(opts map[string]interface{}, secrets *secrets.Store) (Sink, error) {
	account, _ := opts["account"].(string)
	container, _ := opts["container"].(string)
	prefix, _ := opts["prefix"].(string)
	compression, _ := opts["compression"].(string)
	if account == "" || container == "" {
		return nil, fmt.Errorf("azureblob sink requires 'account' and 'container' options")
	}
	return &AzureBlobSink{
		account:     account,
		container:   container,
		prefix:      prefix,
		compression: compression,
		secrets:     secrets,
	}, nil
}

func (a *AzureBlobSink) Open(ctx context.Context, name string) (SinkWriter, error) {
	// 1. Get credentials from secrets
	key, err := a.secrets.Get(ctx, "AZURE_STORAGE_KEY")
	if err != nil {
		return nil, fmt.Errorf("missing AZURE_STORAGE_KEY in secrets: %w", err)
	}
	// 2. Connect to blob service and get container client
	cred, err := azblob.NewSharedKeyCredential(a.account, string(key))
	if err != nil {
		return nil, fmt.Errorf("azure shared key credential error: %w", err)
	}
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", a.account)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azure blob client init error: %w", err)
	}
	blobName := a.prefix + name

	blobClient := client.ServiceClient().NewContainerClient(a.container).NewBlockBlobClient(blobName)

	pr, pw := io.Pipe()
	// 3. Start upload in background
	go func() {
		defer pr.Close()
		_, err := blobClient.UploadStream(ctx, pr, nil)
		if err != nil {
			_ = pr.CloseWithError(err)
		}
	}()
	// 4. Optionally wrap in compression
	w, err := compression.NewWriter(pw, a.compression)
	if err != nil {
		return nil, err
	}
	return &pipeSinkWriter{Writer: w, Closer: pw}, nil
}

func init() {
	Register("azureblob", NewAzureBlobSink)
}
