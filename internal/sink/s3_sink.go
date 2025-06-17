package sink

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/chtzvt/certslurp/internal/compression"
	"github.com/chtzvt/certslurp/internal/secrets"
)

type S3Sink struct {
	bucket             string
	prefix             string
	region             string
	compression        string
	accessKeyIDName    string
	secretAccesKeyName string
	secrets            *secrets.Store
	endpoint           string
	Client             PutObjectAPI // test only; nil in prod, set by test
	disableChecksums   bool
	bufferType         string
}

// PutObjectAPI abstracts the S3 PutObject method (for testing)
type PutObjectAPI interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

type s3SinkWriter struct {
	ctx      context.Context
	client   PutObjectAPI
	bucket   string
	key      string
	buf      *bytes.Buffer // nil if disk
	file     *os.File      // nil if memory
	comp     io.WriteCloser
	closer   io.Closer
	diskMode bool
}

func (w *s3SinkWriter) Write(p []byte) (int, error) {
	return w.comp.Write(p)
}

func (w *s3SinkWriter) Close() error {
	if err := w.comp.Close(); err != nil {
		return err
	}
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

	_, err := w.client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: &w.bucket,
		Key:    &w.key,
		Body:   reader,
	})

	return err
}

func NewS3Sink(opts map[string]interface{}, secrets *secrets.Store) (Sink, error) {
	bucket, _ := opts["bucket"].(string)
	prefix, _ := opts["prefix"].(string)
	region, _ := opts["region"].(string)

	accessKeyIDName, _ := opts["access_key_id_secret"].(string)
	secretAccesKeyName, _ := opts["access_key_secret"].(string)

	compression, _ := opts["compression"].(string)

	endpoint, _ := opts["endpoint"].(string)
	baseEndpoint, _ := opts["base_endpoint"].(string)

	bufferType := "memory"
	if v, ok := opts["buffer_type"].(string); ok && v == "disk" {
		bufferType = "disk"
	}

	var disableChecksums bool
	if v, ok := opts["disable_checksums"]; ok {
		disableChecksums = toBool(v)
	}

	if bucket == "" || region == "" {
		return nil, fmt.Errorf("s3 sink requires 'bucket' and 'region' options")
	}

	return &S3Sink{
		bucket:             bucket,
		prefix:             prefix,
		region:             region,
		compression:        compression,
		accessKeyIDName:    accessKeyIDName,
		secretAccesKeyName: secretAccesKeyName,
		secrets:            secrets,
		endpoint:           chooseS3Endpoint(endpoint, baseEndpoint),
		disableChecksums:   disableChecksums,
		bufferType:         bufferType,
	}, nil
}

func (s *S3Sink) Open(ctx context.Context, name string) (SinkWriter, error) {
	accessKey, err := s.secrets.Get(ctx, s.accessKeyIDName)
	if err != nil {
		return nil, fmt.Errorf("missing AWS Access Key ID credential '%s': %w", s.accessKeyIDName, err)
	}
	secretKey, err := s.secrets.Get(ctx, s.secretAccesKeyName)
	if err != nil {
		return nil, fmt.Errorf("missing AWS Secret Access Key credential '%s': %w", s.secretAccesKeyName, err)
	}

	awsCfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(s.region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(strings.TrimSpace(string(accessKey)), strings.TrimSpace(string(secretKey)), ""),
		),
	}

	if s.disableChecksums {
		awsCfgOpts = append(awsCfgOpts, config.WithRequestChecksumCalculation(0))
		awsCfgOpts = append(awsCfgOpts, config.WithResponseChecksumValidation(0))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, awsCfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("aws config load error: %w", err)
	}
	s3Opts := []func(*s3.Options){}
	if s.endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &s.endpoint
		})
	}

	var client PutObjectAPI
	if s.Client != nil {
		client = s.Client // test: injected
	} else {
		client = s3.NewFromConfig(awsCfg, s3Opts...)
	}

	key := BuildS3Key(s.prefix, name)

	var bufWriter io.Writer
	var file *os.File
	var buf *bytes.Buffer
	var closer io.Closer

	if s.bufferType == "disk" {
		f, err := os.CreateTemp("", "certslurp-s3-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp file: %w", err)
		}
		file = f
		bufWriter = file
		closer = file
	} else {
		b := &bytes.Buffer{}
		buf = b
		bufWriter = buf
		closer = io.NopCloser(nil) // no-op closer for memory
	}

	comp, err := compression.NewWriter(bufWriter, s.compression)
	if err != nil {
		if file != nil {
			file.Close()
			os.Remove(file.Name())
		}
		return nil, err
	}

	return &s3SinkWriter{
		ctx:      ctx,
		client:   client,
		bucket:   s.bucket,
		key:      key,
		buf:      buf,
		file:     file,
		comp:     comp,
		closer:   closer,
		diskMode: s.bufferType == "disk",
	}, nil
}

func chooseS3Endpoint(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func BuildS3Key(prefix, name string) string {
	prefix = strings.Trim(prefix, "/") // remove leading/trailing slashes
	name = strings.TrimLeft(name, "/") // remove leading slash
	if prefix == "" {
		return name
	}
	return path.Join(prefix, name)
}

func init() {
	Register("s3", NewS3Sink)
}
