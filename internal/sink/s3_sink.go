package sink

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/chtzvt/certslurp/internal/compression"
	"github.com/chtzvt/certslurp/internal/secrets"
)

type S3Sink struct {
	bucket           string
	prefix           string
	region           string
	compression      string
	secrets          *secrets.Store
	endpoint         string
	Client           PutObjectAPI // test only; nil in prod, set by test
	disableChecksums bool
}

// PutObjectAPI abstracts the S3 PutObject method (for testing)
type PutObjectAPI interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func NewS3Sink(opts map[string]interface{}, secrets *secrets.Store) (Sink, error) {
	bucket, _ := opts["bucket"].(string)
	prefix, _ := opts["prefix"].(string)
	region, _ := opts["region"].(string)
	compression, _ := opts["compression"].(string)
	endpoint, _ := opts["endpoint"].(string)
	baseEndpoint, _ := opts["base_endpoint"].(string) // support both for flexibility

	// Checksum toggles
	var disableChecksums bool
	if v, ok := opts["disable_checksums"]; ok {
		disableChecksums = toBool(v)
	}

	if bucket == "" || region == "" {
		return nil, fmt.Errorf("s3 sink requires 'bucket' and 'region' options")
	}

	return &S3Sink{
		bucket:           bucket,
		prefix:           prefix,
		region:           region,
		compression:      compression,
		secrets:          secrets,
		endpoint:         chooseEndpoint(endpoint, baseEndpoint),
		disableChecksums: disableChecksums,
	}, nil
}

// Helper to select which endpoint to use
func chooseEndpoint(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// Helper to support bool/int/bool-string conversion
func toBool(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case string:
		return v == "1" || v == "true" || v == "on"
	default:
		return false
	}
}

func (s *S3Sink) Open(ctx context.Context, name string) (SinkWriter, error) {
	accessKey, err := s.secrets.Get(ctx, "AWS_ACCESS_KEY_ID")
	if err != nil {
		return nil, fmt.Errorf("missing AWS_ACCESS_KEY_ID: %w", err)
	}
	secretKey, err := s.secrets.Get(ctx, "AWS_SECRET_ACCESS_KEY")
	if err != nil {
		return nil, fmt.Errorf("missing AWS_SECRET_ACCESS_KEY: %w", err)
	}
	awsCfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(s.region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(string(accessKey), string(secretKey), ""),
		),
	}

	// Set checksum config
	if s.disableChecksums {
		awsCfgOpts = append(awsCfgOpts, config.WithRequestChecksumCalculation(0))
		awsCfgOpts = append(awsCfgOpts, config.WithResponseChecksumValidation(0))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, awsCfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("aws config load error: %w", err)
	}
	// S3 client options
	s3Opts := []func(*s3.Options){}
	if s.endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &s.endpoint
		})
	}

	var client PutObjectAPI

	if s.Client != nil {
		client = s.Client // use injected client for testing
	} else {
		client = s3.NewFromConfig(awsCfg, s3Opts...)
	}

	key := s.prefix + name
	pr, pw := io.Pipe()
	go func() {
		defer pr.Close()
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &s.bucket,
			Key:    &key,
			Body:   pr,
		})
		if err != nil {
			_ = pr.CloseWithError(err)
		}
	}()
	w, err := compression.NewWriter(pw, s.compression)
	if err != nil {
		return nil, err
	}
	return &pipeSinkWriter{Writer: w, Closer: pw}, nil
}

func init() {
	Register("s3", NewS3Sink)
}
