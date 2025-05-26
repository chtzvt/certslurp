package etl

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	"github.com/chtzvt/ctsnarf/internal/extractor"
	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/chtzvt/ctsnarf/internal/secrets"
	"github.com/chtzvt/ctsnarf/internal/sink"
	"github.com/chtzvt/ctsnarf/internal/transformer"
	ct "github.com/google/certificate-transparency-go"
	"github.com/stretchr/testify/require"
)

// --- Fake implementations for test ---

type fakeExtractor struct{}

func (f *fakeExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	return map[string]interface{}{"val": string(raw.Cert.Data)}, nil
}

type fakeTransformer struct{}

func (f *fakeTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	return []byte(fmt.Sprintf("%s\n", data["val"])), nil
}
func (f *fakeTransformer) Header(ctx *etl_core.Context) ([]byte, error) { return nil, nil }
func (f *fakeTransformer) Footer(ctx *etl_core.Context) ([]byte, error) { return nil, nil }

type record struct {
	Name string
	Data []byte
}
type mockSink struct {
	Chunks []record
}
type mockWriter struct {
	name   string
	sink   *mockSink
	buf    bytes.Buffer
	closed bool
}

func (m *mockSink) Open(ctx context.Context, name string) (sink.SinkWriter, error) {
	return &mockWriter{name: name, sink: m}, nil
}
func (w *mockWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *mockWriter) Close() error {
	if !w.closed {
		w.sink.Chunks = append(w.sink.Chunks, record{Name: w.name, Data: w.buf.Bytes()})
		w.closed = true
	}
	return nil
}

// --- Actual test ---

func TestPipeline_ChunkingByRecordsAndBytes(t *testing.T) {
	// Register fakes
	extractor.Register("fake", &fakeExtractor{})
	transformer.Register("fake", &fakeTransformer{})

	// Register mock sink
	ms := &mockSink{}
	sink.Register("mock", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return ms, nil
	})

	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:    "fake",
				Transformer:  "fake",
				Sink:         "mock",
				SinkOptions:  nil,
				ChunkRecords: 3, // Chunk after 3 records
				ChunkBytes:   6, // Or after 6 bytes
			},
		},
	}
	secretsStore := &secrets.Store{} // unused by mockSink

	pipeline, err := NewPipeline(spec, secretsStore, "testfile")
	require.NoError(t, err)

	// Prepare 7 entries, with string data "0", "1", ..., "6"
	entries := make(chan *ct.RawLogEntry, 10)
	for i := 0; i < 7; i++ {
		entries <- &ct.RawLogEntry{
			Index: int64(i),
			Cert:  ct.ASN1Cert{Data: []byte(strconv.Itoa(i))},
		}
	}
	close(entries)

	err = pipeline.StreamProcess(context.Background(), entries)
	require.NoError(t, err)

	// Should create 3 chunks:
	// - 1st chunk: "0\n1\n2\n" (6 bytes, chunk by byte)
	// - 2nd chunk: "3\n4\n5\n" (6 bytes)
	// - 3rd chunk: "6\n"
	require.Len(t, ms.Chunks, 3)
	require.Contains(t, ms.Chunks[0].Name, "testfile.0001")
	require.Contains(t, ms.Chunks[1].Name, "testfile.0002")
	require.Contains(t, ms.Chunks[2].Name, "testfile.0003")

	require.Equal(t, "0\n1\n2\n", string(ms.Chunks[0].Data))
	require.Equal(t, "3\n4\n5\n", string(ms.Chunks[1].Data))
	require.Equal(t, "6\n", string(ms.Chunks[2].Data))
}

func TestPipeline_EmptyInput(t *testing.T) {
	extractor.Register("fake-empty", &fakeExtractor{})
	transformer.Register("fake-empty", &fakeTransformer{})
	ms := &mockSink{}
	sink.Register("mock-empty", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return ms, nil
	})

	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:   "fake-empty",
				Transformer: "fake-empty",
				Sink:        "mock-empty",
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "empty")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry)
	close(entries)

	err = pipeline.StreamProcess(context.Background(), entries)
	require.NoError(t, err)
	require.Len(t, ms.Chunks, 0)
}

func TestPipeline_SingleRecord(t *testing.T) {
	extractor.Register("fake-single", &fakeExtractor{})
	transformer.Register("fake-single", &fakeTransformer{})
	ms := &mockSink{}
	sink.Register("mock-single", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return ms, nil
	})

	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:   "fake-single",
				Transformer: "fake-single",
				Sink:        "mock-single",
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "single")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry, 1)
	entries <- &ct.RawLogEntry{Cert: ct.ASN1Cert{Data: []byte("solo")}}
	close(entries)

	err = pipeline.StreamProcess(context.Background(), entries)
	require.NoError(t, err)
	require.Len(t, ms.Chunks, 1)
	require.Contains(t, ms.Chunks[0].Name, "single")
	require.Equal(t, "solo\n", string(ms.Chunks[0].Data))
}

func TestPipeline_ChunkByBytesOnly(t *testing.T) {
	extractor.Register("fake-bytes", &fakeExtractor{})
	transformer.Register("fake-bytes", &fakeTransformer{})
	ms := &mockSink{}
	sink.Register("mock-bytes", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return ms, nil
	})

	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:   "fake-bytes",
				Transformer: "fake-bytes",
				Sink:        "mock-bytes",
				ChunkBytes:  4, // Each record is "x\n" = 2 bytes, so chunk every 2 records
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "bytes")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry, 4)
	for i := 0; i < 4; i++ {
		entries <- &ct.RawLogEntry{Cert: ct.ASN1Cert{Data: []byte(strconv.Itoa(i))}}
	}
	close(entries)

	err = pipeline.StreamProcess(context.Background(), entries)
	require.NoError(t, err)
	require.Len(t, ms.Chunks, 2)
	require.Equal(t, "0\n1\n", string(ms.Chunks[0].Data))
	require.Equal(t, "2\n3\n", string(ms.Chunks[1].Data))
}

func TestPipeline_ChunkByRecordsOnly(t *testing.T) {
	extractor.Register("fake-recs", &fakeExtractor{})
	transformer.Register("fake-recs", &fakeTransformer{})
	ms := &mockSink{}
	sink.Register("mock-recs", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return ms, nil
	})

	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:    "fake-recs",
				Transformer:  "fake-recs",
				Sink:         "mock-recs",
				ChunkRecords: 2, // Chunk every 2 records
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "recs")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry, 5)
	for i := 0; i < 5; i++ {
		entries <- &ct.RawLogEntry{Cert: ct.ASN1Cert{Data: []byte(strconv.Itoa(i))}}
	}
	close(entries)

	err = pipeline.StreamProcess(context.Background(), entries)
	require.NoError(t, err)
	require.Len(t, ms.Chunks, 3)
	require.Equal(t, "0\n1\n", string(ms.Chunks[0].Data))
	require.Equal(t, "2\n3\n", string(ms.Chunks[1].Data))
	require.Equal(t, "4\n", string(ms.Chunks[2].Data))
}

type errorExtractor struct{}

func (e *errorExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	return nil, fmt.Errorf("extract fail")
}

type errorTransformer struct{}

func (e *errorTransformer) Transform(ctx *etl_core.Context, data map[string]interface{}) ([]byte, error) {
	return nil, fmt.Errorf("transform fail")
}

func (e *errorTransformer) Header(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

func (e *errorTransformer) Footer(ctx *etl_core.Context) ([]byte, error) {
	return []byte{}, nil
}

type errorWriter struct{}

func (e *errorWriter) Write(p []byte) (int, error) { return 0, fmt.Errorf("write fail") }
func (e *errorWriter) Close() error                { return nil }

type errorSink struct{}

func (e *errorSink) Open(ctx context.Context, name string) (sink.SinkWriter, error) {
	return &errorWriter{}, nil
}

func TestPipeline_ExtractorError(t *testing.T) {
	extractor.Register("err-ext", &errorExtractor{})
	transformer.Register("fake", &fakeTransformer{})
	sink.Register("mock-err-ext", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return &mockSink{}, nil
	})
	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:   "err-ext",
				Transformer: "fake",
				Sink:        "mock-err-ext",
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "fail")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry, 1)
	entries <- &ct.RawLogEntry{Cert: ct.ASN1Cert{Data: []byte("fail")}}
	close(entries)
	err = pipeline.StreamProcess(context.Background(), entries)
	require.Error(t, err)
	require.Contains(t, err.Error(), "extract fail")
}

func TestPipeline_TransformerError(t *testing.T) {
	extractor.Register("fake", &fakeExtractor{})
	transformer.Register("err-xform", &errorTransformer{})
	sink.Register("mock-err-xform", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return &mockSink{}, nil
	})
	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:   "fake",
				Transformer: "err-xform",
				Sink:        "mock-err-xform",
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "fail")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry, 1)
	entries <- &ct.RawLogEntry{Cert: ct.ASN1Cert{Data: []byte("fail")}}
	close(entries)
	err = pipeline.StreamProcess(context.Background(), entries)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transform fail")
}

func TestPipeline_SinkWriterError(t *testing.T) {
	extractor.Register("fake", &fakeExtractor{})
	transformer.Register("fake", &fakeTransformer{})
	sink.Register("mock-err-writer", func(opts map[string]interface{}, secrets *secrets.Store) (sink.Sink, error) {
		return &errorSink{}, nil
	})
	spec := &job.JobSpec{
		Options: job.JobOptions{
			Output: job.OutputOptions{
				Extractor:   "fake",
				Transformer: "fake",
				Sink:        "mock-err-writer",
			},
		},
	}
	pipeline, err := NewPipeline(spec, &secrets.Store{}, "fail")
	require.NoError(t, err)

	entries := make(chan *ct.RawLogEntry, 1)
	entries <- &ct.RawLogEntry{Cert: ct.ASN1Cert{Data: []byte("fail")}}
	close(entries)
	err = pipeline.StreamProcess(context.Background(), entries)
	require.Error(t, err)
	require.Contains(t, err.Error(), "write fail")
}
