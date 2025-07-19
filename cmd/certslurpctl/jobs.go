package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/chtzvt/certslurp/internal/job"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// Simple prompt helpers for interactive mode
func promptForJobSpec(spec *job.JobSpec) error {
	r := bufio.NewReader(os.Stdin)
	fmt.Printf("Version [1.0.0]: ")
	spec.Version = promptString(r, "1.0.0")
	fmt.Printf("Job Note: ")
	spec.Note = promptString(r, "")
	fmt.Printf("CT Log URI: ")
	spec.LogURI = promptString(r, "")

	// FetchConfig
	fmt.Printf("Fetch: Index start [0]: ")
	spec.Options.Fetch.IndexStart = promptInt64(r, 0)
	fmt.Printf("Fetch: Index end [0=auto]: ")
	spec.Options.Fetch.IndexEnd = promptInt64(r, 0)
	fmt.Printf("Fetch: Shard size [0=auto]: ")
	spec.Options.Fetch.ShardSize = promptInt(r, 0)
	fmt.Printf("Fetch: Fetch size [10]: ")
	spec.Options.Fetch.FetchSize = promptInt(r, 10)
	fmt.Printf("Fetch: Fetch workers [1]: ")
	spec.Options.Fetch.FetchWorkers = promptInt(r, 1)

	// MatchConfig
	fmt.Printf("Match: Subject regex: ")
	spec.Options.Match.SubjectRegex = promptString(r, "")
	fmt.Printf("Match: Issuer regex: ")
	spec.Options.Match.IssuerRegex = promptString(r, "")
	fmt.Printf("Match: Serial: ")
	spec.Options.Match.Serial = promptString(r, "")
	fmt.Printf("Match: SCT timestamp [0]: ")
	spec.Options.Match.SCTTimestamp = uint64(promptInt64(r, 0))
	fmt.Printf("Match: Domain Include: ")
	spec.Options.Match.DomainInclude = promptString(r, "")
	fmt.Printf("Match: Domain Exclude: ")
	spec.Options.Match.DomainExclude = promptString(r, "")
	fmt.Printf("Match: Parse errors [all/nonfatal]: ")
	spec.Options.Match.ParseErrors = promptString(r, "")
	fmt.Printf("Match: Validation errors (y/N): ")
	spec.Options.Match.ValidationErrors = promptBool(r, false)
	fmt.Printf("Match: Skip precerts (y/N): ")
	spec.Options.Match.SkipPrecerts = promptBool(r, false)
	fmt.Printf("Match: Precerts only (y/N): ")
	spec.Options.Match.PrecertsOnly = promptBool(r, false)
	fmt.Printf("Match: Workers [0]: ")
	spec.Options.Match.Workers = promptInt(r, 0)

	// OutputOptions
	fmt.Printf("Output: Chunk records [0]: ")
	spec.Options.Output.ChunkRecords = promptInt(r, 0)
	fmt.Printf("Output: Chunk bytes [0]: ")
	spec.Options.Output.ChunkBytes = promptInt(r, 0)
	fmt.Printf("Output: Extractor [raw]: ")
	spec.Options.Output.Extractor = promptString(r, "raw")
	fmt.Printf("Output: Transformer [passthrough]: ")
	spec.Options.Output.Transformer = promptString(r, "passthrough")
	fmt.Printf("Output: Sink [null]: ")
	spec.Options.Output.Sink = promptString(r, "null")
	// Not prompting for complex map options for brevity.

	return nil
}

func jobTemplateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "template",
		Short: "Print an example job spec YAML",
		Run: func(cmd *cobra.Command, args []string) {
			tmpl := job.JobSpec{
				Version: "1.0.0",
				Note:    "Example CT log job",
				LogURI:  "https://ct.googleapis.com/logs/argon2024/",
				Options: job.JobOptions{
					Fetch: job.FetchConfig{
						FetchSize:    10,
						FetchWorkers: 1,
						ShardSize:    100,
						IndexStart:   0,
						IndexEnd:     0,
					},
					Match: job.MatchConfig{
						SubjectRegex:     "",
						IssuerRegex:      "",
						DomainInclude:    "",
						DomainExclude:    "",
						ValidationErrors: false,
						SkipPrecerts:     false,
						PrecertsOnly:     false,
					},
					Output: job.OutputOptions{
						Extractor:   "raw",
						Transformer: "passthrough",
						Sink:        "null",
					},
				},
			}
			enc := yaml.NewEncoder(os.Stdout)
			enc.SetIndent(2)
			_ = enc.Encode(&tmpl)
		},
	}
}

func jobSubmitCmd() *cobra.Command {
	var (
		dryRun      bool
		file        string
		interactive bool
		// JobSpec fields
		version string
		note    string
		logURI  string
		// FetchConfig
		indexStart   int64
		indexEnd     int64
		shardSize    int
		fetchSize    int
		fetchWorkers int
		// MatchConfig
		subjectRegex     string
		issuerRegex      string
		serial           string
		sctTimestamp     uint64
		domainInclude    string
		domainExclude    string
		parseErrors      string
		validationErrors bool
		skipPrecerts     bool
		precertsOnly     bool
		matchWorkers     int
		// OutputOptions
		chunkRecords          int
		chunkBytes            int
		extractor             string
		transformer           string
		sink                  string
		extractorOptionsStr   string
		transformerOptionsStr string
		sinkOptionsStr        string
	)

	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a new job (via YAML or CLI flags)",
		Long: `You can submit a job by:
  - providing a YAML/JSON spec with --file,
  - using flags,
  - or interactively (--interactive).
To generate a template: certslurpctl job template`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			var spec job.JobSpec

			switch {
			case file != "":
				// YAML/JSON file
				f, err := os.Open(file)
				if err != nil {
					return err
				}
				defer f.Close()
				dec := yaml.NewDecoder(f)
				if err := dec.Decode(&spec); err != nil {
					// Try JSON fallback
					f.Seek(0, io.SeekStart)
					jdec := json.NewDecoder(f)
					if jerr := jdec.Decode(&spec); jerr != nil {
						return fmt.Errorf("decode spec %s: YAML error: %v; JSON error: %v", file, err, jerr)
					}
				}
			case interactive:
				spec = job.JobSpec{}
				if err := promptForJobSpec(&spec); err != nil {
					return err
				}
			default:
				// CLI flags
				spec.Version = version
				spec.Note = note
				spec.LogURI = logURI
				spec.Options.Fetch.IndexStart = indexStart
				spec.Options.Fetch.IndexEnd = indexEnd
				spec.Options.Fetch.ShardSize = shardSize
				spec.Options.Fetch.FetchSize = fetchSize
				spec.Options.Fetch.FetchWorkers = fetchWorkers

				spec.Options.Match.SubjectRegex = subjectRegex
				spec.Options.Match.IssuerRegex = issuerRegex
				spec.Options.Match.Serial = serial
				spec.Options.Match.SCTTimestamp = sctTimestamp
				spec.Options.Match.DomainInclude = domainInclude
				spec.Options.Match.DomainExclude = domainInclude
				spec.Options.Match.ParseErrors = parseErrors
				spec.Options.Match.ValidationErrors = validationErrors
				spec.Options.Match.SkipPrecerts = skipPrecerts
				spec.Options.Match.PrecertsOnly = precertsOnly
				spec.Options.Match.Workers = matchWorkers

				spec.Options.Output.ChunkRecords = chunkRecords
				spec.Options.Output.ChunkBytes = chunkBytes
				spec.Options.Output.Extractor = extractor
				spec.Options.Output.Transformer = transformer
				spec.Options.Output.Sink = sink

				extractorOpts, err := parseOptions(extractorOptionsStr)
				if err != nil {
					return fmt.Errorf("extractor-options invalid JSON (%q): %w", extractorOptionsStr, err)
				}

				transformerOpts, err := parseOptions(transformerOptionsStr)
				if err != nil {
					return fmt.Errorf("transformer-options invalid JSON (%q): %w", transformerOptionsStr, err)
				}

				sinkOpts, err := parseOptions(sinkOptionsStr)
				if err != nil {
					return fmt.Errorf("sink-options invalid JSON (%q): %w", sinkOptionsStr, err)
				}

				spec.Options.Output.ExtractorOptions = extractorOpts
				spec.Options.Output.TransformerOptions = transformerOpts
				spec.Options.Output.SinkOptions = sinkOpts
			}

			if err := spec.Validate(); err != nil {
				return fmt.Errorf("job spec validation failed: %w", err)
			}

			if dryRun {
				fmt.Println("# JobSpec (YAML preview, not submitted):")
				enc := yaml.NewEncoder(os.Stdout)
				enc.SetIndent(2)
				if err := enc.Encode(&spec); err != nil {
					return fmt.Errorf("error encoding YAML: %w", err)
				}
				return nil
			}

			jobID, err := client.SubmitJob(ctx, &spec)
			if err != nil {
				return err
			}

			fmt.Printf("Job submitted: %s\n", jobID)
			return nil
		},
	}

	// Dry run
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Validate and print job spec without submitting")

	// YAML/JSON input file
	cmd.Flags().StringVar(&file, "file", "", "Job spec YAML/JSON file")

	// Interactive
	cmd.Flags().BoolVar(&interactive, "interactive", false, "Prompt for job fields interactively")

	// JobSpec flags
	cmd.Flags().StringVar(&version, "version", "1.0.0", "Job spec version")
	cmd.Flags().StringVar(&note, "note", "", "Job note")
	cmd.Flags().StringVar(&logURI, "log-uri", "", "CT log URI")

	// FetchConfig
	cmd.Flags().Int64Var(&indexStart, "start", 0, "Index start")
	cmd.Flags().Int64Var(&indexEnd, "end", 0, "Index end (0=auto)")
	cmd.Flags().IntVar(&shardSize, "shard-size", 0, "Shard size (0=auto)")
	cmd.Flags().IntVar(&fetchSize, "fetch-size", 10, "Batch fetch size")
	cmd.Flags().IntVar(&fetchWorkers, "fetch-workers", 1, "Fetch workers per shard")

	// MatchConfig
	cmd.Flags().StringVar(&subjectRegex, "subject-regex", "", "Subject regex")
	cmd.Flags().StringVar(&issuerRegex, "issuer-regex", "", "Issuer regex")
	cmd.Flags().StringVar(&serial, "serial", "", "Serial number filter")
	cmd.Flags().Uint64Var(&sctTimestamp, "sct-timestamp", 0, "SCT timestamp")
	cmd.Flags().StringVar(&domainInclude, "domain-include", "", "Positive match DNS name")
	cmd.Flags().StringVar(&domainExclude, "domain-exclude", "", "Negative match DNS name")
	cmd.Flags().StringVar(&parseErrors, "parse-errors", "", "Parse errors (all/nonfatal)")
	cmd.Flags().BoolVar(&validationErrors, "validation-errors", false, "Match only certs with validation errors")
	cmd.Flags().BoolVar(&skipPrecerts, "skip-precerts", false, "Skip precerts")
	cmd.Flags().BoolVar(&precertsOnly, "precerts-only", false, "Match only precerts")
	cmd.Flags().IntVar(&matchWorkers, "match-workers", 0, "Workers for matching phase")

	// OutputOptions
	cmd.Flags().IntVar(&chunkRecords, "chunk-records", 0, "Chunk size (records)")
	cmd.Flags().IntVar(&chunkBytes, "chunk-bytes", 0, "Chunk size (bytes)")
	cmd.Flags().StringVar(&extractor, "extractor", "raw", "Extractor")
	cmd.Flags().StringVar(&transformer, "transformer", "passthrough", "Transformer")
	cmd.Flags().StringVar(&sink, "sink", "null", "Sink")
	cmd.Flags().StringVar(&extractorOptionsStr, "extractor-options", "", "Extractor options as JSON (e.g., '{\"foo\": \"bar\"}')")
	cmd.Flags().StringVar(&transformerOptionsStr, "transformer-options", "", "Transformer options as JSON")
	cmd.Flags().StringVar(&sinkOptionsStr, "sink-options", "", "Sink options as JSON")

	return cmd
}

func jobListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all jobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			jobs, err := client.ListJobs(ctx)
			if err != nil {
				return err
			}
			outResult(jobs, printJobsTable)
			return nil
		},
	}
}

func jobStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <jobID>",
		Short: "Show job status",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			info, err := client.GetJob(ctx, args[0])
			if err != nil {
				return err
			}
			outResult(info, printJobStatusTable)
			return nil
		},
	}
}

func jobCancelCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <jobID>",
		Short: "Cancel a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			return client.CancelJob(ctx, args[0])
		},
	}
}

func jobStartCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "start <jobID>",
		Short: "Mark a job as started",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			return client.MarkJobStarted(ctx, args[0])
		},
	}
}

func jobCompleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "complete <jobID>",
		Short: "Mark a job as completed",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			client := cliClient()
			return client.MarkJobCompleted(ctx, args[0])
		},
	}
}

func jobShardsCmd() *cobra.Command {
	var start, end int
	cmd := &cobra.Command{
		Use:   "shards <jobID>",
		Short: "List shard assignments for a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := cliClient()
			ctx := context.Background()
			var sPtr, ePtr *int
			if cmd.Flags().Changed("start") {
				sPtr = &start
			}
			if cmd.Flags().Changed("end") {
				ePtr = &end
			}
			shards, err := client.GetShardAssignments(ctx, args[0], sPtr, ePtr)
			if err != nil {
				return err
			}
			outResult(shards, printShardsTable)
			return nil
		},
	}
	cmd.Flags().IntVar(&start, "start", 0, "Start shard index (inclusive)")
	cmd.Flags().IntVar(&end, "end", 0, "End shard index (inclusive)")
	return cmd
}

func jobResetFailedCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset <jobID>",
		Short: "Reset all failed shards for a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := cliClient()
			ctx := context.Background()
			jobID := args[0]
			shards, err := client.ResetFailedShards(ctx, jobID)
			if err != nil {
				return err
			}
			fmt.Printf("Reset %d failed shards for job %s: %v\n", len(shards), jobID, shards)
			return nil
		},
	}
}

func shardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shard",
		Short: "Shard operations",
	}

	cmd.AddCommand(
		shardStatusCmd(),
		shardResetCmd(),
	)
	return cmd
}

func shardStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <jobID> <shardID>",
		Short: "Get status for a single shard",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := cliClient()
			ctx := context.Background()
			jobID := args[0]
			var shardID int
			_, err := fmt.Sscanf(args[1], "%d", &shardID)
			if err != nil {
				return fmt.Errorf("invalid shardID: %w", err)
			}
			status, err := client.GetShardStatus(ctx, jobID, shardID)
			if err != nil {
				return err
			}
			outResult(status, printShardStatusTable)
			return nil
		},
	}
}

func shardResetCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reset <jobID> <shardID>",
		Short: "Reset a specific failed shard",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			client := cliClient()
			ctx := context.Background()
			jobID := args[0]
			var shardID int
			_, err := fmt.Sscanf(args[1], "%d", &shardID)
			if err != nil {
				return fmt.Errorf("invalid shardID: %w", err)
			}
			if err := client.ResetFailedShard(ctx, jobID, shardID); err != nil {
				return err
			}
			fmt.Printf("Reset failed shard %d for job %s\n", shardID, jobID)
			return nil
		},
	}
}
