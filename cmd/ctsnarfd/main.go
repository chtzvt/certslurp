package main

import (
	"fmt"
	"os"

	"github.com/chtzvt/ctsnarf/internal/job"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	jobFile string
	verbose bool
	cfgFile string
	rootCmd = &cobra.Command{
		Use:   "ctsnarfd",
		Short: "ctsnarfd is a distributed CT log fetcher",
		RunE:  runDaemon,
	}
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $PWD/ctsnarfd.yaml)")
	rootCmd.Flags().StringVar(&jobFile, "job", "", "Path to job specification (required)")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose logging")

	rootCmd.MarkFlagRequired("job")

	// Bind viper so config/env can override
	viper.BindPFlag("job", rootCmd.Flags().Lookup("job"))
	viper.BindPFlag("verbose", rootCmd.Flags().Lookup("verbose"))
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search default locations
		viper.AddConfigPath(".")
		viper.SetConfigName("ctsnarfd")
	}
	viper.AutomaticEnv()
	_ = viper.ReadInConfig() // Ignore missing config
}

func runDaemon(cmd *cobra.Command, args []string) error {
	jobPath := viper.GetString("job")
	spec, err := job.LoadFromFile(jobPath)
	if err != nil {
		return fmt.Errorf("error loading job spec: %w", err)
	}

	fmt.Printf("Loaded job spec:\n  Version: %s\n  Note: %s\n  Log URI: %s\n", spec.Version, spec.Note, spec.LogURI)
	// (add your transformer/target logic here as before...)

	return nil
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
