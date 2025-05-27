package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	verbose bool
)

var rootCmd = &cobra.Command{
	Use:   "certslurpctl",
	Short: "Control utility for managing certslurp clusters",
	Long:  `certslurpctl is the CLI for orchestrating and inspecting your certslurp distributed scraping cluster.`,
	// Add Run here if you want a default action
}

func init() {
	cobra.OnInitialize(initConfig)

	// Persistent flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $PWD/certslurpctl.yaml)")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Search for default config file in PWD
		viper.AddConfigPath(".")
		viper.SetConfigName("certslurpctl")
	}
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil && verbose {
		fmt.Printf("Using config file: %s\n", viper.ConfigFileUsed())
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
w