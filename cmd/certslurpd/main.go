package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "certslurpd",
	Short: "certslurpd is a distributed CT log fetcher and processor (head/worker)",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $PWD/certslurpd.yaml)")
	rootCmd.AddCommand(headCmd)
	rootCmd.AddCommand(workerCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
