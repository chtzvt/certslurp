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
	mode := os.Getenv("CERTSLURPD_MODE")
	if mode == "head" || mode == "worker" {
		if len(os.Args) == 1 || (os.Args[1] != "head" && os.Args[1] != "worker") {
			os.Args = append([]string{os.Args[0], mode}, os.Args[1:]...)
		}
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
