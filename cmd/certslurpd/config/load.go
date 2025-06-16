package config

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/spf13/viper"

	"github.com/moby/moby/pkg/namesgenerator"
)

func LoadConfig(cfgFile string) (*ClusterConfig, error) {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("certslurpd")
		viper.AddConfigPath(".")
		viper.AddConfigPath("/etc/certslurpd/")
	}

	viper.SetEnvPrefix("CERTSLURPD") // env vars like CERTSLURPD_NODE__ID
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "__"))

	viper.SetDefault("node.id", "")
	viper.SetDefault("worker.parallelism", 4)
	viper.SetDefault("worker.batch_size", 8)
	viper.SetDefault("worker.poll_period", 5*time.Second)
	viper.SetDefault("etcd.prefix", "/certslurp")
	viper.SetDefault("api.listen_addr", ":8989")
	viper.SetDefault("secrets.keychain_file", "")

	viper.BindEnv("node.id")
	viper.BindEnv("worker.parallelism")
	viper.BindEnv("worker.batch_size")
	viper.BindEnv("worker.poll_period")
	viper.BindEnv("etcd.endpoints")
	viper.BindEnv("etcd.username")
	viper.BindEnv("etcd.password")
	viper.BindEnv("etcd.prefix")
	viper.BindEnv("secrets.keychain_file")
	viper.BindEnv("secrets.cluster_key")
	viper.BindEnv("api.listen_addr")
	viper.BindEnv("api.auth_tokens")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	var cfg ClusterConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	discriminator, err := rand.Int(rand.Reader, big.NewInt(1000))
	if err != nil {
		return nil, fmt.Errorf("genrate name: %w", err)
	}

	if cfg.Node.ID == "" {
		cfg.Node.ID = fmt.Sprintf("%s%03d", namesgenerator.GetRandomName(0), discriminator)
	}

	return &cfg, nil
}
