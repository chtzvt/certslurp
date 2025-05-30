package config

import (
	"fmt"
	"strings"

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

	viper.BindEnv("node.id")
	viper.BindEnv("etcd.endpoints")
	viper.BindEnv("etcd.username")
	viper.BindEnv("etcd.password")
	viper.BindEnv("etcd.prefix")
	viper.BindEnv("secrets.keychain_file")
	viper.BindEnv("secrets.cluster_key")
	viper.BindEnv("api.listen_addr")
	viper.BindEnv("api.auth_tokens")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg ClusterConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if cfg.Node.ID == "" {
		cfg.Node.ID = namesgenerator.GetRandomName(1)
	}

	return &cfg, nil
}
