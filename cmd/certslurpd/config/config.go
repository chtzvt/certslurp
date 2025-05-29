package config

import "github.com/chtzvt/certslurp/internal/api"

type NodeConfig struct {
	ID string `mapstructure:"id"`
}

type EtcdConfig struct {
	Endpoints []string `mapstructure:"endpoints"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
	Prefix    string   `mapstructure:"prefix"`
}

type SecretsConfig struct {
	KeychainFile string `mapstructure:"keychain_file"`
	ClusterKey   string `mapstructure:"cluster_key"`
}

type ClusterConfig struct {
	Node    NodeConfig    `mapstructure:"node"`
	Api     api.Config    `mapstructure:"api"`
	Etcd    EtcdConfig    `mapstructure:"etcd"`
	Secrets SecretsConfig `mapstructure:"secrets"`
}
