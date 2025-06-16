package config

import (
	"time"

	"github.com/chtzvt/certslurp/internal/api"
)

type NodeConfig struct {
	ID string `mapstructure:"id"`
}

type WorkerConfig struct {
	Parallelism int           `mapstructure:"parallelism"`
	BatchSize   int           `mapstructure:"batch_size"`
	PollPeriod  time.Duration `mapstructure:"poll_period"`
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
	Worker  WorkerConfig  `mapstructure:worker`
	Api     api.Config    `mapstructure:"api"`
	Etcd    EtcdConfig    `mapstructure:"etcd"`
	Secrets SecretsConfig `mapstructure:"secrets"`
}
