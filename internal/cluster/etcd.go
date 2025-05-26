package cluster

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdConfig struct {
	Endpoints   []string
	Username    string // optional
	Password    string // optional
	DialTimeout time.Duration
	Prefix      string // default: "/ctsnarf"
}

type etcdCluster struct {
	client *clientv3.Client
	cfg    EtcdConfig
}

func NewEtcdCluster(cfg EtcdConfig) (Cluster, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.Endpoints,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &etcdCluster{
		client: cli,
		cfg:    cfg,
	}, nil
}

func (c *etcdCluster) Prefix() string {
	return c.cfg.Prefix
}

func (c *etcdCluster) Client() *clientv3.Client {
	return c.client
}

func (c *etcdCluster) Close() error {
	return c.client.Close()
}
