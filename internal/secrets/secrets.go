// Package secrets implements a distributed, cryptographically secure secrets store
// using etcd and NaCl secretbox, with node authentication and key approval flows.
package secrets

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Store represents a connection to the distributed secrets store.
// It handles node keypair management, secure cluster key retrieval,
// and encrypted secret storage and retrieval via etcd.
type Store struct {
	etcd     *clientv3.Client
	keys     nodeKeys
	NodeID   string
	prefix   string
	keyPath  string
	clusterK [32]byte
}

func (s *Store) SetClusterKey(key [32]byte) {
	s.clusterK = key
}

func (s *Store) NodeId() string {
	return s.NodeID
}

func (s *Store) Prefix() string {
	return s.prefix
}

func (s *Store) PublicKey() [32]byte {
	return s.keys.Public
}

func (s *Store) PrivateKey() [32]byte {
	return s.keys.Private
}

func (s *Store) HasClusterKey() bool {
	var zero [32]byte
	return s.clusterK != zero
}

func (s *Store) Client() *clientv3.Client {
	return s.etcd
}

// NewStore initializes a Store using the provided etcd client and key path.
// If no keypair exists at keyPath, a new one is generated and persisted.
// Returns an error if keypair creation or loading fails.
func NewStore(etcd *clientv3.Client, keyPath, prefix string) (*Store, error) {
	keys, nodeID, err := LoadOrGenerateNodeKeypair(keyPath)
	if err != nil {
		return nil, err
	}
	return &Store{
		etcd:    etcd,
		keys:    keys,
		NodeID:  nodeID,
		prefix:  prefix,
		keyPath: keyPath,
	}, nil
}
