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
	nodeID   string
	keyPath  string
	clusterK [32]byte
}

// NewStore initializes a Store using the provided etcd client and key path.
// If no keypair exists at keyPath, a new one is generated and persisted.
// Returns an error if keypair creation or loading fails.
func NewStore(etcd *clientv3.Client, keyPath string) (*Store, error) {
	keys, nodeID, err := LoadOrGenerateNodeKeypair(keyPath)
	if err != nil {
		return nil, err
	}
	return &Store{
		etcd:    etcd,
		keys:    keys,
		nodeID:  nodeID,
		keyPath: keyPath,
	}, nil
}
