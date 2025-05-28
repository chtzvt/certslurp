package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/crypto/nacl/box"
)

// GenerateClusterKey creates a new random cluster key
func GenerateClusterKey() ([32]byte, error) {
	var clusterKey [32]byte
	var err error

	if _, err := rand.Read(clusterKey[:]); err != nil {
		return clusterKey, err
	}

	return clusterKey, err
}

// ApproveNode is used by an administrator to approve a pending node registration.
// Encrypts the cluster key with the node's public key and stores it in etcd.
// Removes the pending registration after approval.
func (n *Store) ApproveNode(ctx context.Context, nodeID string, clusterKey [32]byte) error {
	resp, err := n.etcd.Get(ctx, n.Prefix()+"/registration/pending/"+nodeID)
	if err != nil || len(resp.Kvs) == 0 {
		return errors.New("pending registration not found")
	}
	pubKeyB64 := string(resp.Kvs[0].Value)
	pubBytes, _ := base64.StdEncoding.DecodeString(pubKeyB64)
	if len(pubBytes) != 32 {
		return errors.New("invalid pubkey")
	}
	var pubKey [32]byte
	copy(pubKey[:], pubBytes)
	sealed, err := box.SealAnonymous(nil, clusterKey[:], &pubKey, rand.Reader)
	if err != nil {
		return err
	}
	sealedB64 := base64.StdEncoding.EncodeToString(sealed)
	_, err = n.etcd.Put(ctx, n.Prefix()+"/secrets/keys/"+nodeID, sealedB64)
	_, _ = n.etcd.Delete(ctx, n.Prefix()+"/registration/pending/"+nodeID)
	return err
}

// PendingRegistration represents a node that has requested cluster access.
type PendingRegistration struct {
	NodeID    string
	PubKeyB64 string // raw base64 public key
}

// ListPendingRegistrations lists all nodeIDs currently pending approval.
func (s *Store) ListPendingRegistrations(ctx context.Context) ([]PendingRegistration, error) {
	prefix := s.Prefix() + "/registration/pending/"
	resp, err := s.etcd.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	pending := make([]PendingRegistration, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		nodeID := key[len(prefix):]
		pending = append(pending, PendingRegistration{
			NodeID:    nodeID,
			PubKeyB64: string(kv.Value),
		})
	}
	return pending, nil
}
