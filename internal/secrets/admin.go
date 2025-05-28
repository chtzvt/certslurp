package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/crypto/nacl/box"
)

// GenerateAndStoreClusterKey creates a new random cluster key and stores it in etcd
// under the cluster key path. Only needed for initial cluster bootstrapping.
func GenerateAndStoreClusterKey(ctx context.Context, etcd *clientv3.Client, prefix string) ([32]byte, error) {
	var clusterKey [32]byte
	if _, err := rand.Read(clusterKey[:]); err != nil {
		return clusterKey, err
	}
	b64 := base64.StdEncoding.EncodeToString(clusterKey[:])
	_, err := etcd.Put(ctx, prefix+"/secrets/cluster_key", b64)
	return clusterKey, err
}

// ApproveNode is used by an administrator to approve a pending node registration.
// Encrypts the cluster key with the node's public key and stores it in etcd.
// Removes the pending registration after approval.
func ApproveNode(ctx context.Context, etcd *clientv3.Client, nodeID string, prefix string, clusterKey [32]byte) error {
	resp, err := etcd.Get(ctx, prefix+"/registration/pending/"+nodeID)
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
	_, err = etcd.Put(ctx, prefix+"/secrets/keys/"+nodeID, sealedB64)
	_, _ = etcd.Delete(ctx, prefix+"/registration/pending/"+nodeID)
	return err
}
