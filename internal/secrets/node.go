package secrets

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/nacl/box"
)

type nodeKeys struct {
	Public  [32]byte
	Private [32]byte
}

type nodeKeyDisk struct {
	Pub  string `json:"pub"`
	Priv string `json:"priv"`
}

// LoadOrGenerateNodeKeypair loads a NaCl keypair from disk at the specified path,
// or generates and persists a new one if not present. Returns the keypair and
// the nodeID (SHA256 hex of the public key).
func LoadOrGenerateNodeKeypair(path string) (nodeKeys, string, error) {
	// Try to load from disk
	data, err := os.ReadFile(path)
	if err == nil {
		var disk nodeKeyDisk
		if err := json.Unmarshal(data, &disk); err != nil {
			return nodeKeys{}, "", err
		}
		pub, _ := base64.StdEncoding.DecodeString(disk.Pub)
		priv, _ := base64.StdEncoding.DecodeString(disk.Priv)
		if len(pub) != 32 || len(priv) != 32 {
			return nodeKeys{}, "", errors.New("invalid key lengths")
		}
		var keys nodeKeys
		copy(keys.Public[:], pub)
		copy(keys.Private[:], priv)
		hash := sha256.Sum256(keys.Public[:])
		nodeID := fmt.Sprintf("%x", hash[:])
		return keys, nodeID, nil
	}

	// Generate new keys
	pub, priv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nodeKeys{}, "", err
	}
	keys := nodeKeys{*pub, *priv}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nodeKeys{}, "", err
	}
	disk := nodeKeyDisk{
		Pub:  base64.StdEncoding.EncodeToString(keys.Public[:]),
		Priv: base64.StdEncoding.EncodeToString(keys.Private[:]),
	}
	b, _ := json.Marshal(disk)
	if err := os.WriteFile(path, b, 0600); err != nil {
		return nodeKeys{}, "", err
	}
	hash := sha256.Sum256(keys.Public[:])
	nodeID := fmt.Sprintf("%x", hash[:])
	return keys, nodeID, nil
}

// RegisterAndWaitForClusterKey registers this node for approval in etcd,
// then waits for the admin to approve and provide the sealed cluster key.
// Blocks until the cluster key is received and decrypted or the context is canceled.
// On success, the Store can be used for secret operations.
func (n *Store) RegisterAndWaitForClusterKey(ctx context.Context) error {
	pubB64 := base64.StdEncoding.EncodeToString(n.keys.Public[:])
	_, err := n.etcd.Put(ctx, n.Prefix()+"/registration/pending/"+n.NodeId(), pubB64)
	if err != nil {
		return err
	}
	for {
		resp, err := n.etcd.Get(ctx, n.Prefix()+"/secrets/keys/"+n.NodeId())
		if err != nil {
			return err
		}
		if len(resp.Kvs) > 0 {
			sealed, _ := base64.StdEncoding.DecodeString(string(resp.Kvs[0].Value))
			cKey, ok := box.OpenAnonymous(nil, sealed, &n.keys.Public, &n.keys.Private)
			if ok && len(cKey) == 32 {
				copy(n.clusterK[:], cKey)
				// Optional: clean up
				_, _ = n.etcd.Delete(ctx, n.Prefix()+"/registration/pending/"+n.NodeId())
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
