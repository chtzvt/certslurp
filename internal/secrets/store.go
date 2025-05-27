package secrets

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/crypto/nacl/secretbox"
)

// List returns all secret keys in etcd with the given prefix ("" for all).
// The returned keys are relative (prefix removed).
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	keyPrefix := "/certslurp/secrets/store/"
	if prefix != "" {
		keyPrefix += prefix
	}
	resp, err := s.etcd.Get(ctx, keyPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		k := string(kv.Key)
		// Strip leading prefix, return relative key
		keys = append(keys, strings.TrimPrefix(k, "/certslurp/secrets/store/"))
	}
	return keys, nil
}

// Set encrypts the provided value with the cluster key and stores it in etcd
// under the given key. Overwrites any existing value. Returns an error on failure.
func (n *Store) Set(ctx context.Context, key string, value []byte) error {
	var nonce [24]byte
	_, _ = rand.Read(nonce[:])
	sealed := secretbox.Seal(nonce[:], value, &nonce, &n.clusterK)
	b64 := base64.StdEncoding.EncodeToString(sealed)
	_, err := n.etcd.Put(ctx, "/certslurp/secrets/store/"+key, b64)
	return err
}

// Get retrieves and decrypts the value associated with the given key.
// Returns the plaintext or an error if the key is not found or decryption fails.
func (n *Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := n.etcd.Get(ctx, "/certslurp/secrets/store/"+key)
	if err != nil || len(resp.Kvs) == 0 {
		return nil, errors.New("secret not found")
	}
	sealed, _ := base64.StdEncoding.DecodeString(string(resp.Kvs[0].Value))
	if len(sealed) < 24 {
		return nil, errors.New("invalid secret data")
	}
	var nonce [24]byte
	copy(nonce[:], sealed[:24])
	plain, ok := secretbox.Open(nil, sealed[24:], &nonce, &n.clusterK)
	if !ok {
		return nil, errors.New("decryption failed")
	}
	return plain, nil
}

// Delete removes the secret stored under the given key from etcd.
// Returns an error if the operation fails.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.etcd.Delete(ctx, "/certslurp/secrets/store/"+key)
	return err
}
