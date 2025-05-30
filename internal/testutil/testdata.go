package testutil

import (
	"encoding/pem"
	"testing"
	"time"

	ct "github.com/google/certificate-transparency-go"
	testdata "github.com/google/certificate-transparency-go/testdata"
	x509 "github.com/google/certificate-transparency-go/x509"
	x509util "github.com/google/certificate-transparency-go/x509util"
	"github.com/stretchr/testify/require"
)

func RawLogEntryForTestPrecert(t *testing.T, idx int64) *ct.RawLogEntry {
	// 1. Parse PEM to DER
	block, _ := pem.Decode([]byte(testdata.TestPreCertPEM))
	require.NotNil(t, block, "failed to decode PEM block")
	der := block.Bytes

	// 2. Parse both leaf (precert) and CA issuer certs for chain
	chain, err := x509util.CertificatesFromPEM([]byte(testdata.TestPreCertPEM + testdata.CACertPEM))
	require.NoError(t, err, "failed to parse cert chain from PEM")
	require.GreaterOrEqual(t, len(chain), 2, "chain should have precert and CA")

	// 3. Build ASN1Certs for RawLogEntry.Cert and Chain
	var preCertASN1 ct.ASN1Cert
	preCertASN1.Data = der

	var caASN1 ct.ASN1Cert
	caASN1.Data = chain[1].Raw

	// 4. Build TBSCertificate
	x509Cert, err := x509.ParseCertificate(der)
	require.NoError(t, err, "failed to parse x509 cert")

	// 5. Build the MerkleTreeLeaf with PrecertLogEntryType
	leaf := ct.MerkleTreeLeaf{
		Version:  ct.V1,
		LeafType: ct.TimestampedEntryLeafType,
		TimestampedEntry: &ct.TimestampedEntry{
			Timestamp: uint64(time.Now().UnixNano() / 1e6),
			EntryType: ct.PrecertLogEntryType,
			PrecertEntry: &ct.PreCert{
				IssuerKeyHash:  [32]byte{}, // still fine to leave empty
				TBSCertificate: x509Cert.RawTBSCertificate,
			},
			Extensions: ct.CTExtensions{},
		},
	}

	return &ct.RawLogEntry{
		Index: idx,
		Leaf:  leaf,
		Cert:  preCertASN1,
		Chain: []ct.ASN1Cert{caASN1},
	}
}
