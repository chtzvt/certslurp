package extractor

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/chtzvt/ctsnarf/internal/etl_core"
	ct "github.com/google/certificate-transparency-go"
	x509 "github.com/google/certificate-transparency-go/x509"
)

/*
CertFieldsExtractor extracts a variety of useful metadata from CT log certificates, precertificates,
and entries into a map[string]interface{}.

It can be configured in a JobSpec via the following options:

{
	"extractor": "cert_fields",
	"extractor_options": {
		// Syntax:

		// Everything except specific fields:
		"cert_fields": "*,!serial,!postal_code"

		// If a key is omitted or empty, no fields will be included for it
		"precert_fields": ""

		// Specific field list:
		"log_fields": "log_index"
	}
}
*/

// CertFieldsExtractor's output can be unmarshaled into this type
type CertFieldsExtractorOutput struct {
	CommonName         string    `json:"cn,omitempty"`
	EmailAddresses     []string  `json:"em,omitempty"`
	OrganizationalUnit []string  `json:"ou,omitempty"`
	Organization       []string  `json:"org,omitempty"`
	Locality           []string  `json:"loc,omitempty"`
	Province           []string  `json:"prv,omitempty"`
	Country            []string  `json:"co,omitempty"`
	StreetAddress      []string  `json:"st,omitempty"`
	PostalCode         []string  `json:"pc,omitempty"`
	DNSNames           []string  `json:"dns,omitempty"`
	IPAddresses        []string  `json:"ip,omitempty"`
	URIs               []string  `json:"uris,omitempty"`
	Subject            string    `json:"sub,omitempty"`
	Issuer             string    `json:"iss"`
	SerialNumber       string    `json:"sn"`
	NotBefore          time.Time `json:"nbf"`
	NotAfter           time.Time `json:"naf"`
	EntryNumber        int64     `json:"li"`
	Timestamp          int64     `json:"lts"`
}

type CertFieldsExtractor struct {
	Options CertFieldsExtractorOptions
}

type CertFieldsExtractorOptions struct {
	CertFields    string `json:"cert_fields"`
	PrecertFields string `json:"precert_fields"`
	LogFields     string `json:"log_fields"`
}

const (
	CertFieldsExtractorDefaultCertFields    string = "*"
	CertFieldsExtractorDefaultPreCertFields string = "*"
	CertFieldsExtractorDefaultLogFields     string = "*"
)

type CertFieldsExtractorCertFunc func(cert *x509.Certificate) (string, interface{}, error)

var certFuncs = map[string]CertFieldsExtractorCertFunc{
	"common_name": func(cert *x509.Certificate) (string, interface{}, error) {
		return "cn", cert.Subject.CommonName, nil
	},
	"email_addresses": func(cert *x509.Certificate) (string, interface{}, error) {
		return "em", cert.EmailAddresses, nil
	},
	"organizational_unit": func(cert *x509.Certificate) (string, interface{}, error) {
		return "ou", cert.Subject.OrganizationalUnit, nil
	},
	"organization": func(cert *x509.Certificate) (string, interface{}, error) {
		return "org", cert.Subject.Organization, nil
	},
	"locality": func(cert *x509.Certificate) (string, interface{}, error) {
		return "loc", cert.Subject.Locality, nil
	},
	"province": func(cert *x509.Certificate) (string, interface{}, error) {
		return "prv", cert.Subject.Province, nil
	},
	"country": func(cert *x509.Certificate) (string, interface{}, error) {
		return "co", cert.Subject.Country, nil
	},
	"street_address": func(cert *x509.Certificate) (string, interface{}, error) {
		return "st", cert.Subject.StreetAddress, nil
	},
	"postal_code": func(cert *x509.Certificate) (string, interface{}, error) {
		return "pc", cert.Subject.PostalCode, nil
	},
	"dns_names": func(cert *x509.Certificate) (string, interface{}, error) {
		if len(cert.DNSNames) == 0 {
			return "dns", []string{}, fmt.Errorf("no DNS names present")
		}
		return "dns", cert.DNSNames, nil
	},
	"ip_addresses": func(cert *x509.Certificate) (string, interface{}, error) {
		if len(cert.IPAddresses) == 0 {
			return "ip", []string{}, fmt.Errorf("no IP addresses names present")
		}
		return "ip", cert.IPAddresses, nil
	},
	"uris": func(cert *x509.Certificate) (string, interface{}, error) {
		if len(cert.URIs) == 0 {
			return "uris", []string{}, fmt.Errorf("no URIs present")
		}
		return "uris", cert.URIs, nil
	},
	"subject": func(cert *x509.Certificate) (string, interface{}, error) {
		return "sub", cert.Subject.String(), nil
	},
	"issuer": func(cert *x509.Certificate) (string, interface{}, error) {
		return "iss", cert.Issuer.CommonName, nil
	},
	"serial": func(cert *x509.Certificate) (string, interface{}, error) {
		return "sn", cert.SerialNumber.String(), nil
	},
	"not_before": func(cert *x509.Certificate) (string, interface{}, error) {
		return "nbf", cert.NotBefore, nil
	},
	"not_after": func(cert *x509.Certificate) (string, interface{}, error) {
		return "naf", cert.NotAfter, nil
	},
}

type CertFieldsExtractorPrecertFunc func(cert *ct.Precertificate) (string, interface{}, error)

var precertFuncs = map[string]CertFieldsExtractorPrecertFunc{
	"subject": func(cert *ct.Precertificate) (string, interface{}, error) {
		return "presub", cert.TBSCertificate.Subject.String(), nil
	},
	"issuer": func(cert *ct.Precertificate) (string, interface{}, error) {
		return "preiss", cert.TBSCertificate.Issuer.String(), nil
	},
}

type CertFieldsExtractorLogEntryFunc func(le *ct.RawLogEntry) (string, interface{}, error)

var logEntryFuncs = map[string]CertFieldsExtractorLogEntryFunc{
	"log_index": func(le *ct.RawLogEntry) (string, interface{}, error) {
		return "li", le.Index, nil
	},
	"log_timestamp": func(le *ct.RawLogEntry) (string, interface{}, error) {
		return "lts", le.Leaf.TimestampedEntry.Timestamp, nil
	},
}

func (e *CertFieldsExtractor) Extract(ctx *etl_core.Context, raw *ct.RawLogEntry) (map[string]interface{}, error) {
	if raw == nil {
		return nil, errors.New("got a nil log entry")
	}
	if reflect.DeepEqual(raw.Leaf, ct.MerkleTreeLeaf{}) {
		return nil, errors.New("got log entry with unset Leaf")
	}

	if e.Options.CertFields == "" && e.Options.PrecertFields == "" && e.Options.LogFields == "" {
		e.Options = parseOptions(ctx.Spec.Options.Output.ExtractorOptions)
	}

	// Collect all known field keys for each type
	var certKeys, precertKeys, logKeys []string
	for k := range certFuncs {
		certKeys = append(certKeys, k)
	}
	for k := range precertFuncs {
		precertKeys = append(precertKeys, k)
	}
	for k := range logEntryFuncs {
		logKeys = append(logKeys, k)
	}

	certFields := parseFieldSpec(certKeys, e.Options.CertFields)
	precertFields := parseFieldSpec(precertKeys, e.Options.PrecertFields)
	logFields := parseFieldSpec(logKeys, e.Options.LogFields)

	result := map[string]interface{}{}
	parsed, err := raw.ToLogEntry()
	if err != nil {
		return nil, err
	}

	if parsed.X509Cert != nil {
		for key, use := range certFields {
			if !use {
				continue
			}
			if fn, ok := certFuncs[key]; ok {
				outKey, val, err := fn(parsed.X509Cert)
				if err == nil && outKey != "" && val != nil {
					result[outKey] = val
				}
			}
		}
	} else if parsed.Precert != nil {
		for key, use := range precertFields {
			if !use {
				continue
			}
			if fn, ok := precertFuncs[key]; ok {
				outKey, val, err := fn(parsed.Precert)
				if err == nil && outKey != "" && val != nil {
					result[outKey] = val
				}
			}
		}
	}

	for key, use := range logFields {
		if !use {
			continue
		}
		if fn, ok := logEntryFuncs[key]; ok {
			outKey, val, err := fn(raw)
			if err == nil && outKey != "" && val != nil {
				result[outKey] = val
			}
		}
	}

	return result, nil
}

func parseOptions(opts map[string]interface{}) CertFieldsExtractorOptions {
	var o CertFieldsExtractorOptions
	if opts == nil {
		o.CertFields = CertFieldsExtractorDefaultCertFields
		o.PrecertFields = CertFieldsExtractorDefaultPreCertFields
		o.LogFields = CertFieldsExtractorDefaultLogFields
		return o
	}

	for k, v := range opts {
		switch k {
		case "cert_fields":
			o.CertFields, _ = v.(string)
		case "precert_fields":
			o.PrecertFields, _ = v.(string)
		case "log_fields":
			o.LogFields, _ = v.(string)
		}
	}

	if len(o.CertFields) == 0 {
		o.CertFields = CertFieldsExtractorDefaultCertFields
	}

	if len(o.PrecertFields) == 0 {
		o.PrecertFields = CertFieldsExtractorDefaultPreCertFields
	}

	if len(o.LogFields) == 0 {
		o.LogFields = CertFieldsExtractorDefaultLogFields
	}

	return o
}

// parseFieldSpec parses a spec string like "*,!foo,bar" into a map of field keys to include (true) or exclude (false).
func parseFieldSpec(allFields []string, spec string) map[string]bool {
	fields := map[string]bool{}
	if spec == "" {
		return fields
	}
	normalize := func(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
	specs := splitAndTrim(spec)
	includeAll := false
	for _, s := range specs {
		if normalize(s) == "*" {
			includeAll = true
		}
	}
	for _, s := range specs {
		sn := normalize(s)
		if sn == "*" || sn == "" {
			continue
		}
		if sn[0] == '!' {
			fields[sn[1:]] = false
		} else {
			fields[sn] = true
		}
	}
	if includeAll {
		for _, f := range allFields {
			fn := normalize(f)
			if _, ok := fields[fn]; !ok {
				fields[fn] = true
			}
		}
	}
	return fields
}

// splitAndTrim splits on comma and trims whitespace
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func init() {
	Register("cert_fields", &CertFieldsExtractor{})
}
