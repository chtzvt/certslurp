package job

type MatchConfig struct {
	SubjectRegex     string `json:"subject_regex,omitempty"`
	IssuerRegex      string `json:"issuer_regex,omitempty"`
	Serial           string `json:"serial,omitempty"`
	SCTTimestamp     uint64 `json:"sct_timestamp,omitempty"`
	Domain           string `json:"domain,omitempty"`
	ParseErrors      string `json:"parse_errors,omitempty"` // "all" or "nonfatal"
	ValidationErrors bool   `json:"validation_errors,omitempty"`
	PrecertsOnly     bool   `json:"precerts_only,omitempty"`
	Workers          int    `json:"workers,omitempty"`
}
