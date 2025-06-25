package worker

import "testing"

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		wantErr  bool
	}{
		{"https://mysite.domain.com/some/path", "mysite_domain_com__some__path", false},
		{"http://example.com/", "example_com", false},
		{"https://sub.domain.co.uk/a/b/c?query=1", "sub_domain_co_uk__a__b__c", false},
		{"https://localhost:8080/test", "localhost__test", false},
		{"ftp://invalid.scheme.com", "invalid_scheme_com", false},
		{"not a url", "", true},
	}

	for _, tt := range tests {
		got, err := normalizeURL(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("normalizeURL(%q) error = %v, wantErr = %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.expected {
			t.Errorf("normalizeURL(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
