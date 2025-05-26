package sink

import (
	"testing"

	"github.com/chtzvt/ctsnarf/internal/secrets"
)

func TestRegisterAndForName(t *testing.T) {
	// Dummy factory for testing
	dummyFactory := func(opts map[string]interface{}, s *secrets.Store) (Sink, error) {
		return nil, nil
	}
	Register("dummy", dummyFactory)

	got, ok := ForName("dummy")
	if !ok {
		t.Fatal("Expected to find registered sink, got none")
	}
	if got == nil {
		t.Fatal("Expected non-nil factory")
	}

	_, ok = ForName("not-exist")
	if ok {
		t.Fatal("Did not expect to find unregistered sink")
	}
}
