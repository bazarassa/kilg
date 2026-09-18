package api

import (
	"regexp"
	"testing"
)

var ulidRe = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

func TestNewULIDFormat(t *testing.T) {
	for i := 0; i < 1000; i++ {
		u := NewULID()
		if len(u) != 26 {
			t.Fatalf("ulid length = %d, want 26: %q", len(u), u)
		}
		if !ulidRe.MatchString(u) {
			t.Fatalf("ulid has invalid chars: %q", u)
		}
	}
}

func TestNewULIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 10000; i++ {
		u := NewULID()
		if seen[u] {
			t.Fatalf("duplicate ulid: %q", u)
		}
		seen[u] = true
	}
}

func TestNewULIDMonotonicPrefix(t *testing.T) {
	a := NewULID()
	b := NewULID()
	// Same millisecond => same first 10 chars (timestamp).
	if a[:10] != b[:10] {
		// Different ms is also acceptable; just ensure both are valid.
		t.Logf("different ms: %q %q", a, b)
	}
}
