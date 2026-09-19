package api

import (
	"regexp"
	"testing"
)

var ulidRe = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

func TestNewULIDFormat(t *testing.T) {
	t.Parallel()

	for i := 0; i < 1000; i++ {
		u := NewULID()

		if len(u) != 26 {
			t.Fatalf("ULID length = %d, want 26: %q", len(u), u)
		}

		if !ulidRe.MatchString(u) {
			t.Fatalf("ULID has invalid characters: %q", u)
		}
	}
}

func TestNewULIDUnique(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{}, 10000)

	for i := 0; i < 10000; i++ {
		u := NewULID()

		if _, exists := seen[u]; exists {
			t.Fatalf("duplicate ULID: %q", u)
		}

		seen[u] = struct{}{}
	}
}

func TestNewULIDMonotonicPrefix(t *testing.T) {
	t.Parallel()

	const count = 1000

	ids := make([]string, 0, count)

	for i := 0; i < count; i++ {
		u := NewULID()

		if len(u) != 26 {
			t.Fatalf("ULID length = %d, want 26: %q", len(u), u)
		}

		if !ulidRe.MatchString(u) {
			t.Fatalf("ULID has invalid characters: %q", u)
		}

		ids = append(ids, u)
	}

	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf(
				"ULIDs are not monotonic at index %d: previous=%q current=%q",
				i,
				ids[i-1],
				ids[i],
			)
		}
	}
}
