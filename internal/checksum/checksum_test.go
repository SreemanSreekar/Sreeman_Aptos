package checksum

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFieldEncodings(t *testing.T) {
	if got := encInt(-12500); got != "-12500" {
		t.Fatalf("encInt: got %q", got)
	}
	if got := encDate(nil); got != `\N` {
		t.Fatalf("encDate(nil): got %q", got)
	}
	d := time.Date(2026, 11, 28, 0, 0, 0, 0, time.UTC)
	if got := encDate(&d); got != "2026-11-28" {
		t.Fatalf("encDate: got %q", got)
	}
	ts := time.Date(2026, 11, 28, 2, 0, 0, 123456000, time.UTC)
	if got := encTime(&ts); got != "2026-11-28T02:00:00.123456Z" {
		t.Fatalf("encTime: got %q", got)
	}
	if got := encText(nil); got != `\N` {
		t.Fatalf("encText(nil): got %q", got)
	}
	s := "S<1>&\""
	if got := encText(&s); got != `"S<1>&\""` {
		t.Fatalf("encText: got %q", got)
	}
	u := uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8")
	if got := encUUID(&u); got != `"6ba7b811-9dad-11d1-80b4-00c04fd430c8"` {
		t.Fatalf("encUUID: got %q", got)
	}
}
