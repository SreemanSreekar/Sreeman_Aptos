package canonical

import (
	"bytes"
	"testing"
	"time"
)

func TestDayCloseEventIDDeterministic(t *testing.T) {
	id1 := DayCloseEventID("store-001", "2026-11-23")
	id2 := DayCloseEventID("store-001", "2026-11-23")
	id3 := DayCloseEventID("store-001", "2026-11-24")
	if id1 != id2 {
		t.Fatalf("expected deterministic UUIDv5, got %s != %s", id1, id2)
	}
	if id1 == id3 {
		t.Fatalf("expected distinct UUIDv5 for different days, got %s", id1)
	}
}

func TestCanonicalJSONSortingAndNoHTMLEscape(t *testing.T) {
	b, err := CanonicalJSON(map[string]any{
		"store_id":     "store-001",
		"business_day": "2026-11-23",
		"note":         "a&b<c>",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := `{"business_day":"2026-11-23","note":"a&b<c>","store_id":"store-001"}`
	if string(b) != want {
		t.Fatalf("got %s, want %s", string(b), want)
	}
}

func TestPayloadHashMicrosecondDeterminism(t *testing.T) {
	t1 := time.Date(2026, 11, 23, 15, 0, 0, 123456789, time.UTC)
	t2 := time.Date(2026, 11, 23, 15, 0, 0, 123456000, time.UTC) // same microsecond
	payload := []byte(`{"amount_minor":5000}`)

	h1 := PayloadHash("sale", "store-001", "term-1", t1, payload)
	h2 := PayloadHash("sale", "store-001", "term-1", t2, payload)
	if len(h1) != 32 {
		t.Fatalf("expected 32-byte SHA-256, got %d", len(h1))
	}
	if !bytes.Equal(h1, h2) {
		t.Fatalf("expected identical hash for same microsecond")
	}

	hDiff := PayloadHash("sale", "store-001", "term-1", t1, []byte(`{"amount_minor":5001}`))
	if bytes.Equal(h1, hDiff) {
		t.Fatalf("expected different hash for mutated payload")
	}
}
