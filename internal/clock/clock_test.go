package clock

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestBusinessDay0200Boundary(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load tz: %v", err)
	}

	before := time.Date(2026, 11, 24, 1, 59, 59, 999999000, loc)
	at := time.Date(2026, 11, 24, 2, 0, 0, 0, loc)

	if got := BusinessDay(before, loc).Format("2006-01-02"); got != "2026-11-23" {
		t.Fatalf("01:59:59.999999 local: got %s, want 2026-11-23", got)
	}
	if got := BusinessDay(at, loc).Format("2006-01-02"); got != "2026-11-24" {
		t.Fatalf("02:00:00.000000 local: got %s, want 2026-11-24", got)
	}
}

func TestCutoffUTCDSTSpringForwardAndFallBack(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load tz: %v", err)
	}

	// Spring-forward: 2026-03-08 02:00 EST jumps to 03:00 EDT (07:00Z).
	// Business day 2026-03-07 closes at 2026-03-08 02:00 local -> 07:00:00Z.
	springDay := time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC)
	springCutoff := CutoffUTC(springDay, loc)
	wantSpring := time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC)
	if !springCutoff.Equal(wantSpring) {
		t.Fatalf("spring-forward cutoff: got %s, want %s", springCutoff, wantSpring)
	}

	// Fall-back: 2026-11-01 01:00-01:59 repeats; 02:00 EST is unambiguous = 07:00:00Z.
	fallDay := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	fallCutoff := CutoffUTC(fallDay, loc)
	wantFall := time.Date(2026, 11, 1, 7, 0, 0, 0, time.UTC)
	if !fallCutoff.Equal(wantFall) {
		t.Fatalf("fall-back cutoff: got %s, want %s", fallCutoff, wantFall)
	}
}

func TestValidateTimestampsSkewAndLateness(t *testing.T) {
	now := time.Date(2026, 11, 25, 15, 0, 0, 0, time.UTC)

	if code, err := ValidateTimestamps(now.Add(5*time.Minute+time.Second), now, 300, 48); err == nil || code != "FUTURE_TIMESTAMP_EXCEEDED" {
		t.Fatalf("expected FUTURE_TIMESTAMP_EXCEEDED, got code=%q err=%v", code, err)
	}
	if code, err := ValidateTimestamps(now.Add(-48*time.Hour-time.Second), now, 300, 48); err == nil || code != "EVENT_TOO_OLD" {
		t.Fatalf("expected EVENT_TOO_OLD, got code=%q err=%v", code, err)
	}
	if code, err := ValidateTimestamps(now.Add(-47*time.Hour), now, 300, 48); err != nil || code != "" {
		t.Fatalf("expected valid 47h-late event, got code=%q err=%v", code, err)
	}
}

func TestIngestNowSimulatedClockGate(t *testing.T) {
	req := httptest.NewRequest("POST", "/v1/batches", nil)
	req.Header.Set("X-Simulated-Now", "2026-11-25T15:00:00Z")

	if _, _, err := IngestNow(req, false); err == nil {
		t.Fatalf("expected error when ALLOW_SIMULATED_CLOCK=false")
	}
	got, simPtr, err := IngestNow(req, true)
	if err != nil || simPtr == nil {
		t.Fatalf("expected simulated time when allowed, got err=%v", err)
	}
	if got.Format(time.RFC3339) != "2026-11-25T15:00:00Z" {
		t.Fatalf("got %s, want 2026-11-25T15:00:00Z", got.Format(time.RFC3339))
	}
}
