package clock

import (
	"fmt"
	"net/http"
	"time"
	_ "time/tzdata" // Embed IANA timezone database so container tzdata never drifts.
)

const TZVersion = "go-tzdata-2024b"

// IngestNow returns the effective ingest timestamp (UTC, microsecond-truncated).
// Honors X-Simulated-Now ONLY when allowSimulated is true.
func IngestNow(r *http.Request, allowSimulated bool) (time.Time, *time.Time, error) {
	hdr := r.Header.Get("X-Simulated-Now")
	if hdr != "" {
		if !allowSimulated {
			return time.Time{}, nil, fmt.Errorf("SIMULATED_CLOCK_DISABLED")
		}
		parsed, err := time.Parse(time.RFC3339Nano, hdr)
		if err != nil {
			return time.Time{}, nil, fmt.Errorf("INVALID_SIMULATED_NOW: %w", err)
		}
		sim := parsed.UTC().Truncate(time.Microsecond)
		return sim, &sim, nil
	}
	return time.Now().UTC().Truncate(time.Microsecond), nil, nil
}

// BusinessDay returns the store-local business date (YYYY-MM-DD, midnight UTC date value)
// for instant t under the half-open [02:00, next 02:00) rule.
func BusinessDay(t time.Time, loc *time.Location) time.Time {
	local := t.In(loc)
	y, m, d := local.Date()
	day := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	if local.Hour() < 2 {
		day = day.AddDate(0, 0, -1)
	}
	return day
}

// CutoffUTC returns the exact UTC instant when businessDay closes (02:00:00 store-local on businessDay + 1).
// Handles DST spring-forward gaps (resolving to the first valid instant at/after 02:00 local, e.g. 03:00 EDT)
// and fall-back folds deterministically.
func CutoffUTC(businessDay time.Time, loc *time.Location) time.Time {
	next := businessDay.AddDate(0, 0, 1)
	y, m, d := next.Date()
	cutoffLocal := time.Date(y, m, d, 2, 0, 0, 0, loc)
	// On a spring-forward gap where 02:00 local does not exist, Go's time.Date rolls backward
	// into 01:xx standard time; advance to the first valid post-gap instant (e.g. 03:00 DST).
	if cutoffLocal.In(loc).Hour() < 2 {
		cutoffLocal = cutoffLocal.Add(time.Hour)
	}
	return cutoffLocal.UTC().Truncate(time.Microsecond)
}

// ValidateTimestamps enforces +5m future clock-skew and 48h max historical lateness.
func ValidateTimestamps(occurredAt, ingestNow time.Time, maxFutureSkewSec, maxLatenessHours int) (string, error) {
	if occurredAt.After(ingestNow.Add(time.Duration(maxFutureSkewSec) * time.Second)) {
		return "FUTURE_TIMESTAMP_EXCEEDED", fmt.Errorf("occurred_at exceeds max future skew of %ds", maxFutureSkewSec)
	}
	if occurredAt.Before(ingestNow.Add(-time.Duration(maxLatenessHours) * time.Hour)) {
		return "EVENT_TOO_OLD", fmt.Errorf("occurred_at exceeds max lateness of %dh", maxLatenessHours)
	}
	return "", nil
}
