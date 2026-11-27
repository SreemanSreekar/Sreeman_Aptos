package db

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryablePgErr(t *testing.T) {
	cases := []struct {
		code string
		want bool
	}{
		{"40P01", true},  // deadlock_detected
		{"40001", true},  // serialization_failure
		{"22023", false}, // invalid_parameter_value (our custom triggers)
		{"23505", false}, // unique_violation
	}
	for _, tc := range cases {
		err := &pgconn.PgError{Code: tc.code}
		if got := isRetryablePgErr(err); got != tc.want {
			t.Fatalf("code %s: got %v, want %v", tc.code, got, tc.want)
		}
	}
	if isRetryablePgErr(errors.New("plain error")) {
		t.Fatalf("expected non-pg error not retryable")
	}
}
