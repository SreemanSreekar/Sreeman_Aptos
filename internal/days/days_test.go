package days

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
)

func TestCloseDayInputAndSimulatedClockValidation(t *testing.T) {
	h := &Handler{Cfg: &config.Config{AllowSimulatedClock: false}}

	// Invalid date format -> 400 INVALID_BUSINESS_DAY
	req := httptest.NewRequest(http.MethodPost, "/v1/stores/store-1/days/not-a-date/close", nil)
	req.SetPathValue("id", "store-1")
	req.SetPathValue("day", "not-a-date")
	rec := httptest.NewRecorder()
	h.CloseDay(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid date: got %d, want 400", rec.Code)
	}

	// X-Simulated-Now rejected when AllowSimulatedClock=false -> 400
	req2 := httptest.NewRequest(http.MethodPost, "/v1/stores/store-1/days/2026-11-23/close", nil)
	req2.SetPathValue("id", "store-1")
	req2.SetPathValue("day", "2026-11-23")
	req2.Header.Set("X-Simulated-Now", "2026-11-24T08:00:00Z")
	rec2 := httptest.NewRecorder()
	h.CloseDay(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("disabled simulated clock: got %d, want 400", rec2.Code)
	}
}

func TestGetStoreTotalsInputValidation(t *testing.T) {
	h := &Handler{Cfg: &config.Config{}}

	// Missing day query param -> 400
	req := httptest.NewRequest(http.MethodGet, "/v1/stores/store-1/totals", nil)
	req.SetPathValue("id", "store-1")
	rec := httptest.NewRecorder()
	h.GetStoreTotals(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("missing day: got %d, want 400", rec.Code)
	}

	// Malformed day query param -> 400
	req2 := httptest.NewRequest(http.MethodGet, "/v1/stores/store-1/totals?day=2026/11/23", nil)
	req2.SetPathValue("id", "store-1")
	rec2 := httptest.NewRecorder()
	h.GetStoreTotals(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("bad day format: got %d, want 400", rec2.Code)
	}
}
