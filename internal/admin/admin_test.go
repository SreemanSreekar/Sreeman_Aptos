package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestExtractPg22023(t *testing.T) {
	pgErr := &pgconn.PgError{
		Code:    "22023",
		Message: "GIFT_CARD_SEED_CONFLICT: card GC-1 already exists",
	}
	code, msg, ok := extractPg22023(pgErr)
	if !ok || code != "GIFT_CARD_SEED_CONFLICT" || msg != pgErr.Message {
		t.Fatalf("unexpected result: ok=%v code=%q msg=%q", ok, code, msg)
	}
}

func TestRebuildQueryValidation(t *testing.T) {
	h := &Handler{}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/rebuild?mode=bogus", nil)
	h.Rebuild(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid mode, got %d", rec.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["error"] != "INVALID_REBUILD_MODE" {
		t.Fatalf("expected INVALID_REBUILD_MODE, got %v", body["error"])
	}

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/v1/admin/rebuild?mode=snapshot&snapshot_id=-5", nil)
	h.Rebuild(rec2, req2)
	if rec2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid snapshot_id, got %d", rec2.Code)
	}
	body = nil
	_ = json.Unmarshal(rec2.Body.Bytes(), &body)
	if body["error"] != "INVALID_SNAPSHOT_ID" {
		t.Fatalf("expected INVALID_SNAPSHOT_ID, got %v", body["error"])
	}
}
