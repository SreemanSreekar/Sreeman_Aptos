package epochs

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ingest"
)

func TestExtractPgCodeAndMsg(t *testing.T) {
	pgErr := &pgconn.PgError{
		Code:    "22023",
		Message: "SLICE_NOT_EXPIRED_YET: fence threshold not yet reached",
	}
	code, msg, ok := extractPgCodeAndMsg(pgErr)
	if !ok || code != "SLICE_NOT_EXPIRED_YET" || msg != pgErr.Message {
		t.Fatalf("unexpected result: ok=%v code=%q msg=%q", ok, code, msg)
	}
	if _, _, ok := extractPgCodeAndMsg(&pgconn.PgError{Code: "23505", Message: "dup"}); ok {
		t.Fatalf("expected non-22023 error to return ok=false")
	}
}

func TestValidateSyncEventOrigins(t *testing.T) {
	if err := validateSyncEventOrigins("S1", "T1", []ingest.BatchEventInput{
		{StoreID: "", TerminalID: ""},
		{StoreID: "S1", TerminalID: "T1"},
	}); err != nil {
		t.Fatalf("expected valid origins to pass, got %v", err)
	}
	if err := validateSyncEventOrigins("S1", "T1", []ingest.BatchEventInput{
		{StoreID: "S2", TerminalID: "T1"},
	}); err == nil {
		t.Fatalf("expected mismatched store_id to fail")
	}
	if err := validateSyncEventOrigins("S1", "T1", []ingest.BatchEventInput{
		{StoreID: "S1", TerminalID: "T2"},
	}); err == nil {
		t.Fatalf("expected mismatched terminal_id to fail")
	}
}

func TestDeriveExpectedEventIDs_MixedEvents(t *testing.T) {
	eSale := uuid.New()
	eGC := uuid.New()
	eBurn := uuid.New()
	eOldEpoch := uuid.New()

	ep1 := int64(1)
	ep2 := int64(2)

	ids, err := deriveExpectedEventIDs([]ingest.BatchEventInput{
		{EventID: eSale.String(), EventType: "sale"},
		{EventID: eGC.String(), EventType: "giftcard_redeem", EpochGen: &ep2},
		{EventID: eBurn.String(), EventType: "loyalty_burn", EpochGen: &ep2},
		{EventID: eGC.String(), EventType: "giftcard_redeem", EpochGen: &ep2}, // duplicate in batch
		{EventID: eOldEpoch.String(), EventType: "giftcard_redeem", EpochGen: &ep1},
	}, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 || ids[0] != eGC || ids[1] != eBurn {
		t.Fatalf("expected only epoch-2 events [%s, %s], got %v", eGC, eBurn, ids)
	}
}
