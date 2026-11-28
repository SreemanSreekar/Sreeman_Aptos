package ingest

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func i64(v int64) *int64    { return &v }
func i32(v int32) *int32    { return &v }
func strp(v string) *string { return &v }

func TestPrepareOfflineEventsRejectsClientChannel(t *testing.T) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	_, _, he := PrepareOfflineEvents(BatchRequest{
		StoreID: "store-001", TerminalID: "term-1",
		Events: []BatchEventInput{{
			EventID:     uuid.NewString(),
			EventType:   "sale",
			Channel:     strp("ONLINE"),
			OccurredAt:  "2026-11-25T15:00:00Z",
			AmountMinor: i64(1000),
		}},
	}, now, 300, 48)
	if he == nil || he.Status != 400 || he.Code != "CLIENT_CHANNEL_FORBIDDEN" {
		t.Fatalf("expected 400 CLIENT_CHANNEL_FORBIDDEN, got %+v", he)
	}
}

func TestPrepareOfflineEventsInBatchDedupeAndPayloadMismatch(t *testing.T) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	evID := uuid.NewString()

	// Identical duplicate within batch -> deduplicated cleanly
	out, dupes, he := PrepareOfflineEvents(BatchRequest{
		StoreID: "store-001", TerminalID: "term-1",
		Events: []BatchEventInput{
			{EventID: evID, EventType: "sale", OccurredAt: "2026-11-25T15:00:00Z", AmountMinor: i64(1000)},
			{EventID: evID, EventType: "sale", OccurredAt: "2026-11-25T15:00:00Z", AmountMinor: i64(1000)},
		},
	}, now, 300, 48)
	if he != nil || len(out) != 1 || dupes != 1 {
		t.Fatalf("expected 1 event + 1 dupe, got len=%d dupes=%d err=%+v", len(out), dupes, he)
	}

	// Conflicting duplicate within batch -> 409 EVENT_ID_PAYLOAD_MISMATCH
	_, _, he = PrepareOfflineEvents(BatchRequest{
		StoreID: "store-001", TerminalID: "term-1",
		Events: []BatchEventInput{
			{EventID: evID, EventType: "sale", OccurredAt: "2026-11-25T15:00:00Z", AmountMinor: i64(1000)},
			{EventID: evID, EventType: "sale", OccurredAt: "2026-11-25T15:00:00Z", AmountMinor: i64(9999)},
		},
	}, now, 300, 48)
	if he == nil || he.Status != 409 || he.Code != "EVENT_ID_PAYLOAD_MISMATCH" {
		t.Fatalf("expected 409 EVENT_ID_PAYLOAD_MISMATCH, got %+v", he)
	}
}

func TestPrepareOfflineEventsTopologicalPreSort(t *testing.T) {
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	saleID := uuid.NewString()
	retID := uuid.NewString()
	voidID := uuid.NewString()
	gcID := uuid.NewString()

	// Intentionally supply reversed order: giftcard -> return(10:05) -> void(10:02) -> sale(10:00)
	out, _, he := PrepareOfflineEvents(BatchRequest{
		StoreID: "store-001", TerminalID: "term-1",
		Events: []BatchEventInput{
			{EventID: gcID, EventType: "giftcard_redeem", CardID: strp("gc-1"), EpochGen: i64(1), TerminalEpochSeq: i32(1), OccurredAt: "2026-11-25T15:00:00Z", AmountMinor: i64(500)},
			{EventID: retID, EventType: "return", ParentSaleID: strp(saleID), OccurredAt: "2026-11-25T15:05:00Z", AmountMinor: i64(2000)},
			{EventID: voidID, EventType: "void", ParentSaleID: strp(saleID), OccurredAt: "2026-11-25T15:02:00Z", AmountMinor: i64(5000)},
			{EventID: saleID, EventType: "sale", OccurredAt: "2026-11-25T15:00:00Z", AmountMinor: i64(5000)},
		},
	}, now, 300, 48)
	if he != nil {
		t.Fatalf("unexpected error: %+v", he)
	}
	wantOrder := []string{saleID, voidID, retID, gcID}
	for i, want := range wantOrder {
		if out[i].EventID.String() != want {
			t.Fatalf("pos %d: got %s (%s), want %s", i, out[i].EventID, out[i].EventType, want)
		}
	}
	// Verify return/void normalized sale_id == parent_sale_id
	if out[1].SaleID == nil || out[1].SaleID.String() != saleID {
		t.Fatalf("expected void sale_id normalized to parent_sale_id")
	}
}
