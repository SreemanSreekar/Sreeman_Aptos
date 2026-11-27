package giftcards

import (
	"testing"

	"github.com/google/uuid"
)

func TestEvaluateGiftCardPrefix_OverspendAndFreeze(t *testing.T) {
	e1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	e2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	e3 := uuid.MustParse("00000000-0000-0000-0000-000000000003")

	bal, status, flags := EvaluateGiftCardPrefix("GC-1", 3000, false, []PrefixRedeemEvent{
		{EventID: e1, StoreID: "S1", AmountMinor: 2500},
		{EventID: e2, StoreID: "S2", AmountMinor: 2500},
		{EventID: e3, StoreID: "S3", AmountMinor: 1000},
	})

	if bal != -3000 {
		t.Fatalf("expected balance -3000, got %d", bal)
	}
	if status != "OVERSPENT_FROZEN" {
		t.Fatalf("expected OVERSPENT_FROZEN, got %s", status)
	}
	if len(flags) != 2 {
		t.Fatalf("expected 2 overspend flags, got %d", len(flags))
	}
	if flags[0].EventID != e2 || flags[0].BalanceAfterMinor != -2000 || flags[0].OverspendDeltaMinor != 2000 {
		t.Fatalf("unexpected first flag: %+v", flags[0])
	}
	if flags[1].EventID != e3 || flags[1].BalanceAfterMinor != -3000 || flags[1].OverspendDeltaMinor != 1000 {
		t.Fatalf("unexpected second flag: %+v", flags[1])
	}
}

func TestEvaluateGiftCardPrefix_AdminFrozenPreserved(t *testing.T) {
	e1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	bal, status, flags := EvaluateGiftCardPrefix("GC-2", 5000, true, []PrefixRedeemEvent{
		{EventID: e1, StoreID: "S1", AmountMinor: 1000},
	})
	if bal != 4000 || status != "ADMIN_FROZEN" || len(flags) != 0 {
		t.Fatalf("unexpected result: bal=%d status=%s flags=%d", bal, status, len(flags))
	}
}
