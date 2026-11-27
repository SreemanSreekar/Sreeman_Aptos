package loyalty

import (
	"testing"

	"github.com/google/uuid"
)

func TestEvaluateLoyaltyPrefix_ConvergenceAndRecovery(t *testing.T) {
	e1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	e2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	e3 := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	e4 := uuid.MustParse("00000000-0000-0000-0000-000000000004")

	// Start with +500, burn 300, burn 300 -> -100 (NEGATIVE_RESTRICTED), then earn +200 -> +100 (ACTIVE, flag resolved)
	events := []PrefixLoyaltyEvent{
		{EventID: e1, EventType: "loyalty_earn", Points: 500},
		{EventID: e2, EventType: "loyalty_burn", Points: 300},
		{EventID: e3, EventType: "loyalty_burn", Points: 300},
		{EventID: e4, EventType: "loyalty_earn", Points: 200},
	}

	earned, burned, bal, status, flags := EvaluateLoyaltyPrefix("ACC-1", events)
	if earned != 700 || burned != 600 || bal != 100 || status != "ACTIVE" {
		t.Fatalf("unexpected totals: earned=%d burned=%d bal=%d status=%s", earned, burned, bal, status)
	}
	if len(flags) != 1 || flags[0].TriggerEventID != e3 || flags[0].DeficitPoints != 100 || flags[0].ResolvedByEventID == nil || *flags[0].ResolvedByEventID != e4 {
		t.Fatalf("unexpected flags: %+v", flags)
	}

	// Without e4, status should be NEGATIVE_RESTRICTED and flag unresolved
	earned, burned, bal, status, flags = EvaluateLoyaltyPrefix("ACC-1", events[:3])
	if earned != 500 || burned != 600 || bal != -100 || status != "NEGATIVE_RESTRICTED" {
		t.Fatalf("unexpected restricted totals: earned=%d burned=%d bal=%d status=%s", earned, burned, bal, status)
	}
	if len(flags) != 1 || flags[0].ResolvedByEventID != nil {
		t.Fatalf("expected unresolved flag, got %+v", flags)
	}
}
