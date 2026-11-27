package pos_ruleset

import (
	"errors"
	"math"
	"testing"
	"time"
)

func testGrant() *LocalSliceGrant {
	now := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	loyaltyCap := int64(2500)
	return &LocalSliceGrant{
		StoreID:            "S1",
		TerminalID:         "T1",
		EpochGen:           1,
		SliceMinor:         1250,
		LoyaltySlicePoints: &loyaltyCap,
		IssuedAtUTC:        now,
		SliceExpiresAtUTC:  now.Add(OfflineSliceTTL),
		BootID:             "boot-1",
		GrantBoottimeMs:    100_000,
		HWMUTC:             now,
		Cards: map[string]CardSnapshot{
			"GC-1": {
				CardID:         "GC-1",
				BalanceMinor:   3000,
				Status:         "ACTIVE",
				EligibleStores: []string{"S1", "S2", "S3", "S4", "S5"},
			},
			"GC-OTHER": {
				CardID:         "GC-OTHER",
				BalanceMinor:   3000,
				Status:         "ACTIVE",
				EligibleStores: []string{"S2", "S3", "S4", "S5", "S6"},
			},
			"GC-LOW": {
				CardID:         "GC-LOW",
				BalanceMinor:   500,
				Status:         "ACTIVE",
				EligibleStores: []string{"S1"},
			},
		},
	}
}

func TestCanRedeemGiftCard_HappyAndCap(t *testing.T) {
	g := testGrant()
	wall := g.IssuedAtUTC.Add(10 * time.Minute)
	if err := CanRedeemGiftCard(g, "GC-1", 1000, "boot-1", 700_000, wall); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := CanRedeemGiftCard(g, "GC-1", 300, "boot-1", 701_000, wall.Add(time.Second)); !errors.Is(err, ErrOfflineCapExceeded) {
		t.Fatalf("expected ErrOfflineCapExceeded, got %v", err)
	}
	if err := CanRedeemGiftCard(g, "GC-LOW", 600, "boot-1", 702_000, wall.Add(2*time.Second)); !errors.Is(err, ErrOfflineCapExceeded) {
		t.Fatalf("expected ErrOfflineCapExceeded for low snapshot balance, got %v", err)
	}
}

func TestCanRedeemGiftCard_IneligibleStore(t *testing.T) {
	g := testGrant()
	wall := g.IssuedAtUTC.Add(time.Minute)
	if err := CanRedeemGiftCard(g, "GC-OTHER", 500, "boot-1", 160_000, wall); !errors.Is(err, ErrOfflineStoreNotEligible) {
		t.Fatalf("expected ErrOfflineStoreNotEligible, got %v", err)
	}
}

func TestCanRedeemGiftCard_RebootAndRollbackAndTTL(t *testing.T) {
	g := testGrant()
	wall := g.IssuedAtUTC.Add(time.Minute)

	// Backward clock rollback
	if err := CanRedeemGiftCard(g, "GC-1", 100, "boot-1", 160_000, g.IssuedAtUTC.Add(-time.Second)); !errors.Is(err, ErrClockRollback) {
		t.Fatalf("expected ErrClockRollback, got %v", err)
	}

	// Boottime TTL expired (>= 6h)
	if err := CanRedeemGiftCard(g, "GC-1", 100, "boot-1", g.GrantBoottimeMs+OfflineSliceTTLMs, wall); !errors.Is(err, ErrOfflineSliceExpired) {
		t.Fatalf("expected ErrOfflineSliceExpired on boottime, got %v", err)
	}

	// Reboot changes boot_id and permanently fences locally
	if err := CanRedeemGiftCard(g, "GC-1", 100, "boot-2", 160_000, wall); !errors.Is(err, ErrBootIDChanged) {
		t.Fatalf("expected ErrBootIDChanged, got %v", err)
	}
	if !g.LocalFenced {
		t.Fatalf("expected LocalFenced=true after reboot")
	}
	if err := CanRedeemGiftCard(g, "GC-1", 100, "boot-1", 160_000, wall); !errors.Is(err, ErrTerminalLocalFenced) {
		t.Fatalf("expected ErrTerminalLocalFenced, got %v", err)
	}
}

func TestCanBurnLoyalty_CapAndUnlimited(t *testing.T) {
	g := testGrant()
	wall := g.IssuedAtUTC.Add(time.Minute)

	if err := CanBurnLoyalty(g, "ACC-1", 2000, "boot-1", 160_000, wall); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := CanBurnLoyalty(g, "ACC-1", 600, "boot-1", 161_000, wall.Add(time.Second)); !errors.Is(err, ErrOfflineLoyaltyCapExceeded) {
		t.Fatalf("expected ErrOfflineLoyaltyCapExceeded, got %v", err)
	}

	g.LoyaltySlicePoints = nil // unlimited
	if err := CanBurnLoyalty(g, "ACC-1", 10000, "boot-1", 162_000, wall.Add(2*time.Second)); err != nil {
		t.Fatalf("expected unlimited loyalty burn to succeed, got %v", err)
	}
}

func TestCanRedeemGiftCard_FailClosedFrozenZeroUnknown(t *testing.T) {
	g := testGrant()
	g.Cards["GC-OVERSPENT"] = CardSnapshot{
		CardID:         "GC-OVERSPENT",
		BalanceMinor:   -500,
		Status:         "OVERSPENT_FROZEN",
		EligibleStores: []string{"S1"},
	}
	g.Cards["GC-ADMIN"] = CardSnapshot{
		CardID:         "GC-ADMIN",
		BalanceMinor:   2000,
		Status:         "ADMIN_FROZEN",
		EligibleStores: []string{"S1"},
	}
	g.Cards["GC-ZERO"] = CardSnapshot{
		CardID:         "GC-ZERO",
		BalanceMinor:   0,
		Status:         "ACTIVE",
		EligibleStores: []string{"S1"},
	}
	wall := g.IssuedAtUTC.Add(time.Minute)

	if err := CanRedeemGiftCard(g, "GC-OVERSPENT", 100, "boot-1", 160_000, wall); !errors.Is(err, ErrCardNotActive) {
		t.Fatalf("expected ErrCardNotActive for OVERSPENT_FROZEN, got %v", err)
	}
	if err := CanRedeemGiftCard(g, "GC-ADMIN", 100, "boot-1", 161_000, wall); !errors.Is(err, ErrCardNotActive) {
		t.Fatalf("expected ErrCardNotActive for ADMIN_FROZEN, got %v", err)
	}
	if err := CanRedeemGiftCard(g, "GC-ZERO", 100, "boot-1", 162_000, wall); !errors.Is(err, ErrOfflineCapExceeded) {
		t.Fatalf("expected ErrOfflineCapExceeded for zero balance, got %v", err)
	}
	if err := CanRedeemGiftCard(g, "GC-UNKNOWN", 100, "boot-1", 163_000, wall); !errors.Is(err, ErrCardNotFound) {
		t.Fatalf("expected ErrCardNotFound for omitted card, got %v", err)
	}
}

func TestClockHardening_EmptyBootIDBackwardBoottimeAndDriftTTL(t *testing.T) {
	// Empty boot_id must fail closed
	gEmpty := testGrant()
	gEmpty.BootID = ""
	wall := gEmpty.IssuedAtUTC.Add(time.Minute)
	if err := CanRedeemGiftCard(gEmpty, "GC-1", 100, "boot-1", 160_000, wall); !errors.Is(err, ErrBootIDChanged) {
		t.Fatalf("expected ErrBootIDChanged on empty grant BootID, got %v", err)
	}
	if !gEmpty.LocalFenced {
		t.Fatalf("expected LocalFenced=true when BootID missing")
	}

	// Backward monotonic boottime must fail closed
	gBack := testGrant()
	if err := CanRedeemGiftCard(gBack, "GC-1", 100, "boot-1", gBack.GrantBoottimeMs-1, wall); !errors.Is(err, ErrClockRollback) {
		t.Fatalf("expected ErrClockRollback on backward boottime, got %v", err)
	}

	// Drift-tightened TTL boundary (1000 ppm on 6h = 21.6s earlier)
	gDrift := testGrant()
	if err := CanRedeemGiftCard(gDrift, "GC-1", 100, "boot-1", gDrift.GrantBoottimeMs+EffectiveBoottimeTTLMs(), wall); !errors.Is(err, ErrOfflineSliceExpired) {
		t.Fatalf("expected ErrOfflineSliceExpired at drift-tightened TTL boundary, got %v", err)
	}
	if err := CanRedeemGiftCard(gDrift, "GC-1", 100, "boot-1", gDrift.GrantBoottimeMs+EffectiveBoottimeTTLMs()-1, wall); err != nil {
		t.Fatalf("expected redemption 1ms before drift-tightened TTL to succeed, got %v", err)
	}
}

func TestCapOverflowProtectionAndFiveStoreLimit(t *testing.T) {
	// 1. Gift-card int64 overflow bypass regression: spent=1 + MaxInt64 must NOT wrap and pass cap
	g := testGrant()
	wall := g.IssuedAtUTC.Add(time.Minute)
	if err := CanRedeemGiftCard(g, "GC-1", 1, "boot-1", 160_000, wall); err != nil {
		t.Fatalf("initial spend of 1 failed: %v", err)
	}
	if err := CanRedeemGiftCard(g, "GC-1", math.MaxInt64, "boot-1", 161_000, wall.Add(time.Second)); !errors.Is(err, ErrOfflineCapExceeded) {
		t.Fatalf("expected ErrOfflineCapExceeded for MaxInt64 overflow attempt, got %v", err)
	}
	if g.SpentByCardMinor["GC-1"] != 1 {
		t.Fatalf("expected counter unchanged at 1, got %d", g.SpentByCardMinor["GC-1"])
	}

	// 2. Loyalty int64 overflow bypass regression (capped and unlimited)
	if err := CanBurnLoyalty(g, "ACC-1", 1, "boot-1", 162_000, wall.Add(2*time.Second)); err != nil {
		t.Fatalf("initial loyalty burn of 1 failed: %v", err)
	}
	if err := CanBurnLoyalty(g, "ACC-1", math.MaxInt64, "boot-1", 163_000, wall.Add(3*time.Second)); !errors.Is(err, ErrOfflineLoyaltyCapExceeded) {
		t.Fatalf("expected ErrOfflineLoyaltyCapExceeded for capped MaxInt64 overflow, got %v", err)
	}
	g.LoyaltySlicePoints = nil // unlimited mode must still reject counter wrap
	if err := CanBurnLoyalty(g, "ACC-1", math.MaxInt64, "boot-1", 164_000, wall.Add(4*time.Second)); !errors.Is(err, ErrOfflineLoyaltyCapExceeded) {
		t.Fatalf("expected ErrOfflineLoyaltyCapExceeded for unlimited counter wrap, got %v", err)
	}
	if g.TotalLoyaltyBurnedPoints != 1 {
		t.Fatalf("expected loyalty counter unchanged at 1, got %d", g.TotalLoyaltyBurnedPoints)
	}

	// 3. Standalone ruleset must reject cards with > 5 eligible stores
	g.Cards["GC-SIX"] = CardSnapshot{
		CardID:         "GC-SIX",
		BalanceMinor:   5000,
		Status:         "ACTIVE",
		EligibleStores: []string{"S1", "S2", "S3", "S4", "S5", "S6"},
	}
	if err := CanRedeemGiftCard(g, "GC-SIX", 100, "boot-1", 165_000, wall.Add(5*time.Second)); !errors.Is(err, ErrOfflineStoreNotEligible) {
		t.Fatalf("expected ErrOfflineStoreNotEligible for 6 eligible stores, got %v", err)
	}
}
