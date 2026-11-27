package pos_ruleset

import (
	"errors"
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
