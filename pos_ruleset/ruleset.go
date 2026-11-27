// Package pos_ruleset implements the zero-dependency deterministic offline ruleset
// executed by POS terminals while disconnected from central HQ.
package pos_ruleset

import (
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	// OfflineSliceTTL is the maximum duration (6 hours) an offline slice grant remains valid.
	OfflineSliceTTL   = 6 * time.Hour
	OfflineSliceTTLMs = int64(21_600_000)

	// MaxOfflineEligibleStores is the chain-wide hard bound (K_max = 5) on eligible offline stores per card.
	MaxOfflineEligibleStores = 5

	// DefaultMaxDriftPPM is the bounded hardware oscillator drift (1000 ppm = 0.1%).
	DefaultMaxDriftPPM = 1000
	// DefaultSyncMaxRTT is the max allowed sync RPC round-trip time (30s).
	DefaultSyncMaxRTT = 30 * time.Second
)

var (
	ErrInvalidAmount            = errors.New("INVALID_AMOUNT")
	ErrTerminalLocalFenced      = errors.New("TERMINAL_LOCAL_FENCED")
	ErrBootIDChanged            = errors.New("OFFLINE_REBOOT_FENCED")
	ErrClockRollback            = errors.New("CLOCK_ROLLBACK_DETECTED")
	ErrOfflineSliceExpired      = errors.New("OFFLINE_SLICE_EXPIRED")
	ErrOfflineStoreNotEligible   = errors.New("OFFLINE_STORE_NOT_ELIGIBLE")
	ErrCardNotFound              = errors.New("CARD_NOT_FOUND")
	ErrCardNotActive             = errors.New("CARD_NOT_ACTIVE")
	ErrOfflineCapExceeded        = errors.New("OFFLINE_CAP_EXCEEDED")
	ErrOfflineLoyaltyCapExceeded = errors.New("OFFLINE_LOYALTY_CAP_EXCEEDED")
)

// EffectiveBoottimeTTLMs returns the offline slice TTL conservatively tightened
// by DefaultMaxDriftPPM (1000 ppm = 0.1%) to account for hardware oscillator drift.
func EffectiveBoottimeTTLMs() int64 {
	return OfflineSliceTTLMs - (OfflineSliceTTLMs*DefaultMaxDriftPPM)/1_000_000
}

// CardSnapshot holds the last-known ACTIVE card state delivered to the terminal at sync.
type CardSnapshot struct {
	CardID         string   `json:"card_id"`
	BalanceMinor   int64    `json:"balance_minor"`
	Status         string   `json:"status"`
	EligibleStores []string `json:"eligible_stores"`
}

// LocalSliceGrant is the durable offline lease state held by a POS terminal.
type LocalSliceGrant struct {
	StoreID                  string                  `json:"store_id"`
	TerminalID               string                  `json:"terminal_id"`
	EpochGen                 int64                   `json:"epoch_gen"`
	SliceMinor               int64                   `json:"slice_minor"`
	LoyaltySlicePoints       *int64                  `json:"loyalty_slice_points"`
	IssuedAtUTC              time.Time               `json:"issued_at_utc"`
	SliceExpiresAtUTC        time.Time               `json:"slice_expires_at_utc"`
	BootID                   string                  `json:"boot_id,omitempty"`
	GrantBoottimeMs          int64                   `json:"grant_boottime_ms,omitempty"`
	HWMUTC                   time.Time               `json:"hwm_utc,omitempty"`
	LocalFenced              bool                    `json:"local_fenced,omitempty"`
	SpentByCardMinor         map[string]int64        `json:"spent_by_card_minor,omitempty"`
	TotalLoyaltyBurnedPoints int64                   `json:"total_loyalty_burned_points,omitempty"`
	Cards                    map[string]CardSnapshot `json:"cards,omitempty"`
}

// checkLocalTimeAndFence enforces all fail-closed local clock, drift, and reboot guards.
func checkLocalTimeAndFence(grant *LocalSliceGrant, currentBootID string, currentBoottimeMs int64, currentWallUTC time.Time) error {
	if grant.LocalFenced {
		return ErrTerminalLocalFenced
	}
	if grant.BootID == "" || currentBootID == "" || currentBootID != grant.BootID {
		grant.LocalFenced = true
		return ErrBootIDChanged
	}
	if !grant.HWMUTC.IsZero() && currentWallUTC.Before(grant.HWMUTC) {
		return ErrClockRollback
	}
	if currentBoottimeMs < grant.GrantBoottimeMs {
		return ErrClockRollback
	}
	if currentBoottimeMs-grant.GrantBoottimeMs >= EffectiveBoottimeTTLMs() {
		return ErrOfflineSliceExpired
	}
	if !grant.SliceExpiresAtUTC.IsZero() && !currentWallUTC.Before(grant.SliceExpiresAtUTC) {
		return ErrOfflineSliceExpired
	}
	return nil
}

// CanRedeemGiftCard evaluates whether an offline gift-card redemption is permitted
// under the terminal's LocalSliceGrant. On approval, it updates spent_by_card_minor and hwm_utc.
func CanRedeemGiftCard(
	grant *LocalSliceGrant,
	cardID string,
	amountMinor int64,
	currentBootID string,
	currentBoottimeMs int64,
	currentWallUTC time.Time,
) error {
	if grant == nil {
		return ErrTerminalLocalFenced
	}
	if amountMinor <= 0 {
		return ErrInvalidAmount
	}
	if err := checkLocalTimeAndFence(grant, currentBootID, currentBoottimeMs, currentWallUTC); err != nil {
		return err
	}

	card, ok := grant.Cards[cardID]
	if !ok {
		return ErrCardNotFound
	}
	if card.Status != "ACTIVE" {
		return ErrCardNotActive
	}
	if len(card.EligibleStores) == 0 || len(card.EligibleStores) > MaxOfflineEligibleStores {
		return fmt.Errorf("%w: eligible_stores count %d outside [1, %d]", ErrOfflineStoreNotEligible, len(card.EligibleStores), MaxOfflineEligibleStores)
	}
	eligible := false
	for _, s := range card.EligibleStores {
		if s == grant.StoreID {
			eligible = true
			break
		}
	}
	if !eligible {
		return ErrOfflineStoreNotEligible
	}

	limit := grant.SliceMinor
	if card.BalanceMinor < limit {
		limit = card.BalanceMinor
	}
	if limit <= 0 {
		return ErrOfflineCapExceeded
	}

	if grant.SpentByCardMinor == nil {
		grant.SpentByCardMinor = make(map[string]int64)
	}
	spent := grant.SpentByCardMinor[cardID]
	if spent < 0 || spent > limit || amountMinor > limit-spent {
		return ErrOfflineCapExceeded
	}

	grant.SpentByCardMinor[cardID] = spent + amountMinor
	if currentWallUTC.After(grant.HWMUTC) {
		grant.HWMUTC = currentWallUTC
	}
	return nil
}

// CanBurnLoyalty evaluates whether an offline loyalty burn is permitted under
// the terminal's LocalSliceGrant. On approval, it updates total_loyalty_burned_points and hwm_utc.
func CanBurnLoyalty(
	grant *LocalSliceGrant,
	_ string,
	points int64,
	currentBootID string,
	currentBoottimeMs int64,
	currentWallUTC time.Time,
) error {
	if grant == nil {
		return ErrTerminalLocalFenced
	}
	if points <= 0 {
		return ErrInvalidAmount
	}
	if err := checkLocalTimeAndFence(grant, currentBootID, currentBoottimeMs, currentWallUTC); err != nil {
		return err
	}

	if grant.TotalLoyaltyBurnedPoints < 0 || points > math.MaxInt64-grant.TotalLoyaltyBurnedPoints {
		return ErrOfflineLoyaltyCapExceeded
	}
	if grant.LoyaltySlicePoints != nil {
		capPts := *grant.LoyaltySlicePoints
		if capPts < 0 || grant.TotalLoyaltyBurnedPoints > capPts || points > capPts-grant.TotalLoyaltyBurnedPoints {
			return ErrOfflineLoyaltyCapExceeded
		}
	}

	grant.TotalLoyaltyBurnedPoints += points
	if currentWallUTC.After(grant.HWMUTC) {
		grant.HWMUTC = currentWallUTC
	}
	return nil
}
