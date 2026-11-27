package giftcards

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/canonical"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/clock"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/days"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

type PrefixRedeemEvent struct {
	EventID     uuid.UUID
	StoreID     string
	AmountMinor int64
}

type OverspendFlag struct {
	CardID              string    `json:"card_id"`
	EventID             uuid.UUID `json:"event_id"`
	StoreID             string    `json:"store_id"`
	BalanceAfterMinor   int64     `json:"balance_after_minor"`
	OverspendDeltaMinor int64     `json:"overspend_delta_minor"`
}

// EvaluateGiftCardPrefix computes the deterministic gift-card balance, status, and
// overspend flags over all valid redemptions in canonical (occurred_at ASC, event_id ASC) order.
func EvaluateGiftCardPrefix(cardID string, initialBalance int64, adminFrozen bool, events []PrefixRedeemEvent) (int64, string, []OverspendFlag) {
	running := initialBalance
	var flags []OverspendFlag

	for _, ev := range events {
		running -= ev.AmountMinor
		if running < 0 {
			delta := ev.AmountMinor
			if -running < delta {
				delta = -running
			}
			flags = append(flags, OverspendFlag{
				CardID:              cardID,
				EventID:             ev.EventID,
				StoreID:             ev.StoreID,
				BalanceAfterMinor:   running,
				OverspendDeltaMinor: delta,
			})
		}
	}

	status := "ACTIVE"
	if adminFrozen {
		status = "ADMIN_FROZEN"
	} else if running < 0 {
		status = "OVERSPENT_FROZEN"
	}
	return running, status, flags
}

// ProjectGiftCardEvent projects a single giftcard_redeem event deterministically
// using prefix-scoped (applied_order <= e.AppliedOrder) canonical recomputation.
func ProjectGiftCardEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	if e.CardID == nil || e.AmountMinor == nil {
		return fmt.Errorf("giftcard_redeem %s missing card_id or amount_minor", e.EventID)
	}
	cardID := *e.CardID
	amt := *e.AmountMinor

	var initialBalance int64
	var currentStatus string
	var eligibleStores []string
	err := tx.QueryRow(ctx, `
		SELECT initial_balance_minor, status, eligible_stores
		FROM gift_cards
		WHERE card_id = $1
		FOR UPDATE`,
		cardID,
	).Scan(&initialBalance, &currentStatus, &eligibleStores)
	if errors.Is(err, pgx.ErrNoRows) {
		return upsertEventStatus(ctx, tx, e.EventID, "REJECTED_INVALID", ptrStr("CARD_NOT_FOUND"), 0, nil, nil, false)
	}
	if err != nil {
		return err
	}

	if e.Channel == "OFFLINE" {
		eligible := false
		for _, s := range eligibleStores {
			if s == e.StoreID {
				eligible = true
				break
			}
		}
		if !eligible {
			return upsertEventStatus(ctx, tx, e.EventID, "REJECTED_INVALID", ptrStr("OFFLINE_STORE_NOT_ELIGIBLE"), 0, nil, nil, false)
		}

		if e.EpochGen == nil {
			return upsertEventStatus(ctx, tx, e.EventID, "REJECTED_INVALID", ptrStr("MISSING_EPOCH_PROVENANCE"), 0, nil, nil, false)
		}

		var capMinor int64
		if err := tx.QueryRow(ctx, `SELECT giftcard_offline_store_cap_minor FROM ledger_config WHERE id = 1`).Scan(&capMinor); err != nil {
			return err
		}

		var priorEpochSpend int64
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(le.amount_minor), 0)
			FROM ledger_events le
			JOIN event_application_status eas ON eas.event_id = le.event_id
			WHERE le.event_type = 'giftcard_redeem'
			  AND le.channel = 'OFFLINE'
			  AND le.card_id = $1
			  AND le.store_id = $2
			  AND le.epoch_gen = $3
			  AND le.applied_order < $4
			  AND eas.status = 'APPLIED'`,
			cardID, e.StoreID, *e.EpochGen, e.AppliedOrder,
		).Scan(&priorEpochSpend)
		if err != nil {
			return err
		}
		if priorEpochSpend+amt > capMinor {
			return upsertEventStatus(ctx, tx, e.EventID, "REJECTED_INVALID", ptrStr("OFFLINE_CAP_EXCEEDED"), 0, nil, nil, false)
		}
	}

	loc, _, err := days.LoadStoreLocation(ctx, tx, e.StoreID)
	if err != nil {
		return err
	}
	postingDay, isAdj, err := days.ResolvePostingDay(ctx, tx, e.StoreID, loc, e.OriginBusinessDay, e.IngestedAt)
	if err != nil {
		return err
	}

	entryKind := "DIRECT"
	if isAdj {
		entryKind = "LATE_POSTING"
	}
	if err := days.ApplyTotalsDelta(ctx, tx, e.StoreID, postingDay, e.OriginBusinessDay, e.EventID, e.EventID, "giftcard_redeem", entryKind, 0, amt, isAdj); err != nil {
		return err
	}

	if err := upsertEventStatus(ctx, tx, e.EventID, "APPLIED", nil, amt, &e.StoreID, &postingDay, isAdj); err != nil {
		return err
	}

	adminFrozen := currentStatus == "ADMIN_FROZEN"
	_, _, err = recomputeGiftCardState(ctx, tx, cardID, e.AppliedOrder, initialBalance, adminFrozen)
	return err
}

func recomputeGiftCardState(
	ctx context.Context,
	tx pgx.Tx,
	cardID string,
	appliedOrder int64,
	initialBalance int64,
	adminFrozen bool,
) (int64, string, error) {
	rows, err := tx.Query(ctx, `
		SELECT le.event_id, le.store_id, le.amount_minor
		FROM ledger_events le
		JOIN event_application_status eas ON eas.event_id = le.event_id
		WHERE le.card_id = $1
		  AND le.applied_order <= $2
		  AND le.event_type = 'giftcard_redeem'
		  AND eas.status = 'APPLIED'
		ORDER BY le.occurred_at ASC, le.event_id ASC`,
		cardID, appliedOrder,
	)
	if err != nil {
		return 0, "", err
	}
	var prefix []PrefixRedeemEvent
	for rows.Next() {
		var ev PrefixRedeemEvent
		if err := rows.Scan(&ev.EventID, &ev.StoreID, &ev.AmountMinor); err != nil {
			rows.Close()
			return 0, "", err
		}
		prefix = append(prefix, ev)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, "", err
	}
	rows.Close()

	newBalance, newStatus, flags := EvaluateGiftCardPrefix(cardID, initialBalance, adminFrozen, prefix)

	if _, err := tx.Exec(ctx, `DELETE FROM giftcard_overspend_flags WHERE card_id = $1`, cardID); err != nil {
		return 0, "", err
	}
	for _, f := range flags {
		if _, err := tx.Exec(ctx, `
			INSERT INTO giftcard_overspend_flags (card_id, event_id, store_id, balance_after_minor, overspend_delta_minor)
			VALUES ($1, $2, $3, $4, $5)`,
			f.CardID, f.EventID, f.StoreID, f.BalanceAfterMinor, f.OverspendDeltaMinor,
		); err != nil {
			return 0, "", err
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE gift_cards
		SET balance_minor = $2, status = $3
		WHERE card_id = $1`,
		cardID, newBalance, newStatus,
	); err != nil {
		return 0, "", err
	}

	return newBalance, newStatus, nil
}

func upsertEventStatus(
	ctx context.Context,
	tx pgx.Tx,
	eventID uuid.UUID,
	status string,
	rejectReason *string,
	appliedAmt int64,
	appliedStore *string,
	appliedPostingDay *time.Time,
	isAdj bool,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_application_status (
			event_id, status, rejection_reason, applied_amount_minor,
			applied_store_id, applied_posting_day, applied_is_adjustment
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (event_id) DO UPDATE SET
			status                = EXCLUDED.status,
			rejection_reason      = EXCLUDED.rejection_reason,
			applied_amount_minor  = EXCLUDED.applied_amount_minor,
			applied_store_id      = EXCLUDED.applied_store_id,
			applied_posting_day   = EXCLUDED.applied_posting_day,
			applied_is_adjustment = EXCLUDED.applied_is_adjustment`,
		eventID, status, rejectReason, appliedAmt, appliedStore, appliedPostingDay, isAdj,
	)
	return err
}

func ptrStr(s string) *string { return &s }

type Handler struct {
	Cfg   *config.Config
	Pools *db.Pools
}

type OnlineRedeemRequest struct {
	EventID     string          `json:"event_id"`
	Channel     *string         `json:"channel,omitempty"`
	StoreID     string          `json:"store_id"`
	TerminalID  string          `json:"terminal_id"`
	OccurredAt  string          `json:"occurred_at"`
	CardID      string          `json:"card_id"`
	AmountMinor int64           `json:"amount_minor"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

func (h *Handler) OnlineRedeem(w http.ResponseWriter, r *http.Request) {
	ingestNow, _, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var req OnlineRedeemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}
	if req.Channel != nil {
		ledger.WriteError(w, http.StatusBadRequest, "CLIENT_CHANNEL_FORBIDDEN", "client-supplied channel is forbidden")
		return
	}
	eventID, err := uuid.Parse(req.EventID)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_EVENT_ID", "invalid event_id")
		return
	}
	if req.StoreID == "" || req.TerminalID == "" || req.CardID == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_REQUIRED_FIELDS", "store_id, terminal_id, and card_id are required")
		return
	}
	if req.AmountMinor <= 0 {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_AMOUNT", "amount_minor must be > 0")
		return
	}

	occ := ingestNow
	if req.OccurredAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, req.OccurredAt)
		if err != nil {
			ledger.WriteError(w, http.StatusBadRequest, "INVALID_TIMESTAMP", "invalid occurred_at")
			return
		}
		occ = parsed.UTC().Truncate(time.Microsecond)
	}
	if code, err := clock.ValidateTimestamps(occ, ingestNow, h.Cfg.MaxFutureSkewSeconds, h.Cfg.MaxEventLatenessHours); err != nil {
		ledger.WriteError(w, http.StatusBadRequest, code, err.Error())
		return
	}

	payloadObj := map[string]any{}
	if len(req.Payload) > 0 && string(req.Payload) != "null" {
		if err := json.Unmarshal(req.Payload, &payloadObj); err != nil {
			ledger.WriteError(w, http.StatusBadRequest, "INVALID_PAYLOAD", "payload must be a JSON object")
			return
		}
	}
	payloadObj["card_id"] = req.CardID
	payloadObj["amount_minor"] = req.AmountMinor
	canonPayload, err := canonical.CanonicalJSON(payloadObj)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_PAYLOAD", err.Error())
		return
	}
	payloadHash := canonical.PayloadHash("giftcard_redeem", req.StoreID, req.TerminalID, occ, canonPayload)

	var respStatus int
	var respBody json.RawMessage
	var abortStatus int
	var abortCode, abortMsg string

	txErr := db.WithTxRetry(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		abortStatus = 0

		// Step 1: Claim-first idempotency receipt
		var claimed uuid.UUID
		err := tx.QueryRow(r.Context(), `
			INSERT INTO online_action_receipts (event_id, action_type, payload_hash, status)
			VALUES ($1, 'giftcard_redeem', $2, 'PENDING')
			ON CONFLICT (event_id) DO NOTHING
			RETURNING event_id`,
			eventID, payloadHash,
		).Scan(&claimed)
		if errors.Is(err, pgx.ErrNoRows) {
			var existingHash []byte
			var status string
			var httpStatus *int
			var body []byte
			if err := tx.QueryRow(r.Context(), `
				SELECT payload_hash, status, http_status, response_body
				FROM online_action_receipts
				WHERE event_id = $1
				FOR UPDATE`,
				eventID,
			).Scan(&existingHash, &status, &httpStatus, &body); err != nil {
				return err
			}
			if !bytes.Equal(existingHash, payloadHash) {
				abortStatus = http.StatusConflict
				abortCode = "EVENT_ID_PAYLOAD_MISMATCH"
				abortMsg = fmt.Sprintf("event_id %s already exists with a different payload", eventID)
				return errors.New("abort")
			}
			if httpStatus != nil {
				respStatus = *httpStatus
			} else {
				respStatus = http.StatusOK
			}
			respBody = body
			return nil
		}
		if err != nil {
			return err
		}

		// Step 2: Canonical lock order: store_business_days -> gift_cards
		// Use a savepoint around ResolvePostingDay so declined online redeems never leave behind
		// an empty store_business_days / store_day_totals row that diverges on full rebuild.
		loc, _, err := days.LoadStoreLocation(r.Context(), tx, req.StoreID)
		if err != nil {
			abortStatus = http.StatusNotFound
			abortCode = "STORE_NOT_FOUND"
			abortMsg = err.Error()
			return errors.New("abort")
		}
		originDay := clock.BusinessDay(occ, loc)
		originCutoff := clock.CutoffUTC(originDay, loc)

		if _, err := tx.Exec(r.Context(), `SAVEPOINT sp_online_redeem_day`); err != nil {
			return err
		}
		postingDay, isAdj, err := days.ResolvePostingDay(r.Context(), tx, req.StoreID, loc, originDay, ingestNow)
		if err != nil {
			return err
		}

		var initialBalance, balanceMinor int64
		var cardStatus string
		err = tx.QueryRow(r.Context(), `
			SELECT initial_balance_minor, balance_minor, status
			FROM gift_cards
			WHERE card_id = $1
			FOR UPDATE`,
			req.CardID,
		).Scan(&initialBalance, &balanceMinor, &cardStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			if _, rbErr := tx.Exec(r.Context(), `ROLLBACK TO SAVEPOINT sp_online_redeem_day`); rbErr != nil {
				return rbErr
			}
			return finalizeDeclined(r.Context(), tx, eventID, http.StatusNotFound, "CARD_NOT_FOUND", "gift card not found", &respStatus, &respBody)
		}
		if err != nil {
			return err
		}

		if cardStatus != "ACTIVE" {
			if _, rbErr := tx.Exec(r.Context(), `ROLLBACK TO SAVEPOINT sp_online_redeem_day`); rbErr != nil {
				return rbErr
			}
			return finalizeDeclined(r.Context(), tx, eventID, http.StatusConflict, "CARD_FROZEN", fmt.Sprintf("gift card is %s", cardStatus), &respStatus, &respBody)
		}
		if balanceMinor < req.AmountMinor {
			if _, rbErr := tx.Exec(r.Context(), `ROLLBACK TO SAVEPOINT sp_online_redeem_day`); rbErr != nil {
				return rbErr
			}
			return finalizeDeclined(r.Context(), tx, eventID, http.StatusConflict, "INSUFFICIENT_FUNDS", "insufficient gift card balance", &respStatus, &respBody)
		}

		// Step 3: Attempt atomic insert into ledger_events with ON CONFLICT RETURNING
		var appliedOrder int64
		err = tx.QueryRow(r.Context(), `
			INSERT INTO ledger_events (
				event_id, applied_order, event_type, channel,
				store_id, terminal_id, occurred_at, ingested_at,
				origin_business_day, origin_cutoff_utc, tz_version,
				card_id, amount_minor, payload_json, payload_hash
			) VALUES (
				$1, nextval('ledger_applied_order_seq'), 'giftcard_redeem', 'ONLINE',
				$2, $3, $4, $5,
				$6, $7, $8,
				$9, $10, $11::jsonb, $12
			)
			ON CONFLICT (event_id) DO NOTHING
			RETURNING applied_order`,
			eventID, req.StoreID, req.TerminalID, occ, ingestNow,
			originDay, originCutoff, clock.TZVersion,
			req.CardID, req.AmountMinor, string(canonPayload), payloadHash,
		).Scan(&appliedOrder)

		// Step 4: Online-vs-batch race branch
		if errors.Is(err, pgx.ErrNoRows) {
			if _, rbErr := tx.Exec(r.Context(), `ROLLBACK TO SAVEPOINT sp_online_redeem_day`); rbErr != nil {
				return rbErr
			}
			var existingHash []byte
			if err := tx.QueryRow(r.Context(), `SELECT payload_hash FROM ledger_events WHERE event_id = $1`, eventID).Scan(&existingHash); err != nil {
				return err
			}
			if !bytes.Equal(existingHash, payloadHash) {
				abortStatus = http.StatusConflict
				abortCode = "EVENT_ID_PAYLOAD_MISMATCH"
				abortMsg = fmt.Sprintf("event_id %s already exists in ledger_events with a different payload", eventID)
				return errors.New("abort")
			}
			body, _ := canonical.CanonicalJSON(map[string]any{
				"event_id":      eventID.String(),
				"card_id":       req.CardID,
				"amount_minor":  req.AmountMinor,
				"balance_minor": balanceMinor,
				"status":        "APPROVED",
			})
			if _, err := tx.Exec(r.Context(), `
				UPDATE online_action_receipts
				SET status = 'APPROVED', http_status = 200, response_body = $2::jsonb
				WHERE event_id = $1`,
				eventID, string(body),
			); err != nil {
				return err
			}
			respStatus = http.StatusOK
			respBody = body
			return nil
		}
		if err != nil {
			return err
		}

		if _, err := tx.Exec(r.Context(), `RELEASE SAVEPOINT sp_online_redeem_day`); err != nil {
			return err
		}

		// Winner: apply projection mutations using the exact same prefix recomputation as rebuild
		if err := upsertEventStatus(r.Context(), tx, eventID, "APPLIED", nil, req.AmountMinor, &req.StoreID, &postingDay, isAdj); err != nil {
			return err
		}
		entryKind := "DIRECT"
		if isAdj {
			entryKind = "LATE_POSTING"
		}
		if err := days.ApplyTotalsDelta(r.Context(), tx, req.StoreID, postingDay, originDay, eventID, eventID, "giftcard_redeem", entryKind, 0, req.AmountMinor, isAdj); err != nil {
			return err
		}
		newBalance, _, err := recomputeGiftCardState(r.Context(), tx, req.CardID, appliedOrder, initialBalance, false)
		if err != nil {
			return err
		}

		body, _ := canonical.CanonicalJSON(map[string]any{
			"event_id":      eventID.String(),
			"card_id":       req.CardID,
			"amount_minor":  req.AmountMinor,
			"balance_minor": newBalance,
			"status":        "APPROVED",
		})
		if _, err := tx.Exec(r.Context(), `
			UPDATE online_action_receipts
			SET status = 'APPROVED', http_status = 200, response_body = $2::jsonb
			WHERE event_id = $1`,
			eventID, string(body),
		); err != nil {
			return err
		}
		respStatus = http.StatusOK
		respBody = body
		return nil
	})

	if txErr != nil {
		if abortStatus != 0 {
			ledger.WriteError(w, abortStatus, abortCode, abortMsg)
			return
		}
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", txErr.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(respStatus)
	_, _ = w.Write(respBody)
}

func finalizeDeclined(
	ctx context.Context,
	tx pgx.Tx,
	eventID uuid.UUID,
	httpStatus int,
	code, msg string,
	outStatus *int,
	outBody *json.RawMessage,
) error {
	body, _ := canonical.CanonicalJSON(map[string]any{
		"event_id": eventID.String(),
		"status":   "DECLINED",
		"error":    code,
		"message":  msg,
	})
	// Note: online_action_receipts.http_status stores 409 for declined actions per Phase 2 §3.4
	storedHTTPStatus := httpStatus
	if storedHTTPStatus == http.StatusNotFound {
		storedHTTPStatus = http.StatusConflict
	}
	if _, err := tx.Exec(ctx, `
		UPDATE online_action_receipts
		SET status = 'DECLINED', http_status = $2, response_body = $3::jsonb
		WHERE event_id = $1`,
		eventID, storedHTTPStatus, string(body),
	); err != nil {
		return err
	}
	*outStatus = storedHTTPStatus
	*outBody = body
	return nil
}

func (h *Handler) GetGiftCard(w http.ResponseWriter, r *http.Request) {
	cardID := r.PathValue("id")
	if cardID == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_CARD_ID", "card id is required")
		return
	}

	var initialBalance, balanceMinor int64
	var status string
	var eligibleStores []string
	flags := make([]OverspendFlag, 0)

	err := db.WithReadOnlyTx(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		err := tx.QueryRow(r.Context(), `
			SELECT initial_balance_minor, balance_minor, status, eligible_stores
			FROM gift_cards
			WHERE card_id = $1`,
			cardID,
		).Scan(&initialBalance, &balanceMinor, &status, &eligibleStores)
		if err != nil {
			return err
		}

		rows, err := tx.Query(r.Context(), `
			SELECT card_id, event_id, store_id, balance_after_minor, overspend_delta_minor
			FROM giftcard_overspend_flags
			WHERE card_id = $1
			ORDER BY event_id ASC`,
			cardID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var f OverspendFlag
			if err := rows.Scan(&f.CardID, &f.EventID, &f.StoreID, &f.BalanceAfterMinor, &f.OverspendDeltaMinor); err != nil {
				return err
			}
			flags = append(flags, f)
		}
		return rows.Err()
	})
	if errors.Is(err, pgx.ErrNoRows) {
		ledger.WriteError(w, http.StatusNotFound, "CARD_NOT_FOUND", "gift card not found")
		return
	}
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	ledger.WriteJSON(w, http.StatusOK, map[string]any{
		"card_id":               cardID,
		"initial_balance_minor": initialBalance,
		"balance_minor":         balanceMinor,
		"status":                status,
		"eligible_stores":       eligibleStores,
		"overspend_flags":       flags,
	})
}
