package loyalty

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

type PrefixLoyaltyEvent struct {
	EventID   uuid.UUID
	EventType string
	Points    int64
}

type NegativeFlag struct {
	AccountID         string     `json:"account_id"`
	TriggerEventID    uuid.UUID  `json:"trigger_event_id"`
	DeficitPoints     int64      `json:"deficit_points"`
	ResolvedByEventID *uuid.UUID `json:"resolved_by_event_id"`
}

// EvaluateLoyaltyPrefix computes the convergent PN-Counter CRDT state and negative flags
// over accepted loyalty events in canonical (occurred_at ASC, event_id ASC) order.
func EvaluateLoyaltyPrefix(accountID string, events []PrefixLoyaltyEvent) (earnedTotal, burnedTotal, balance int64, status string, flags []NegativeFlag) {
	var running int64
	for _, ev := range events {
		switch ev.EventType {
		case "loyalty_earn":
			earnedTotal += ev.Points
			running += ev.Points
			if running >= 0 {
				resolveID := ev.EventID
				for i := range flags {
					if flags[i].ResolvedByEventID == nil {
						flags[i].ResolvedByEventID = &resolveID
					}
				}
			}
		case "loyalty_burn":
			burnedTotal += ev.Points
			running -= ev.Points
			if running < 0 {
				flags = append(flags, NegativeFlag{
					AccountID:      accountID,
					TriggerEventID: ev.EventID,
					DeficitPoints:  -running,
				})
			}
		}
	}
	balance = earnedTotal - burnedTotal
	if balance >= 0 {
		status = "ACTIVE"
	} else {
		status = "NEGATIVE_RESTRICTED"
	}
	return earnedTotal, burnedTotal, balance, status, flags
}

// ExceedsOfflineLoyaltyCap performs an overflow-safe int64 check for priorBurn + points > capPoints.
func ExceedsOfflineLoyaltyCap(priorBurn, points, capPoints int64) bool {
	if points <= 0 || capPoints < 0 || priorBurn < 0 || priorBurn > capPoints {
		return true
	}
	return points > capPoints-priorBurn
}

// ProjectLoyaltyEvent projects a single loyalty_earn or loyalty_burn event
// deterministically using prefix-scoped (applied_order <= e.AppliedOrder) recomputation.
func ProjectLoyaltyEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	if e.AccountID == nil || e.Points == nil {
		return fmt.Errorf("loyalty event %s missing account_id or points", e.EventID)
	}
	accountID := *e.AccountID
	pts := *e.Points

	if _, err := tx.Exec(ctx, `
		INSERT INTO loyalty_accounts (account_id, earned_total, burned_total, balance, status)
		VALUES ($1, 0, 0, 0, 'ACTIVE')
		ON CONFLICT (account_id) DO NOTHING`,
		accountID,
	); err != nil {
		return err
	}
	var locked string
	if err := tx.QueryRow(ctx, `SELECT account_id FROM loyalty_accounts WHERE account_id = $1 FOR UPDATE`, accountID).Scan(&locked); err != nil {
		return err
	}

	if e.EventType == "loyalty_burn" && e.Channel == "OFFLINE" {
		if e.EpochGen == nil {
			return upsertEventStatus(ctx, tx, e.EventID, "REJECTED_INVALID", ptrStr("MISSING_EPOCH_PROVENANCE"), 0, nil, nil)
		}
		var loyaltyCap int64
		if err := tx.QueryRow(ctx, `SELECT loyalty_offline_store_burn_cap FROM ledger_config WHERE id = 1`).Scan(&loyaltyCap); err != nil {
			return err
		}
		if loyaltyCap > 0 {
			var priorBurn int64
			if err := tx.QueryRow(ctx, `
				SELECT COALESCE(SUM(le.points), 0)
				FROM ledger_events le
				JOIN event_application_status eas ON eas.event_id = le.event_id
				WHERE le.event_type = 'loyalty_burn'
				  AND le.channel = 'OFFLINE'
				  AND le.store_id = $1
				  AND le.epoch_gen = $2
				  AND le.applied_order < $3
				  AND eas.status = 'APPLIED'`,
				e.StoreID, *e.EpochGen, e.AppliedOrder,
			).Scan(&priorBurn); err != nil {
				return err
			}
			if ExceedsOfflineLoyaltyCap(priorBurn, pts, loyaltyCap) {
				return upsertEventStatus(ctx, tx, e.EventID, "REJECTED_INVALID", ptrStr("OFFLINE_LOYALTY_CAP_EXCEEDED"), 0, nil, nil)
			}
		}
	}

	if err := upsertEventStatus(ctx, tx, e.EventID, "APPLIED", nil, pts, &e.StoreID, &e.OriginBusinessDay); err != nil {
		return err
	}

	_, err := recomputeLoyaltyState(ctx, tx, accountID, e.AppliedOrder)
	return err
}

func recomputeLoyaltyState(
	ctx context.Context,
	tx pgx.Tx,
	accountID string,
	appliedOrder int64,
) (int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT le.event_id, le.event_type, le.points
		FROM ledger_events le
		JOIN event_application_status eas ON eas.event_id = le.event_id
		WHERE le.account_id = $1
		  AND le.applied_order <= $2
		  AND le.event_type IN ('loyalty_earn', 'loyalty_burn')
		  AND eas.status = 'APPLIED'
		ORDER BY le.occurred_at ASC, le.event_id ASC`,
		accountID, appliedOrder,
	)
	if err != nil {
		return 0, err
	}
	var prefix []PrefixLoyaltyEvent
	for rows.Next() {
		var ev PrefixLoyaltyEvent
		if err := rows.Scan(&ev.EventID, &ev.EventType, &ev.Points); err != nil {
			rows.Close()
			return 0, err
		}
		prefix = append(prefix, ev)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	earned, burned, balance, status, flags := EvaluateLoyaltyPrefix(accountID, prefix)

	if _, err := tx.Exec(ctx, `DELETE FROM loyalty_negative_flags WHERE account_id = $1`, accountID); err != nil {
		return 0, err
	}
	for _, f := range flags {
		if _, err := tx.Exec(ctx, `
			INSERT INTO loyalty_negative_flags (account_id, trigger_event_id, deficit_points, resolved_by_event_id)
			VALUES ($1, $2, $3, $4)`,
			f.AccountID, f.TriggerEventID, f.DeficitPoints, f.ResolvedByEventID,
		); err != nil {
			return 0, err
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE loyalty_accounts
		SET earned_total = $2,
		    burned_total = $3,
		    balance      = $4,
		    status       = $5
		WHERE account_id = $1`,
		accountID, earned, burned, balance, status,
	); err != nil {
		return 0, err
	}

	return balance, nil
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
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_application_status (
			event_id, status, rejection_reason, applied_amount_minor,
			applied_store_id, applied_posting_day, applied_is_adjustment
		) VALUES ($1, $2, $3, $4, $5, $6, false)
		ON CONFLICT (event_id) DO UPDATE SET
			status                = EXCLUDED.status,
			rejection_reason      = EXCLUDED.rejection_reason,
			applied_amount_minor  = EXCLUDED.applied_amount_minor,
			applied_store_id      = EXCLUDED.applied_store_id,
			applied_posting_day   = EXCLUDED.applied_posting_day,
			applied_is_adjustment = false`,
		eventID, status, rejectReason, appliedAmt, appliedStore, appliedPostingDay,
	)
	return err
}

func ptrStr(s string) *string { return &s }

type Handler struct {
	Cfg   *config.Config
	Pools *db.Pools
}

type OnlineBurnRequest struct {
	EventID    string          `json:"event_id"`
	Channel    *string         `json:"channel,omitempty"`
	StoreID    string          `json:"store_id"`
	TerminalID string          `json:"terminal_id"`
	OccurredAt string          `json:"occurred_at"`
	AccountID  string          `json:"account_id"`
	Points     int64           `json:"points"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

func (h *Handler) OnlineBurn(w http.ResponseWriter, r *http.Request) {
	ingestNow, _, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var req OnlineBurnRequest
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
	if req.StoreID == "" || req.TerminalID == "" || req.AccountID == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_REQUIRED_FIELDS", "store_id, terminal_id, and account_id are required")
		return
	}
	if req.Points <= 0 {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_AMOUNT", "points must be > 0")
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
	payloadObj["account_id"] = req.AccountID
	payloadObj["points"] = req.Points
	canonPayload, err := canonical.CanonicalJSON(payloadObj)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_PAYLOAD", err.Error())
		return
	}
	payloadHash := canonical.PayloadHash("loyalty_burn", req.StoreID, req.TerminalID, occ, canonPayload)

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
			VALUES ($1, 'loyalty_burn', $2, 'PENDING')
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

		// Step 2: Validate store timezone & lock loyalty_accounts (loyalty does not touch store_business_days)
		loc, _, err := days.LoadStoreLocation(r.Context(), tx, req.StoreID)
		if err != nil {
			abortStatus = http.StatusNotFound
			abortCode = "STORE_NOT_FOUND"
			abortMsg = err.Error()
			return errors.New("abort")
		}
		originDay := clock.BusinessDay(occ, loc)
		originCutoff := clock.CutoffUTC(originDay, loc)

		var earnedTotal, burnedTotal, balance int64
		var accStatus string
		err = tx.QueryRow(r.Context(), `
			SELECT earned_total, burned_total, balance, status
			FROM loyalty_accounts
			WHERE account_id = $1
			FOR UPDATE`,
			req.AccountID,
		).Scan(&earnedTotal, &burnedTotal, &balance, &accStatus)
		if errors.Is(err, pgx.ErrNoRows) {
			return finalizeDeclined(r.Context(), tx, eventID, http.StatusConflict, "INSUFFICIENT_POINTS", "loyalty account not found or zero balance", &respStatus, &respBody)
		}
		if err != nil {
			return err
		}

		if accStatus != "ACTIVE" {
			return finalizeDeclined(r.Context(), tx, eventID, http.StatusConflict, "ACCOUNT_NEGATIVE_RESTRICTED", "loyalty account is NEGATIVE_RESTRICTED", &respStatus, &respBody)
		}
		if balance < req.Points {
			return finalizeDeclined(r.Context(), tx, eventID, http.StatusConflict, "INSUFFICIENT_POINTS", "insufficient loyalty points", &respStatus, &respBody)
		}

		// Step 3: Attempt atomic insert into ledger_events
		var appliedOrder int64
		err = tx.QueryRow(r.Context(), `
			INSERT INTO ledger_events (
				event_id, applied_order, event_type, channel,
				store_id, terminal_id, occurred_at, ingested_at,
				origin_business_day, origin_cutoff_utc, tz_version,
				account_id, points, payload_json, payload_hash
			) VALUES (
				$1, nextval('ledger_applied_order_seq'), 'loyalty_burn', 'ONLINE',
				$2, $3, $4, $5,
				$6, $7, $8,
				$9, $10, $11::jsonb, $12
			)
			ON CONFLICT (event_id) DO NOTHING
			RETURNING applied_order`,
			eventID, req.StoreID, req.TerminalID, occ, ingestNow,
			originDay, originCutoff, clock.TZVersion,
			req.AccountID, req.Points, string(canonPayload), payloadHash,
		).Scan(&appliedOrder)

		// Step 4: Online-vs-batch race branch
		if errors.Is(err, pgx.ErrNoRows) {
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
				"event_id":   eventID.String(),
				"account_id": req.AccountID,
				"points":     req.Points,
				"balance":    balance,
				"status":     "APPROVED",
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

		// Winner: apply projection mutations using prefix recomputation so loyalty_negative_flags match rebuild
		if err := upsertEventStatus(r.Context(), tx, eventID, "APPLIED", nil, req.Points, &req.StoreID, &originDay); err != nil {
			return err
		}
		newBalance, err := recomputeLoyaltyState(r.Context(), tx, req.AccountID, appliedOrder)
		if err != nil {
			return err
		}

		body, _ := canonical.CanonicalJSON(map[string]any{
			"event_id":   eventID.String(),
			"account_id": req.AccountID,
			"points":     req.Points,
			"balance":    newBalance,
			"status":     "APPROVED",
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
	if _, err := tx.Exec(ctx, `
		UPDATE online_action_receipts
		SET status = 'DECLINED', http_status = $2, response_body = $3::jsonb
		WHERE event_id = $1`,
		eventID, httpStatus, string(body),
	); err != nil {
		return err
	}
	*outStatus = httpStatus
	*outBody = body
	return nil
}

func (h *Handler) GetLoyalty(w http.ResponseWriter, r *http.Request) {
	accountID := r.PathValue("id")
	if accountID == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_ACCOUNT_ID", "account id is required")
		return
	}

	var earnedTotal, burnedTotal, balance int64
	var status string
	flags := make([]NegativeFlag, 0)

	err := db.WithReadOnlyTx(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		err := tx.QueryRow(r.Context(), `
			SELECT earned_total, burned_total, balance, status
			FROM loyalty_accounts
			WHERE account_id = $1`,
			accountID,
		).Scan(&earnedTotal, &burnedTotal, &balance, &status)
		if err != nil {
			return err
		}

		rows, err := tx.Query(r.Context(), `
			SELECT account_id, trigger_event_id, deficit_points, resolved_by_event_id
			FROM loyalty_negative_flags
			WHERE account_id = $1
			ORDER BY trigger_event_id ASC`,
			accountID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var f NegativeFlag
			if err := rows.Scan(&f.AccountID, &f.TriggerEventID, &f.DeficitPoints, &f.ResolvedByEventID); err != nil {
				return err
			}
			flags = append(flags, f)
		}
		return rows.Err()
	})
	if errors.Is(err, pgx.ErrNoRows) {
		ledger.WriteError(w, http.StatusNotFound, "ACCOUNT_NOT_FOUND", "loyalty account not found")
		return
	}
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	ledger.WriteJSON(w, http.StatusOK, map[string]any{
		"account_id":     accountID,
		"earned_total":   earnedTotal,
		"burned_total":   burnedTotal,
		"balance":        balance,
		"status":         status,
		"negative_flags": flags,
	})
}
