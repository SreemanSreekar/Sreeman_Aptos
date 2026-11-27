package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
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

type BatchRequest struct {
	BatchID    string            `json:"batch_id"`
	StoreID    string            `json:"store_id"`
	TerminalID string            `json:"terminal_id"`
	Events     []BatchEventInput `json:"events"`
}

type BatchEventInput struct {
	EventID          string          `json:"event_id"`
	EventType        string          `json:"event_type"`
	Channel          *string         `json:"channel,omitempty"` // Forbidden from client; always rejected if present
	StoreID          string          `json:"store_id"`
	TerminalID       string          `json:"terminal_id"`
	OccurredAt       string          `json:"occurred_at"`
	EpochGen         *int64          `json:"epoch_gen,omitempty"`
	TerminalEpochSeq *int32          `json:"terminal_epoch_seq,omitempty"`
	SaleID           *string         `json:"sale_id,omitempty"`
	ParentSaleID     *string         `json:"parent_sale_id,omitempty"`
	CardID           *string         `json:"card_id,omitempty"`
	AccountID        *string         `json:"account_id,omitempty"`
	AmountMinor      *int64          `json:"amount_minor,omitempty"`
	Points           *int64          `json:"points,omitempty"`
	Payload          json.RawMessage `json:"payload,omitempty"`
}

type BatchSummary struct {
	BatchID         string `json:"batch_id"`
	Accepted        int    `json:"accepted"`
	Deduplicated    int    `json:"deduplicated"`
	Parked          int    `json:"parked"`
	RejectedInvalid int    `json:"rejected_invalid"`
}

type HTTPError struct {
	Status  int
	Code    string
	Message string
}

func (e *HTTPError) Error() string { return e.Code + ": " + e.Message }

type preparedEvent struct {
	EventID          uuid.UUID
	EventType        string
	Phase            int
	StoreID          string
	TerminalID       string
	OccurredAt       time.Time
	EpochGen         *int64
	TerminalEpochSeq *int32
	SaleID           *uuid.UUID
	ParentSaleID     *uuid.UUID
	CardID           *string
	AccountID        *string
	AmountMinor      *int64
	Points           *int64
	PayloadJSON      []byte
	PayloadHash      []byte
}

type Handler struct {
	Cfg       *config.Config
	Pools     *db.Pools
	Projector ledger.Projector
}

func (h *Handler) IngestBatch(w http.ResponseWriter, r *http.Request) {
	ingestNow, _, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var req BatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	batchUUID, err := parseOrGenBatchID(req.BatchID)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_BATCH_ID", err.Error())
		return
	}

	prepared, inBatchDupes, httpErr := PrepareOfflineEvents(req, ingestNow, h.Cfg.MaxFutureSkewSeconds, h.Cfg.MaxEventLatenessHours)
	if httpErr != nil {
		ledger.WriteError(w, httpErr.Status, httpErr.Code, httpErr.Message)
		return
	}

	var summary BatchSummary
	txErr := db.WithTxRetry(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		s, err := IngestPreparedEventsInTx(r.Context(), tx, batchUUID, req.StoreID, req.TerminalID, prepared, inBatchDupes, ingestNow, h.Projector)
		if err != nil {
			return err
		}
		summary = s
		return nil
	})
	if txErr != nil {
		var he *HTTPError
		if errors.As(txErr, &he) {
			ledger.WriteError(w, he.Status, he.Code, he.Message)
			return
		}
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", txErr.Error())
		return
	}
	ledger.WriteJSON(w, http.StatusOK, summary)
}

func parseOrGenBatchID(s string) (uuid.UUID, error) {
	if s == "" {
		return uuid.New(), nil
	}
	return uuid.Parse(s)
}

func eventPhase(evType string) (int, bool) {
	switch evType {
	case "sale":
		return 1, true
	case "return", "void":
		return 2, true
	case "giftcard_redeem", "loyalty_earn", "loyalty_burn":
		return 3, true
	default:
		return 0, false
	}
}

// PrepareOfflineEvents validates, rejects client-supplied channel, computes canonical payload_hash,
// deduplicates within batch (detecting payload collisions), and topologically pre-sorts events.
func PrepareOfflineEvents(
	req BatchRequest,
	ingestNow time.Time,
	maxFutureSkewSec int,
	maxLatenessHours int,
) ([]preparedEvent, int, *HTTPError) {
	seen := make(map[uuid.UUID][]byte, len(req.Events))
	out := make([]preparedEvent, 0, len(req.Events))
	inBatchDupes := 0

	for idx, raw := range req.Events {
		if raw.Channel != nil {
			return nil, 0, &HTTPError{
				Status:  http.StatusBadRequest,
				Code:    "CLIENT_CHANNEL_FORBIDDEN",
				Message: fmt.Sprintf("event[%d]: client-supplied channel is forbidden", idx),
			}
		}
		phase, ok := eventPhase(raw.EventType)
		if !ok {
			return nil, 0, &HTTPError{
				Status:  http.StatusBadRequest,
				Code:    "INVALID_EVENT_TYPE",
				Message: fmt.Sprintf("event[%d]: unsupported event_type %q", idx, raw.EventType),
			}
		}
		evID, err := uuid.Parse(raw.EventID)
		if err != nil {
			return nil, 0, &HTTPError{
				Status:  http.StatusBadRequest,
				Code:    "INVALID_EVENT_ID",
				Message: fmt.Sprintf("event[%d]: invalid event_id", idx),
			}
		}
		storeID := raw.StoreID
		if storeID == "" {
			storeID = req.StoreID
		}
		termID := raw.TerminalID
		if termID == "" {
			termID = req.TerminalID
		}
		if storeID == "" || termID == "" {
			return nil, 0, &HTTPError{
				Status:  http.StatusBadRequest,
				Code:    "MISSING_STORE_OR_TERMINAL",
				Message: fmt.Sprintf("event[%d]: store_id and terminal_id are required", idx),
			}
		}
		occ, err := time.Parse(time.RFC3339Nano, raw.OccurredAt)
		if err != nil {
			return nil, 0, &HTTPError{
				Status:  http.StatusBadRequest,
				Code:    "INVALID_TIMESTAMP",
				Message: fmt.Sprintf("event[%d]: invalid occurred_at", idx),
			}
		}
		occ = occ.UTC().Truncate(time.Microsecond)
		if code, err := clock.ValidateTimestamps(occ, ingestNow, maxFutureSkewSec, maxLatenessHours); err != nil {
			return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: code, Message: err.Error()}
		}

		pe := preparedEvent{
			EventID:          evID,
			EventType:        raw.EventType,
			Phase:            phase,
			StoreID:          storeID,
			TerminalID:       termID,
			OccurredAt:       occ,
			EpochGen:         raw.EpochGen,
			TerminalEpochSeq: raw.TerminalEpochSeq,
		}

		// Per-type structural validation & normalized keys
		switch raw.EventType {
		case "sale":
			if raw.AmountMinor == nil || *raw.AmountMinor <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_AMOUNT", Message: "sale amount_minor must be > 0"}
			}
			saleUUID := evID
			if raw.SaleID != nil && *raw.SaleID != "" {
				parsed, err := uuid.Parse(*raw.SaleID)
				if err != nil {
					return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_SALE_ID", Message: "invalid sale_id"}
				}
				saleUUID = parsed
			}
			pe.SaleID = &saleUUID
			pe.AmountMinor = raw.AmountMinor

		case "return", "void":
			if raw.AmountMinor == nil || *raw.AmountMinor <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_AMOUNT", Message: raw.EventType + " amount_minor must be > 0"}
			}
			if raw.ParentSaleID == nil || *raw.ParentSaleID == "" {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "MISSING_PARENT_SALE_ID", Message: raw.EventType + " requires parent_sale_id"}
			}
			parentUUID, err := uuid.Parse(*raw.ParentSaleID)
			if err != nil {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_PARENT_SALE_ID", Message: "invalid parent_sale_id"}
			}
			// Normalize sale_id = parent_sale_id per Phase 2 v10
			pe.SaleID = &parentUUID
			pe.ParentSaleID = &parentUUID
			pe.AmountMinor = raw.AmountMinor

		case "giftcard_redeem":
			if raw.CardID == nil || *raw.CardID == "" {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "MISSING_CARD_ID", Message: "giftcard_redeem requires card_id"}
			}
			if raw.AmountMinor == nil || *raw.AmountMinor <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_AMOUNT", Message: "giftcard_redeem amount_minor must be > 0"}
			}
			if raw.EpochGen == nil || *raw.EpochGen <= 0 || raw.TerminalEpochSeq == nil || *raw.TerminalEpochSeq <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "MISSING_EPOCH_PROVENANCE", Message: "offline giftcard_redeem requires epoch_gen > 0 and terminal_epoch_seq > 0"}
			}
			pe.CardID = raw.CardID
			pe.AmountMinor = raw.AmountMinor

		case "loyalty_earn":
			if raw.AccountID == nil || *raw.AccountID == "" {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "MISSING_ACCOUNT_ID", Message: "loyalty_earn requires account_id"}
			}
			if raw.Points == nil || *raw.Points <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_AMOUNT", Message: "loyalty_earn points must be > 0"}
			}
			pe.AccountID = raw.AccountID
			pe.Points = raw.Points

		case "loyalty_burn":
			if raw.AccountID == nil || *raw.AccountID == "" {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "MISSING_ACCOUNT_ID", Message: "loyalty_burn requires account_id"}
			}
			if raw.Points == nil || *raw.Points <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_AMOUNT", Message: "loyalty_burn points must be > 0"}
			}
			if raw.EpochGen == nil || *raw.EpochGen <= 0 || raw.TerminalEpochSeq == nil || *raw.TerminalEpochSeq <= 0 {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "MISSING_EPOCH_PROVENANCE", Message: "offline loyalty_burn requires epoch_gen > 0 and terminal_epoch_seq > 0"}
			}
			pe.AccountID = raw.AccountID
			pe.Points = raw.Points
		}

		// Canonicalize payload JSON
		payloadObj := map[string]any{}
		if len(raw.Payload) > 0 && string(raw.Payload) != "null" {
			if err := json.Unmarshal(raw.Payload, &payloadObj); err != nil {
				return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_PAYLOAD", Message: "payload must be a JSON object"}
			}
		}
		if pe.SaleID != nil {
			payloadObj["sale_id"] = pe.SaleID.String()
		}
		if pe.ParentSaleID != nil {
			payloadObj["parent_sale_id"] = pe.ParentSaleID.String()
		}
		if pe.CardID != nil {
			payloadObj["card_id"] = *pe.CardID
		}
		if pe.AccountID != nil {
			payloadObj["account_id"] = *pe.AccountID
		}
		if pe.AmountMinor != nil {
			payloadObj["amount_minor"] = *pe.AmountMinor
		}
		if pe.Points != nil {
			payloadObj["points"] = *pe.Points
		}

		canonPayload, err := canonical.CanonicalJSON(payloadObj)
		if err != nil {
			return nil, 0, &HTTPError{Status: http.StatusBadRequest, Code: "INVALID_PAYLOAD", Message: err.Error()}
		}
		pe.PayloadJSON = canonPayload
		pe.PayloadHash = canonical.PayloadHash(pe.EventType, pe.StoreID, pe.TerminalID, pe.OccurredAt, canonPayload)

		if prevHash, exists := seen[pe.EventID]; exists {
			if !bytes.Equal(prevHash, pe.PayloadHash) {
				return nil, 0, &HTTPError{
					Status:  http.StatusConflict,
					Code:    "EVENT_ID_PAYLOAD_MISMATCH",
					Message: fmt.Sprintf("conflicting payload for duplicate event_id %s within batch", pe.EventID),
				}
			}
			inBatchDupes++
			continue
		}
		seen[pe.EventID] = pe.PayloadHash
		out = append(out, pe)
	}

	// Pre-sort into exact topological projector order BEFORE DB insert & applied_order allocation.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Phase != out[j].Phase {
			return out[i].Phase < out[j].Phase
		}
		if !out[i].OccurredAt.Equal(out[j].OccurredAt) {
			return out[i].OccurredAt.Before(out[j].OccurredAt)
		}
		return out[i].EventID.String() < out[j].EventID.String()
	})

	return out, inBatchDupes, nil
}

// IngestPreparedEventsInTx executes the full ACID batch ingestion protocol inside tx.
// Shared by POST /v1/batches (Person 1) and POST /v1/stores/{id}/terminals/{tid}/sync (Person 2).
func IngestPreparedEventsInTx(
	ctx context.Context,
	tx pgx.Tx,
	batchID uuid.UUID,
	batchStoreID string,
	batchTerminalID string,
	events []preparedEvent,
	inBatchDupes int,
	ingestNow time.Time,
	projector ledger.Projector,
) (BatchSummary, error) {
	summary := BatchSummary{
		BatchID:      batchID.String(),
		Deduplicated: inBatchDupes,
	}
	if len(events) == 0 {
		return summary, nil
	}

	// 0. Pre-check already-committed event_ids in ledger_events so idempotent retries are true no-ops
	//    and never create empty store_business_days / sales / loyalty_accounts rows on a new day.
	skipEvent := make(map[uuid.UUID]bool)
	allIDs := make([]uuid.UUID, len(events))
	for i, ev := range events {
		allIDs[i] = ev.EventID
	}
	existingHashes := make(map[uuid.UUID][]byte)
	preRows, err := tx.Query(ctx, `SELECT event_id, payload_hash FROM ledger_events WHERE event_id = ANY($1)`, allIDs)
	if err != nil {
		return summary, err
	}
	for preRows.Next() {
		var eid uuid.UUID
		var ph []byte
		if err := preRows.Scan(&eid, &ph); err != nil {
			preRows.Close()
			return summary, err
		}
		existingHashes[eid] = ph
	}
	if err := preRows.Err(); err != nil {
		preRows.Close()
		return summary, err
	}
	preRows.Close()

	activeCount := 0
	for _, ev := range events {
		if existingHash, ok := existingHashes[ev.EventID]; ok {
			if !bytes.Equal(existingHash, ev.PayloadHash) {
				return summary, &HTTPError{
					Status:  http.StatusConflict,
					Code:    "EVENT_ID_PAYLOAD_MISMATCH",
					Message: fmt.Sprintf("event_id %s already exists with a different payload", ev.EventID),
				}
			}
			skipEvent[ev.EventID] = true
			summary.Deduplicated++
			continue
		}
		activeCount++
	}
	if activeCount == 0 {
		// All events in this batch are confirmed idempotent duplicates -> true no-op.
		return summary, nil
	}

	// 1. Live-ingestion-only slice spendability gate for offline giftcard_redeem & loyalty_burn
	type sliceKey struct {
		storeID    string
		terminalID string
		epochGen   int64
	}
	checkedSlices := make(map[sliceKey]bool)
	for _, ev := range events {
		if skipEvent[ev.EventID] {
			continue
		}
		if ev.EventType == "giftcard_redeem" || ev.EventType == "loyalty_burn" {
			sk := sliceKey{ev.StoreID, ev.TerminalID, *ev.EpochGen}
			if !checkedSlices[sk] {
				var st string
				err := tx.QueryRow(ctx, `
					SELECT state FROM terminal_offline_slices
					WHERE store_id = $1 AND terminal_id = $2 AND epoch_gen = $3`,
					sk.storeID, sk.terminalID, sk.epochGen,
				).Scan(&st)
				if errors.Is(err, pgx.ErrNoRows) {
					return summary, &HTTPError{
						Status:  http.StatusBadRequest,
						Code:    "INVALID_OFFLINE_SLICE",
						Message: fmt.Sprintf("slice (%s, %s, epoch %d) does not exist", sk.storeID, sk.terminalID, sk.epochGen),
					}
				}
				if err != nil {
					return summary, err
				}
				if st == "RECONCILED" {
					return summary, &HTTPError{
						Status:  http.StatusConflict,
						Code:    "SLICE_ALREADY_RECONCILED",
						Message: fmt.Sprintf("slice (%s, %s, epoch %d) is already RECONCILED", sk.storeID, sk.terminalID, sk.epochGen),
					}
				}
				checkedSlices[sk] = true
			}
		}
	}

	// 2. Claim-first online_action_receipts (PENDING) for giftcard_redeem & loyalty_burn in ascending event_id order
	claimedReceipts := make(map[uuid.UUID]bool)
	receiptCandidates := make([]preparedEvent, 0)
	for _, ev := range events {
		if skipEvent[ev.EventID] {
			continue
		}
		if ev.EventType == "giftcard_redeem" || ev.EventType == "loyalty_burn" {
			receiptCandidates = append(receiptCandidates, ev)
		}
	}
	sort.Slice(receiptCandidates, func(i, j int) bool {
		return receiptCandidates[i].EventID.String() < receiptCandidates[j].EventID.String()
	})
	for _, ev := range receiptCandidates {
		var insertedID uuid.UUID
		err := tx.QueryRow(ctx, `
			INSERT INTO online_action_receipts (event_id, action_type, payload_hash, status)
			VALUES ($1, $2, $3, 'PENDING')
			ON CONFLICT (event_id) DO NOTHING
			RETURNING event_id`,
			ev.EventID, ev.EventType, ev.PayloadHash,
		).Scan(&insertedID)
		if errors.Is(err, pgx.ErrNoRows) {
			// Conflict with committed OR uncommitted PENDING online tx -> block on FOR UPDATE
			var existingHash []byte
			var existingStatus string
			if err := tx.QueryRow(ctx, `
				SELECT payload_hash, status
				FROM online_action_receipts
				WHERE event_id = $1
				FOR UPDATE`,
				ev.EventID,
			).Scan(&existingHash, &existingStatus); err != nil {
				return summary, err
			}
			if !bytes.Equal(existingHash, ev.PayloadHash) {
				return summary, &HTTPError{
					Status:  http.StatusConflict,
					Code:    "EVENT_ID_PAYLOAD_MISMATCH",
					Message: fmt.Sprintf("event_id %s payload mismatches existing online_action_receipts", ev.EventID),
				}
			}
			skipEvent[ev.EventID] = true
			summary.Deduplicated++
			continue
		}
		if err != nil {
			return summary, err
		}
		claimedReceipts[ev.EventID] = true
	}

	// 3. Canonical entity lock acquisition in global table order:
	//    sales -> (recheck ledger_events!) -> store_business_days -> gift_cards -> loyalty_accounts
	saleIDSet := make(map[uuid.UUID]struct{})
	for _, ev := range events {
		if skipEvent[ev.EventID] {
			continue
		}
		if ev.SaleID != nil {
			saleIDSet[*ev.SaleID] = struct{}{}
		}
	}

	sortedSales := make([]uuid.UUID, 0, len(saleIDSet))
	for id := range saleIDSet {
		sortedSales = append(sortedSales, id)
	}
	sort.Slice(sortedSales, func(i, j int) bool { return sortedSales[i].String() < sortedSales[j].String() })
	for _, sid := range sortedSales {
		if _, err := tx.Exec(ctx, `INSERT INTO sales (sale_id, status) VALUES ($1, 'PLACEHOLDER') ON CONFLICT (sale_id) DO NOTHING`, sid); err != nil {
			return summary, err
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM sales WHERE sale_id = $1 FOR UPDATE`, sid); err != nil {
			return summary, err
		}
	}

	// Recheck ledger_events AFTER acquiring the serializing sales lock (and Step 2's online_action_receipts lock)
	// and BEFORE creating/locking store_business_days, so a losing concurrent duplicate arriving on a different
	// simulated day never creates an otherwise-empty store_business_days row.
	if len(sortedSales) > 0 {
		remIDs := make([]uuid.UUID, 0, len(events))
		for _, ev := range events {
			if !skipEvent[ev.EventID] {
				remIDs = append(remIDs, ev.EventID)
			}
		}
		if len(remIDs) > 0 {
			reRows, err := tx.Query(ctx, `SELECT event_id, payload_hash FROM ledger_events WHERE event_id = ANY($1)`, remIDs)
			if err != nil {
				return summary, err
			}
			reHashes := make(map[uuid.UUID][]byte)
			for reRows.Next() {
				var eid uuid.UUID
				var ph []byte
				if err := reRows.Scan(&eid, &ph); err != nil {
					reRows.Close()
					return summary, err
				}
				reHashes[eid] = ph
			}
			if err := reRows.Err(); err != nil {
				reRows.Close()
				return summary, err
			}
			reRows.Close()

			for _, ev := range events {
				if skipEvent[ev.EventID] {
					continue
				}
				if existingHash, ok := reHashes[ev.EventID]; ok {
					if !bytes.Equal(existingHash, ev.PayloadHash) {
						return summary, &HTTPError{
							Status:  http.StatusConflict,
							Code:    "EVENT_ID_PAYLOAD_MISMATCH",
							Message: fmt.Sprintf("event_id %s already exists with a different payload", ev.EventID),
						}
					}
					skipEvent[ev.EventID] = true
					summary.Deduplicated++
				}
			}
		}
	}

	// Build remaining day, card, and loyalty lock sets ONLY for confirmed non-duplicate events.
	cardIDSet := make(map[string]struct{})
	acctIDSet := make(map[string]struct{})
	locCache := make(map[string]*time.Location)
	type storeDayKey struct {
		storeID string
		day     time.Time
		dayStr  string
	}
	dayKeyMap := make(map[string]storeDayKey)
	remainingActive := 0

	for _, ev := range events {
		if skipEvent[ev.EventID] {
			continue
		}
		remainingActive++
		if ev.CardID != nil {
			cardIDSet[*ev.CardID] = struct{}{}
		}
		if ev.AccountID != nil {
			acctIDSet[*ev.AccountID] = struct{}{}
		}
		loc, ok := locCache[ev.StoreID]
		if !ok {
			var err error
			loc, _, err = days.LoadStoreLocation(ctx, tx, ev.StoreID)
			if err != nil {
				return summary, &HTTPError{Status: http.StatusNotFound, Code: "STORE_NOT_FOUND", Message: err.Error()}
			}
			locCache[ev.StoreID] = loc
		}
		// Only financial/tender events touch store_business_days / store_day_totals.
		if ev.EventType == "sale" || ev.EventType == "return" || ev.EventType == "void" || ev.EventType == "giftcard_redeem" {
			origDay := clock.BusinessDay(ev.OccurredAt, loc)
			origStr := origDay.Format("2006-01-02")
			dayKeyMap[ev.StoreID+"|"+origStr] = storeDayKey{storeID: ev.StoreID, day: origDay, dayStr: origStr}

			currDay := clock.BusinessDay(ingestNow, loc)
			currStr := currDay.Format("2006-01-02")
			dayKeyMap[ev.StoreID+"|"+currStr] = storeDayKey{storeID: ev.StoreID, day: currDay, dayStr: currStr}
		}
	}
	if remainingActive == 0 {
		return summary, nil
	}

	// Pre-lock already-existing store_business_days rows in sorted (store_id, business_day) order
	// before gift_cards/loyalty_accounts. New OPEN rows are created lazily by ResolvePostingDay
	// only when an event is actually APPLIED, so REJECTED_INVALID/PARKED events and unused
	// current-day pre-locks never create empty day rows that diverge on full rebuild.
	sortedDays := make([]storeDayKey, 0, len(dayKeyMap))
	for _, dk := range dayKeyMap {
		sortedDays = append(sortedDays, dk)
	}
	sort.Slice(sortedDays, func(i, j int) bool {
		if sortedDays[i].storeID != sortedDays[j].storeID {
			return sortedDays[i].storeID < sortedDays[j].storeID
		}
		return sortedDays[i].dayStr < sortedDays[j].dayStr
	})
	for _, dk := range sortedDays {
		var locked int
		err := tx.QueryRow(ctx, `
			SELECT 1
			FROM store_business_days
			WHERE store_id = $1 AND business_day = $2
			FOR UPDATE`,
			dk.storeID, dk.day,
		).Scan(&locked)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return summary, err
		}
	}

	sortedCards := make([]string, 0, len(cardIDSet))
	for id := range cardIDSet {
		sortedCards = append(sortedCards, id)
	}
	sort.Strings(sortedCards)
	for _, cid := range sortedCards {
		var exists int
		err := tx.QueryRow(ctx, `SELECT 1 FROM gift_cards WHERE card_id = $1 FOR UPDATE`, cid).Scan(&exists)
		if errors.Is(err, pgx.ErrNoRows) {
			return summary, &HTTPError{Status: http.StatusNotFound, Code: "GIFT_CARD_NOT_FOUND", Message: "gift card not found: " + cid}
		}
		if err != nil {
			return summary, err
		}
	}

	sortedAccts := make([]string, 0, len(acctIDSet))
	for id := range acctIDSet {
		sortedAccts = append(sortedAccts, id)
	}
	sort.Strings(sortedAccts)
	for _, aid := range sortedAccts {
		if _, err := tx.Exec(ctx, `
			INSERT INTO loyalty_accounts (account_id, earned_total, burned_total, balance, status)
			VALUES ($1, 0, 0, 0, 'ACTIVE')
			ON CONFLICT (account_id) DO NOTHING`, aid); err != nil {
			return summary, err
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM loyalty_accounts WHERE account_id = $1 FOR UPDATE`, aid); err != nil {
			return summary, err
		}
	}

	// 4. Insert pre-sorted events into ledger_events with post-lock applied_order
	insertedRecords := make([]ledger.EventRecord, 0, len(events))

	for _, ev := range events {
		if skipEvent[ev.EventID] {
			continue
		}
		loc := locCache[ev.StoreID]
		origDay := clock.BusinessDay(ev.OccurredAt, loc)
		origCutoff := clock.CutoffUTC(origDay, loc)

		var appliedOrder int64
		err := tx.QueryRow(ctx, `
			INSERT INTO ledger_events (
				event_id, applied_order, event_type, channel, epoch_gen, terminal_epoch_seq,
				store_id, terminal_id, occurred_at, ingested_at,
				origin_business_day, origin_cutoff_utc, tz_version,
				sale_id, parent_sale_id, card_id, account_id,
				amount_minor, points, payload_json, payload_hash
			) VALUES (
				$1, nextval('ledger_applied_order_seq'), $2, 'OFFLINE', $3, $4,
				$5, $6, $7, $8,
				$9, $10, $11,
				$12, $13, $14, $15,
				$16, $17, $18::jsonb, $19
			)
			ON CONFLICT (event_id) DO NOTHING
			RETURNING applied_order`,
			ev.EventID, ev.EventType, ev.EpochGen, ev.TerminalEpochSeq,
			ev.StoreID, ev.TerminalID, ev.OccurredAt, ingestNow,
			origDay, origCutoff, clock.TZVersion,
			ev.SaleID, ev.ParentSaleID, ev.CardID, ev.AccountID,
			ev.AmountMinor, ev.Points, string(ev.PayloadJSON), ev.PayloadHash,
		).Scan(&appliedOrder)

		if errors.Is(err, pgx.ErrNoRows) {
			var existingHash []byte
			if err := tx.QueryRow(ctx, `SELECT payload_hash FROM ledger_events WHERE event_id = $1`, ev.EventID).Scan(&existingHash); err != nil {
				return summary, err
			}
			if !bytes.Equal(existingHash, ev.PayloadHash) {
				return summary, &HTTPError{
					Status:  http.StatusConflict,
					Code:    "EVENT_ID_PAYLOAD_MISMATCH",
					Message: fmt.Sprintf("event_id %s already exists with a different payload", ev.EventID),
				}
			}
			summary.Deduplicated++
			continue
		}
		if err != nil {
			return summary, err
		}

		insertedRecords = append(insertedRecords, ledger.EventRecord{
			EventID:           ev.EventID,
			AppliedOrder:      appliedOrder,
			EventType:         ev.EventType,
			Channel:           "OFFLINE",
			EpochGen:          ev.EpochGen,
			TerminalEpochSeq:  ev.TerminalEpochSeq,
			StoreID:           ev.StoreID,
			TerminalID:        ev.TerminalID,
			OccurredAt:        ev.OccurredAt,
			IngestedAt:        ingestNow,
			OriginBusinessDay: origDay,
			OriginCutoffUTC:   origCutoff,
			TZVersion:         clock.TZVersion,
			SaleID:            ev.SaleID,
			ParentSaleID:      ev.ParentSaleID,
			CardID:            ev.CardID,
			AccountID:         ev.AccountID,
			AmountMinor:       ev.AmountMinor,
			Points:            ev.Points,
			PayloadJSON:       ev.PayloadJSON,
			PayloadHash:       ev.PayloadHash,
		})
	}

	// 5. Project newly inserted events event-by-event in exact applied_order ASC
	for _, rec := range insertedRecords {
		var projErr error
		switch rec.EventType {
		case "sale", "return", "void":
			projErr = projector.ProjectSaleEvent(ctx, tx, rec)
		case "giftcard_redeem":
			projErr = projector.ProjectGiftCardEvent(ctx, tx, rec)
		case "loyalty_earn", "loyalty_burn":
			projErr = projector.ProjectLoyaltyEvent(ctx, tx, rec)
		}
		if projErr != nil {
			return summary, projErr
		}

		// Read resulting application status for summary counts & receipt finalization
		var appStatus string
		var rejectReason *string
		err := tx.QueryRow(ctx, `SELECT status, rejection_reason FROM event_application_status WHERE event_id = $1`, rec.EventID).Scan(&appStatus, &rejectReason)
		if errors.Is(err, pgx.ErrNoRows) {
			appStatus = "APPLIED"
		} else if err != nil {
			return summary, err
		}

		switch appStatus {
		case "APPLIED":
			summary.Accepted++
		case "PARKED":
			summary.Parked++
		case "REJECTED_INVALID":
			summary.RejectedInvalid++
		}

		// Finalize batch-claimed online_action_receipts AFTER projectEvent completes
		if claimedReceipts[rec.EventID] {
			if appStatus == "REJECTED_INVALID" {
				reason := "REJECTED_INVALID"
				if rejectReason != nil {
					reason = *rejectReason
				}
				body, _ := canonical.CanonicalJSON(map[string]any{
					"event_id": rec.EventID.String(),
					"status":   "DECLINED",
					"error":    reason,
				})
				if _, err := tx.Exec(ctx, `
					UPDATE online_action_receipts
					SET status = 'DECLINED', http_status = 409, response_body = $2::jsonb
					WHERE event_id = $1`,
					rec.EventID, string(body),
				); err != nil {
					return summary, err
				}
			} else {
				body, _ := canonical.CanonicalJSON(map[string]any{
					"event_id": rec.EventID.String(),
					"status":   "APPROVED",
				})
				if _, err := tx.Exec(ctx, `
					UPDATE online_action_receipts
					SET status = 'APPROVED', http_status = 200, response_body = $2::jsonb
					WHERE event_id = $1`,
					rec.EventID, string(body),
				); err != nil {
					return summary, err
				}
			}
		}
	}

	// 6. Record transport-level batch audit receipt
	if batchStoreID == "" {
		batchStoreID = events[0].StoreID
	}
	if batchTerminalID == "" {
		batchTerminalID = events[0].TerminalID
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO ingest_batches (batch_id, store_id, terminal_id, event_count, accepted_count, duplicate_count, parked_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (batch_id) DO NOTHING`,
		batchID, batchStoreID, batchTerminalID,
		len(events)+inBatchDupes, summary.Accepted, summary.Deduplicated, summary.Parked,
	); err != nil {
		return summary, err
	}

	return summary, nil
}
