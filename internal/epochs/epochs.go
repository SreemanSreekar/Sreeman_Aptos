package epochs

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/clock"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ingest"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
	"github.com/SreemanSreekar/Sreeman_Aptos/pos_ruleset"
)

type Handler struct {
	Cfg       *config.Config
	Pools     *db.Pools
	Projector ledger.Projector
}

type SyncRequest struct {
	BatchID          string                   `json:"batch_id,omitempty"`
	EpochGen         int64                    `json:"epoch_gen"`
	ExpectedEventIDs []string                 `json:"expected_event_ids,omitempty"`
	SubmittedCount   *int                     `json:"submitted_count,omitempty"`
	Events           []ingest.BatchEventInput `json:"events,omitempty"`
}

type FenceRequest struct {
	EpochGen int64 `json:"epoch_gen"`
}

func extractPgCodeAndMsg(err error) (string, string, bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "22023" {
		code := pgErr.Message
		if idx := strings.IndexByte(code, ':'); idx > 0 {
			code = strings.TrimSpace(code[:idx])
		}
		return code, pgErr.Message, true
	}
	return "", "", false
}

func (h *Handler) TerminalSync(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("id")
	terminalID := r.PathValue("tid")
	if storeID == "" || terminalID == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_STORE_OR_TERMINAL", "store_id and terminal_id are required")
		return
	}

	ingestNow, simNow, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	batchID := uuid.New()
	if req.BatchID != "" {
		parsed, err := uuid.Parse(req.BatchID)
		if err != nil {
			ledger.WriteError(w, http.StatusBadRequest, "INVALID_BATCH_ID", "invalid batch_id")
			return
		}
		batchID = parsed
	}

	var expectedIDs []uuid.UUID
	if req.ExpectedEventIDs != nil {
		expectedIDs = make([]uuid.UUID, 0, len(req.ExpectedEventIDs))
		for _, raw := range req.ExpectedEventIDs {
			uid, err := uuid.Parse(raw)
			if err != nil {
				ledger.WriteError(w, http.StatusBadRequest, "INVALID_EVENT_ID", "invalid event_id in expected_event_ids")
				return
			}
			expectedIDs = append(expectedIDs, uid)
		}
	} else {
		seen := make(map[uuid.UUID]bool, len(req.Events))
		expectedIDs = make([]uuid.UUID, 0, len(req.Events))
		for _, ev := range req.Events {
			uid, err := uuid.Parse(ev.EventID)
			if err != nil {
				ledger.WriteError(w, http.StatusBadRequest, "INVALID_EVENT_ID", "invalid event_id in events")
				return
			}
			if !seen[uid] {
				seen[uid] = true
				expectedIDs = append(expectedIDs, uid)
			}
		}
	}

	submittedCount := len(expectedIDs)
	if req.SubmittedCount != nil {
		submittedCount = *req.SubmittedCount
	}

	batchReq := ingest.BatchRequest{
		BatchID:    batchID.String(),
		StoreID:    storeID,
		TerminalID: terminalID,
		Events:     req.Events,
	}
	prepared, inBatchDupes, httpErr := ingest.PrepareOfflineEvents(batchReq, ingestNow, h.Cfg.MaxFutureSkewSeconds, h.Cfg.MaxEventLatenessHours)
	if httpErr != nil {
		ledger.WriteError(w, httpErr.Status, httpErr.Code, httpErr.Message)
		return
	}

	var resp map[string]any
	var abortStatus int
	var abortCode, abortMsg string

	txErr := db.WithTxRetry(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		abortStatus = 0

		summary, err := ingest.IngestPreparedEventsInTx(r.Context(), tx, batchID, storeID, terminalID, prepared, inBatchDupes, ingestNow, h.Projector)
		if err != nil {
			var he *ingest.HTTPError
			if errors.As(err, &he) {
				abortStatus = he.Status
				abortCode = he.Code
				abortMsg = he.Message
				return errors.New("abort")
			}
			return err
		}

		epochGen := req.EpochGen
		if epochGen <= 0 {
			if err := tx.QueryRow(r.Context(), `SELECT active_epoch_gen FROM store_offline_epochs WHERE store_id = $1`, storeID).Scan(&epochGen); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					abortStatus = http.StatusNotFound
					abortCode = "EPOCH_NOT_FOUND"
					abortMsg = "store_offline_epochs row not found"
					return errors.New("abort")
				}
				return err
			}
		}

		if _, err := tx.Exec(r.Context(),
			`SELECT reconcile_terminal_slice($1, $2, $3, $4, $5, $6)`,
			storeID, terminalID, epochGen, expectedIDs, submittedCount, simNow,
		); err != nil {
			if code, msg, ok := extractPgCodeAndMsg(err); ok {
				abortStatus = http.StatusConflict
				abortCode = code
				abortMsg = msg
				return errors.New("abort")
			}
			return err
		}

		var newEpoch int64
		if err := tx.QueryRow(r.Context(),
			`SELECT advance_store_epoch($1, $2)`,
			storeID, simNow,
		).Scan(&newEpoch); err != nil {
			if code, msg, ok := extractPgCodeAndMsg(err); ok {
				abortStatus = http.StatusConflict
				abortCode = code
				abortMsg = msg
				return errors.New("abort")
			}
			return err
		}

		var activeEpoch int64
		if err := tx.QueryRow(r.Context(), `SELECT active_epoch_gen FROM store_offline_epochs WHERE store_id = $1`, storeID).Scan(&activeEpoch); err != nil {
			return err
		}

		// If activeEpoch is still waiting on sibling terminals (and we reconciled activeEpoch itself), return BLOCKED_PENDING_SIBLINGS.
		if newEpoch == 0 && epochGen == activeEpoch {
			resp = map[string]any{
				"store_id":                storeID,
				"terminal_id":             terminalID,
				"reconciled_epoch":        epochGen,
				"active_epoch_gen":        activeEpoch,
				"offline_allowance_minor": 0,
				"renewal_status":          "BLOCKED_PENDING_SIBLINGS",
				"batch_summary":           summary,
			}
			return nil
		}

		// Fetch this terminal's ACTIVE slice in activeEpoch (newly granted or already advanced).
		var sliceMinor int64
		var loyaltySlice *int64
		var issuedAt, expiresAt time.Time
		var state string
		err = tx.QueryRow(r.Context(), `
			SELECT slice_minor, loyalty_slice_points, issued_at_utc, slice_expires_at_utc, state
			FROM terminal_offline_slices
			WHERE store_id = $1 AND terminal_id = $2 AND epoch_gen = $3`,
			storeID, terminalID, activeEpoch,
		).Scan(&sliceMinor, &loyaltySlice, &issuedAt, &expiresAt, &state)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && state != "ACTIVE") {
			resp = map[string]any{
				"store_id":                storeID,
				"terminal_id":             terminalID,
				"reconciled_epoch":        epochGen,
				"active_epoch_gen":        activeEpoch,
				"offline_allowance_minor": 0,
				"renewal_status":          "BLOCKED_PENDING_SIBLINGS",
				"batch_summary":           summary,
			}
			return nil
		}
		if err != nil {
			return err
		}

		// Load fresh snapshot of ACTIVE cards with balance_minor > 0
		cards := make(map[string]pos_ruleset.CardSnapshot)
		cardRows, err := tx.Query(r.Context(), `
			SELECT card_id, balance_minor, status, eligible_stores
			FROM gift_cards
			WHERE status = 'ACTIVE' AND balance_minor > 0
			ORDER BY card_id ASC`,
		)
		if err != nil {
			return err
		}
		for cardRows.Next() {
			var cs pos_ruleset.CardSnapshot
			if err := cardRows.Scan(&cs.CardID, &cs.BalanceMinor, &cs.Status, &cs.EligibleStores); err != nil {
				cardRows.Close()
				return err
			}
			cards[cs.CardID] = cs
		}
		if err := cardRows.Err(); err != nil {
			cardRows.Close()
			return err
		}
		cardRows.Close()

		grant := pos_ruleset.LocalSliceGrant{
			StoreID:            storeID,
			TerminalID:         terminalID,
			EpochGen:           activeEpoch,
			SliceMinor:         sliceMinor,
			LoyaltySlicePoints: loyaltySlice,
			IssuedAtUTC:        issuedAt.UTC().Truncate(time.Microsecond),
			SliceExpiresAtUTC:  expiresAt.UTC().Truncate(time.Microsecond),
			Cards:              cards,
		}

		resp = map[string]any{
			"store_id":                storeID,
			"terminal_id":             terminalID,
			"reconciled_epoch":        epochGen,
			"active_epoch_gen":        activeEpoch,
			"offline_allowance_minor": sliceMinor,
			"renewal_status":          "GRANTED",
			"grant":                   grant,
			"batch_summary":           summary,
		}
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

	ledger.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) TerminalFence(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("id")
	terminalID := r.PathValue("tid")
	if storeID == "" || terminalID == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_STORE_OR_TERMINAL", "store_id and terminal_id are required")
		return
	}

	_, simNow, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var req FenceRequest
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			ledger.WriteError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
			return
		}
	}

	var fencedEpoch int64
	var abortStatus int
	var abortCode, abortMsg string

	txErr := db.WithTxRetry(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		abortStatus = 0
		epochGen := req.EpochGen
		if epochGen <= 0 {
			if err := tx.QueryRow(r.Context(), `SELECT active_epoch_gen FROM store_offline_epochs WHERE store_id = $1`, storeID).Scan(&epochGen); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					abortStatus = http.StatusNotFound
					abortCode = "EPOCH_NOT_FOUND"
					abortMsg = "store_offline_epochs row not found"
					return errors.New("abort")
				}
				return err
			}
		}

		if _, err := tx.Exec(r.Context(),
			`SELECT fence_terminal_slice($1, $2, $3, $4)`,
			storeID, terminalID, epochGen, simNow,
		); err != nil {
			if code, msg, ok := extractPgCodeAndMsg(err); ok {
				abortStatus = http.StatusConflict
				abortCode = code
				abortMsg = msg
				return errors.New("abort")
			}
			return err
		}
		fencedEpoch = epochGen
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

	ledger.WriteJSON(w, http.StatusOK, map[string]any{
		"store_id":    storeID,
		"terminal_id": terminalID,
		"epoch_gen":   fencedEpoch,
		"state":       "FENCED",
	})
}
