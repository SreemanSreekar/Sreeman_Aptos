package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/checksum"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/clock"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

type Handler struct {
	Cfg       *config.Config
	Pools     *db.Pools
	Projector ledger.Projector
}

type StoreSeed struct {
	StoreID      string   `json:"store_id"`
	IANATimezone string   `json:"iana_timezone"`
	TerminalIDs  []string `json:"terminal_ids"`
}

type GiftCardSeed struct {
	CardID              string   `json:"card_id"`
	InitialBalanceMinor int64    `json:"initial_balance_minor"`
	EligibleStores      []string `json:"eligible_stores"`
}

type LoyaltySeed struct {
	AccountID string `json:"account_id"`
}

type SeedRequest struct {
	Stores          []StoreSeed    `json:"stores,omitempty"`
	GiftCards       []GiftCardSeed `json:"gift_cards,omitempty"`
	LoyaltyAccounts []LoyaltySeed  `json:"loyalty_accounts,omitempty"`
}

func extractPg22023(err error) (string, string, bool) {
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

func (h *Handler) Seed(w http.ResponseWriter, r *http.Request) {
	_, simNow, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var req SeedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_JSON", err.Error())
		return
	}

	var abortStatus int
	var abortCode, abortMsg string

	txErr := db.WithTxRetry(r.Context(), h.Pools.Admin, func(tx pgx.Tx) error {
		abortStatus = 0
		for _, s := range req.Stores {
			if _, err := tx.Exec(r.Context(),
				`SELECT admin_seed_store($1, $2, $3, $4)`,
				s.StoreID, s.IANATimezone, s.TerminalIDs, simNow,
			); err != nil {
				if code, msg, ok := extractPg22023(err); ok {
					abortStatus = http.StatusConflict
					abortCode = code
					abortMsg = msg
					return errors.New("abort")
				}
				return err
			}
		}
		for _, gc := range req.GiftCards {
			if _, err := tx.Exec(r.Context(),
				`SELECT admin_seed_gift_card($1, $2, $3)`,
				gc.CardID, gc.InitialBalanceMinor, gc.EligibleStores,
			); err != nil {
				if code, msg, ok := extractPg22023(err); ok {
					abortStatus = http.StatusConflict
					abortCode = code
					abortMsg = msg
					return errors.New("abort")
				}
				return err
			}
		}
		for _, la := range req.LoyaltyAccounts {
			if _, err := tx.Exec(r.Context(),
				`SELECT admin_seed_loyalty_account($1)`,
				la.AccountID,
			); err != nil {
				return err
			}
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

	ledger.WriteJSON(w, http.StatusOK, map[string]any{
		"status":           "seeded",
		"stores":           len(req.Stores),
		"gift_cards":       len(req.GiftCards),
		"loyalty_accounts": len(req.LoyaltyAccounts),
	})
}

func (h *Handler) CaptureSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 1. Reader Tx: REPEATABLE READ READ ONLY + pg_export_snapshot()
	readerTx, err := h.Pools.Admin.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	defer func() { _ = readerTx.Rollback(ctx) }()

	var exportID, pgSnapText, xminText, xmaxText string
	if err := readerTx.QueryRow(ctx, `
		SELECT
			pg_export_snapshot(),
			pg_current_snapshot()::TEXT,
			pg_snapshot_xmin(pg_current_snapshot())::TEXT,
			pg_snapshot_xmax(pg_current_snapshot())::TEXT`,
	).Scan(&exportID, &pgSnapText, &xminText, &xmaxText); err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	csum, err := checksum.ComputeChecksums(ctx, readerTx)
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	// 2. Writer Tx: REPEATABLE READ READ WRITE + SET TRANSACTION SNAPSHOT before any query
	writerTx, err := h.Pools.Admin.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	defer func() { _ = writerTx.Rollback(ctx) }()

	// SET TRANSACTION SNAPSHOT requires a literal snapshot ID string.
	if _, err := writerTx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", strings.ReplaceAll(exportID, "'", "''"))); err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	var snapshotID int64
	if err := writerTx.QueryRow(ctx, `SELECT admin_capture_snapshot($1)`, csum.ReadModelSHA256).Scan(&snapshotID); err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	if err := writerTx.Commit(ctx); err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if err := readerTx.Commit(ctx); err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}

	ledger.WriteJSON(w, http.StatusOK, map[string]any{
		"snapshot_id":       snapshotID,
		"pg_snapshot_text":  pgSnapText,
		"snap_xmin":         xminText,
		"snap_xmax":         xmaxText,
		"read_model_sha256": csum.ReadModelSHA256,
		"economic_sha256":   csum.EconomicSHA256,
		"event_count":       csum.EventCount,
	})
}

func (h *Handler) Rebuild(w http.ResponseWriter, r *http.Request) {
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = "full"
	}
	if mode != "full" && mode != "snapshot" {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_REBUILD_MODE", "mode must be full or snapshot")
		return
	}

	var reqSnapID *int64
	if rawID := r.URL.Query().Get("snapshot_id"); rawID != "" {
		parsed, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || parsed <= 0 {
			ledger.WriteError(w, http.StatusBadRequest, "INVALID_SNAPSHOT_ID", "snapshot_id must be a positive integer")
			return
		}
		reqSnapID = &parsed
	}

	var replayed int
	var usedSnapID *int64
	var abortStatus int
	var abortCode, abortMsg string

	txErr := db.WithTxRetry(r.Context(), h.Pools.Admin, func(tx pgx.Tx) error {
		abortStatus = 0
		var events []ledger.EventRecord
		var err error

		if mode == "full" {
			if _, err := tx.Exec(r.Context(), `SELECT admin_reset_read_models()`); err != nil {
				return err
			}
			events, err = loadEvents(r.Context(), tx, `
				SELECT event_id, applied_order, event_type, channel,
				       epoch_gen, terminal_epoch_seq, store_id, terminal_id,
				       occurred_at, ingested_at, origin_business_day, origin_cutoff_utc,
				       tz_version, sale_id, parent_sale_id, card_id, account_id,
				       amount_minor, points, payload_json, payload_hash
				FROM ledger_events
				ORDER BY applied_order ASC`)
			if err != nil {
				return err
			}
		} else {
			var snapID int64
			var snapText, snapXmin string
			if reqSnapID != nil {
				err = tx.QueryRow(r.Context(), `
					SELECT snapshot_id, pg_snapshot_text, snap_xmin::TEXT
					FROM ledger_snapshots
					WHERE snapshot_id = $1`,
					*reqSnapID,
				).Scan(&snapID, &snapText, &snapXmin)
			} else {
				err = tx.QueryRow(r.Context(), `
					SELECT snapshot_id, pg_snapshot_text, snap_xmin::TEXT
					FROM ledger_snapshots
					ORDER BY snapshot_id DESC
					LIMIT 1`,
				).Scan(&snapID, &snapText, &snapXmin)
			}
			if errors.Is(err, pgx.ErrNoRows) {
				abortStatus = http.StatusNotFound
				abortCode = "SNAPSHOT_NOT_FOUND"
				abortMsg = "no snapshot available for rebuild"
				return errors.New("abort")
			}
			if err != nil {
				return err
			}
			usedSnapID = &snapID

			if _, err := tx.Exec(r.Context(), `SELECT admin_restore_snapshot($1)`, snapID); err != nil {
				if code, msg, ok := extractPg22023(err); ok {
					abortStatus = http.StatusNotFound
					abortCode = code
					abortMsg = msg
					return errors.New("abort")
				}
				return err
			}

			events, err = loadEvents(r.Context(), tx, `
				SELECT event_id, applied_order, event_type, channel,
				       epoch_gen, terminal_epoch_seq, store_id, terminal_id,
				       occurred_at, ingested_at, origin_business_day, origin_cutoff_utc,
				       tz_version, sale_id, parent_sale_id, card_id, account_id,
				       amount_minor, points, payload_json, payload_hash
				FROM ledger_events
				WHERE commit_xid >= $1::text::xid8
				  AND NOT pg_visible_in_snapshot(commit_xid, $2::pg_snapshot)
				ORDER BY applied_order ASC`,
				snapXmin, snapText,
			)
			if err != nil {
				return err
			}
		}

		for _, ev := range events {
			var projErr error
			switch ev.EventType {
			case "sale", "return", "void":
				projErr = h.Projector.ProjectSaleEvent(r.Context(), tx, ev)
			case "giftcard_redeem":
				projErr = h.Projector.ProjectGiftCardEvent(r.Context(), tx, ev)
			case "loyalty_earn", "loyalty_burn":
				projErr = h.Projector.ProjectLoyaltyEvent(r.Context(), tx, ev)
			case "day_close":
				projErr = h.Projector.ProjectDayCloseEvent(r.Context(), tx, ev)
			}
			if projErr != nil {
				return projErr
			}
		}
		replayed = len(events)
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

	resp := map[string]any{
		"mode":            mode,
		"replayed_events": replayed,
	}
	if usedSnapID != nil {
		resp["snapshot_id"] = *usedSnapID
	}
	ledger.WriteJSON(w, http.StatusOK, resp)
}

func loadEvents(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]ledger.EventRecord, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []ledger.EventRecord
	for rows.Next() {
		var ev ledger.EventRecord
		var payloadBytes []byte
		if err := rows.Scan(
			&ev.EventID, &ev.AppliedOrder, &ev.EventType, &ev.Channel,
			&ev.EpochGen, &ev.TerminalEpochSeq, &ev.StoreID, &ev.TerminalID,
			&ev.OccurredAt, &ev.IngestedAt, &ev.OriginBusinessDay, &ev.OriginCutoffUTC,
			&ev.TZVersion, &ev.SaleID, &ev.ParentSaleID, &ev.CardID, &ev.AccountID,
			&ev.AmountMinor, &ev.Points, &payloadBytes, &ev.PayloadHash,
		); err != nil {
			return nil, err
		}
		ev.OccurredAt = ev.OccurredAt.UTC().Truncate(time.Microsecond)
		ev.IngestedAt = ev.IngestedAt.UTC().Truncate(time.Microsecond)
		ev.OriginCutoffUTC = ev.OriginCutoffUTC.UTC().Truncate(time.Microsecond)
		ev.PayloadJSON = payloadBytes
		events = append(events, ev)
	}
	return events, rows.Err()
}
