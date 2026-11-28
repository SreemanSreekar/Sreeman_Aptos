package days

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/canonical"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/clock"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

// LoadStoreLocation fetches the store's immutable IANA timezone and returns *time.Location.
func LoadStoreLocation(ctx context.Context, tx pgx.Tx, storeID string) (*time.Location, string, error) {
	var tzName string
	err := tx.QueryRow(ctx, `SELECT iana_timezone FROM stores WHERE store_id = $1`, storeID).Scan(&tzName)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, "", fmt.Errorf("STORE_NOT_FOUND:%s", storeID)
		}
		return nil, "", err
	}
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return nil, "", fmt.Errorf("INVALID_STORE_TZ:%s: %w", tzName, err)
	}
	return loc, tzName, nil
}

// EnsureOpenDayRow inserts (if missing) and locks store_business_days(storeID, day) FOR UPDATE.
func EnsureOpenDayRow(ctx context.Context, tx pgx.Tx, storeID string, loc *time.Location, day time.Time) (string, time.Time, error) {
	cutoffUTC := clock.CutoffUTC(day, loc)
	_, err := tx.Exec(ctx, `
		INSERT INTO store_business_days (store_id, business_day, cutoff_utc, tz_version, status)
		VALUES ($1, $2, $3, $4, 'OPEN')
		ON CONFLICT (store_id, business_day) DO NOTHING`,
		storeID, day, cutoffUTC, clock.TZVersion,
	)
	if err != nil {
		return "", time.Time{}, err
	}
	var status string
	var persistedCutoff time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, cutoff_utc
		FROM store_business_days
		WHERE store_id = $1 AND business_day = $2
		FOR UPDATE`,
		storeID, day,
	).Scan(&status, &persistedCutoff)
	if err != nil {
		return "", time.Time{}, err
	}
	return status, persistedCutoff.UTC().Truncate(time.Microsecond), nil
}

// AppendDayCloseEventAndClose durably appends the deterministic UUIDv5 day_close event to
// ledger_events (idempotently) and transitions store_business_days(storeID, day) OPEN -> CLOSED.
func AppendDayCloseEventAndClose(
	ctx context.Context,
	tx pgx.Tx,
	storeID string,
	day time.Time,
	cutoffUTC time.Time,
	closeAt time.Time,
) (uuid.UUID, time.Time, error) {
	dayStr := day.Format("2006-01-02")
	closeEventID := canonical.DayCloseEventID(storeID, dayStr)
	closeAt = closeAt.UTC().Truncate(time.Microsecond)

	// Exact sorted-key canonical JSON matching chk_day_close_fields in 001_init.sql
	payloadBytes, err := canonical.CanonicalJSON(map[string]string{
		"business_day": dayStr,
		"store_id":     storeID,
	})
	if err != nil {
		return uuid.Nil, time.Time{}, err
	}
	payloadHash := canonical.PayloadHash("day_close", storeID, "SYSTEM", cutoffUTC, payloadBytes)

	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_events (
			event_id, applied_order, event_type, channel,
			store_id, terminal_id, occurred_at, ingested_at,
			origin_business_day, origin_cutoff_utc, tz_version,
			payload_json, payload_hash
		) VALUES (
			$1, nextval('ledger_applied_order_seq'), 'day_close', 'SYSTEM',
			$2, 'SYSTEM', $3, $4,
			$5, $3, $6,
			$7::jsonb, $8
		)
		ON CONFLICT (event_id) DO NOTHING`,
		closeEventID, storeID, cutoffUTC, closeAt,
		day, clock.TZVersion,
		string(payloadBytes), payloadHash,
	)
	if err != nil {
		return uuid.Nil, time.Time{}, fmt.Errorf("insert day_close event: %w", err)
	}

	// Read back the authoritative ingested_at from ledger_events in case it was already inserted
	var actualClosedAt time.Time
	err = tx.QueryRow(ctx, `SELECT ingested_at FROM ledger_events WHERE event_id = $1`, closeEventID).Scan(&actualClosedAt)
	if err != nil {
		return uuid.Nil, time.Time{}, err
	}
	actualClosedAt = actualClosedAt.UTC().Truncate(time.Microsecond)

	_, err = tx.Exec(ctx, `
		UPDATE store_business_days
		SET status = 'CLOSED',
		    closed_at_utc = $3,
		    close_event_id = $4
		WHERE store_id = $1 AND business_day = $2 AND status = 'OPEN'`,
		storeID, day, actualClosedAt, closeEventID,
	)
	if err != nil {
		return uuid.Nil, time.Time{}, fmt.Errorf("close store_business_days: %w", err)
	}
	return closeEventID, actualClosedAt, nil
}

// ResolvePostingDay determines whether originDay is still OPEN at effectiveNow.
// If originDay is already CLOSED or effectiveNow >= cutoffUTC, it lazily closes originDay
// (appending the deterministic day_close event) and returns the current open business day
// with isAdjustment = true so closed days are never mutated.
func ResolvePostingDay(
	ctx context.Context,
	tx pgx.Tx,
	storeID string,
	loc *time.Location,
	originDay time.Time,
	effectiveNow time.Time,
) (time.Time, bool, error) {
	status, cutoffUTC, err := EnsureOpenDayRow(ctx, tx, storeID, loc, originDay)
	if err != nil {
		return time.Time{}, false, err
	}

	if status == "OPEN" && effectiveNow.Before(cutoffUTC) {
		return originDay, false, nil
	}

	// Origin day is closed or past 02:00 cutoff: lazily close if still OPEN.
	if status == "OPEN" {
		if _, _, err := AppendDayCloseEventAndClose(ctx, tx, storeID, originDay, cutoffUTC, effectiveNow); err != nil {
			return time.Time{}, false, err
		}
	}

	postingDay := clock.BusinessDay(effectiveNow, loc)
	// Ensure postingDay exists and is locked FOR UPDATE.
	postStatus, _, err := EnsureOpenDayRow(ctx, tx, storeID, loc, postingDay)
	if err != nil {
		return time.Time{}, false, err
	}
	if postStatus == "CLOSED" {
		// Edge case: if effectiveNow's own day was already closed (should not happen before cutoff),
		// advance to next day.
		postingDay = postingDay.AddDate(0, 0, 1)
		if _, _, err := EnsureOpenDayRow(ctx, tx, storeID, loc, postingDay); err != nil {
			return time.Time{}, false, err
		}
	}
	return postingDay, true, nil
}

// ApplyTotalsDelta updates store_day_totals (and inserts into store_day_adjustments if isAdjustment).
// signedDeltaMinor is:
//   - positive for sale (+amt) or reversal of return/void (+amt)
//   - negative for return (-amt), void (-amt), or reversal of sale (-amt)
//   - positive magnitude for giftcard_redeem (tender tracking; does not alter net_total_minor).
func ApplyTotalsDelta(
	ctx context.Context,
	tx pgx.Tx,
	storeID string,
	postingDay time.Time,
	originDay time.Time,
	eventID uuid.UUID,
	causedByEventID uuid.UUID,
	eventType string,
	entryKind string, // "DIRECT", "LATE_POSTING", or "RECLASSIFICATION"
	revision int,
	signedDeltaMinor int64,
	isAdjustment bool,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO store_day_totals (store_id, business_day)
		VALUES ($1, $2)
		ON CONFLICT (store_id, business_day) DO NOTHING`,
		storeID, postingDay,
	)
	if err != nil {
		return fmt.Errorf("ensure store_day_totals: %w", err)
	}

	if isAdjustment || entryKind == "RECLASSIFICATION" || entryKind == "LATE_POSTING" {
		if entryKind == "DIRECT" {
			entryKind = "LATE_POSTING"
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO store_day_adjustments (
				store_id, posting_business_day, origin_business_day,
				event_id, entry_kind, revision,
				caused_by_event_id, event_type, amount_delta_minor
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			storeID, postingDay, originDay,
			eventID, entryKind, revision,
			causedByEventID, eventType, signedDeltaMinor,
		)
		if err != nil {
			return fmt.Errorf("insert store_day_adjustments: %w", err)
		}

		// giftcard_redeem is a tender type and does not alter net sales totals.
		if eventType != "giftcard_redeem" {
			_, err = tx.Exec(ctx, `
				UPDATE store_day_totals
				SET adjustments_net_minor = adjustments_net_minor + $3,
				    net_total_minor       = net_total_minor + $3
				WHERE store_id = $1 AND business_day = $2`,
				storeID, postingDay, signedDeltaMinor,
			)
			if err != nil {
				return fmt.Errorf("update store_day_totals adjustment: %w", err)
			}
		}
		return nil
	}

	// Direct posting to open origin business day.
	switch eventType {
	case "sale":
		_, err = tx.Exec(ctx, `
			UPDATE store_day_totals
			SET sales_minor     = sales_minor + $3,
			    net_total_minor = net_total_minor + $3
			WHERE store_id = $1 AND business_day = $2`,
			storeID, postingDay, signedDeltaMinor,
		)
	case "return":
		mag := -signedDeltaMinor
		_, err = tx.Exec(ctx, `
			UPDATE store_day_totals
			SET returns_minor   = returns_minor + $3,
			    net_total_minor = net_total_minor - $3
			WHERE store_id = $1 AND business_day = $2`,
			storeID, postingDay, mag,
		)
	case "void":
		mag := -signedDeltaMinor
		_, err = tx.Exec(ctx, `
			UPDATE store_day_totals
			SET voids_minor     = voids_minor + $3,
			    net_total_minor = net_total_minor - $3
			WHERE store_id = $1 AND business_day = $2`,
			storeID, postingDay, mag,
		)
	case "giftcard_redeem":
		mag := signedDeltaMinor
		if mag < 0 {
			mag = -mag
		}
		_, err = tx.Exec(ctx, `
			UPDATE store_day_totals
			SET giftcard_redeem_minor = giftcard_redeem_minor + $3
			WHERE store_id = $1 AND business_day = $2`,
			storeID, postingDay, mag,
		)
	default:
		return fmt.Errorf("unsupported totals event_type: %s", eventType)
	}
	return err
}

// ProjectDayCloseEvent replays a durable day_close event during full or snapshot+tail rebuilds.
func ProjectDayCloseEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO store_business_days (store_id, business_day, cutoff_utc, tz_version, status)
		VALUES ($1, $2, $3, $4, 'OPEN')
		ON CONFLICT (store_id, business_day) DO NOTHING`,
		e.StoreID, e.OriginBusinessDay, e.OriginCutoffUTC, e.TZVersion,
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE store_business_days
		SET status = 'CLOSED',
		    closed_at_utc = $3,
		    close_event_id = $4
		WHERE store_id = $1 AND business_day = $2 AND status = 'OPEN'`,
		e.StoreID, e.OriginBusinessDay, e.IngestedAt.UTC().Truncate(time.Microsecond), e.EventID,
	)
	return err
}

// Handler serves POST /v1/stores/{id}/days/{day}/close and GET /v1/stores/{id}/totals.
type Handler struct {
	Cfg   *config.Config
	Pools *db.Pools
}

func (h *Handler) CloseDay(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("id")
	dayStr := r.PathValue("day")
	day, err := time.Parse("2006-01-02", dayStr)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_BUSINESS_DAY", "day must be YYYY-MM-DD")
		return
	}

	ingestNow, _, err := clock.IngestNow(r, h.Cfg.AllowSimulatedClock)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_CLOCK", err.Error())
		return
	}

	var resp map[string]any
	var httpErrStatus int
	var httpErrCode, httpErrMsg string

	txErr := db.WithTxRetry(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		loc, _, err := LoadStoreLocation(r.Context(), tx, storeID)
		if err != nil {
			httpErrStatus = http.StatusNotFound
			httpErrCode = "STORE_NOT_FOUND"
			httpErrMsg = err.Error()
			return errors.New("abort")
		}

		status, cutoffUTC, err := EnsureOpenDayRow(r.Context(), tx, storeID, loc, day)
		if err != nil {
			return err
		}

		if status == "CLOSED" {
			var closedAt time.Time
			var closeEventID uuid.UUID
			if err := tx.QueryRow(r.Context(), `
				SELECT closed_at_utc, close_event_id
				FROM store_business_days
				WHERE store_id = $1 AND business_day = $2`,
				storeID, day,
			).Scan(&closedAt, &closeEventID); err != nil {
				return err
			}
			resp = map[string]any{
				"store_id":       storeID,
				"business_day":   dayStr,
				"status":         "CLOSED",
				"cutoff_utc":     cutoffUTC.Format(canonical.TimestampFormatMicros),
				"closed_at_utc":  closedAt.UTC().Format(canonical.TimestampFormatMicros),
				"close_event_id": closeEventID.String(),
				"idempotent":     true,
			}
			return nil
		}

		if ingestNow.Before(cutoffUTC) {
			httpErrStatus = http.StatusConflict
			httpErrCode = "BUSINESS_DAY_CUTOFF_NOT_REACHED"
			httpErrMsg = fmt.Sprintf("cannot close %s at %s before cutoff %s", dayStr, ingestNow.Format(time.RFC3339), cutoffUTC.Format(time.RFC3339))
			return errors.New("abort")
		}

		closeEventID, closedAt, err := AppendDayCloseEventAndClose(r.Context(), tx, storeID, day, cutoffUTC, ingestNow)
		if err != nil {
			return err
		}
		resp = map[string]any{
			"store_id":       storeID,
			"business_day":   dayStr,
			"status":         "CLOSED",
			"cutoff_utc":     cutoffUTC.Format(canonical.TimestampFormatMicros),
			"closed_at_utc":  closedAt.UTC().Format(canonical.TimestampFormatMicros),
			"close_event_id": closeEventID.String(),
			"idempotent":     false,
		}
		return nil
	})

	if httpErrStatus != 0 {
		ledger.WriteError(w, httpErrStatus, httpErrCode, httpErrMsg)
		return
	}
	if txErr != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", txErr.Error())
		return
	}
	ledger.WriteJSON(w, http.StatusOK, resp)
}

func (h *Handler) GetStoreTotals(w http.ResponseWriter, r *http.Request) {
	storeID := r.PathValue("id")
	dayStr := r.URL.Query().Get("day")
	if dayStr == "" {
		ledger.WriteError(w, http.StatusBadRequest, "MISSING_DAY", "query parameter day=YYYY-MM-DD is required")
		return
	}
	day, err := time.Parse("2006-01-02", dayStr)
	if err != nil {
		ledger.WriteError(w, http.StatusBadRequest, "INVALID_BUSINESS_DAY", "day must be YYYY-MM-DD")
		return
	}

	var resp map[string]any
	var notFound bool

	err = db.WithReadOnlyTx(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		var dayStatus, tzVersion string
		var cutoffUTC time.Time
		var closedAt *time.Time
		var closeEventID *uuid.UUID
		var salesMinor, returnsMinor, voidsMinor, gcRedeemMinor, adjNetMinor, netTotalMinor int64

		// Fetch store existence, business-day metadata, and totals in one round trip.
		err := tx.QueryRow(r.Context(), `
			SELECT
				COALESCE(bd.status, 'OPEN'),
				COALESCE(bd.cutoff_utc, '0001-01-01'::timestamptz),
				COALESCE(bd.tz_version, ''),
				bd.closed_at_utc,
				bd.close_event_id,
				COALESCE(t.sales_minor, 0),
				COALESCE(t.returns_minor, 0),
				COALESCE(t.voids_minor, 0),
				COALESCE(t.giftcard_redeem_minor, 0),
				COALESCE(t.adjustments_net_minor, 0),
				COALESCE(t.net_total_minor, 0)
			FROM stores s
			LEFT JOIN store_business_days bd
				ON bd.store_id = s.store_id AND bd.business_day = $2
			LEFT JOIN store_day_totals t
				ON t.store_id = s.store_id AND t.business_day = $2
			WHERE s.store_id = $1`,
			storeID, day,
		).Scan(
			&dayStatus, &cutoffUTC, &tzVersion, &closedAt, &closeEventID,
			&salesMinor, &returnsMinor, &voidsMinor, &gcRedeemMinor,
			&adjNetMinor, &netTotalMinor,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			notFound = true
			return nil
		}
		if err != nil {
			return err
		}

		rows, err := tx.Query(r.Context(), `
			SELECT origin_business_day, event_id, entry_kind, revision,
			       caused_by_event_id, event_type, amount_delta_minor
			FROM store_day_adjustments
			WHERE store_id = $1 AND posting_business_day = $2
			ORDER BY origin_business_day ASC, event_id ASC, entry_kind ASC, revision ASC`,
			storeID, day,
		)
		if err != nil {
			return err
		}
		defer rows.Close()

		adjs := make([]map[string]any, 0)
		for rows.Next() {
			var origDay time.Time
			var evID, causedBy uuid.UUID
			var kind, evType string
			var rev int
			var delta int64
			if err := rows.Scan(&origDay, &evID, &kind, &rev, &causedBy, &evType, &delta); err != nil {
				return err
			}
			adjs = append(adjs, map[string]any{
				"origin_business_day": origDay.Format("2006-01-02"),
				"event_id":            evID.String(),
				"entry_kind":          kind,
				"revision":            rev,
				"caused_by_event_id":  causedBy.String(),
				"event_type":          evType,
				"amount_delta_minor":  delta,
			})
		}
		if err := rows.Err(); err != nil {
			return err
		}

		dayMeta := map[string]any{
			"store_id":     storeID,
			"business_day": dayStr,
			"status":       dayStatus,
		}
		if !cutoffUTC.IsZero() {
			dayMeta["cutoff_utc"] = cutoffUTC.UTC().Format(canonical.TimestampFormatMicros)
			dayMeta["tz_version"] = tzVersion
		}
		if closedAt != nil {
			dayMeta["closed_at_utc"] = closedAt.UTC().Format(canonical.TimestampFormatMicros)
		}
		if closeEventID != nil {
			dayMeta["close_event_id"] = closeEventID.String()
		}

		resp = map[string]any{
			"business_day": dayMeta,
			"totals": map[string]int64{
				"sales_minor":           salesMinor,
				"returns_minor":         returnsMinor,
				"voids_minor":           voidsMinor,
				"giftcard_redeem_minor": gcRedeemMinor,
				"adjustments_net_minor": adjNetMinor,
				"net_total_minor":       netTotalMinor,
			},
			"adjustments": adjs,
		}
		return nil
	})
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	if notFound {
		ledger.WriteError(w, http.StatusNotFound, "STORE_NOT_FOUND", "store not found: "+storeID)
		return
	}
	ledger.WriteJSON(w, http.StatusOK, resp)
}
