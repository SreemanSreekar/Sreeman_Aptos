package checksum

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/canonical"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

type Result struct {
	EconomicSHA256  string `json:"economic_sha256"`
	ReadModelSHA256 string `json:"read_model_sha256"`
	EventCount      int64  `json:"event_count"`
}

func encInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

func encDate(d *time.Time) string {
	if d == nil {
		return `\N`
	}
	return d.UTC().Format("2006-01-02")
}

func encTime(t *time.Time) string {
	if t == nil {
		return `\N`
	}
	return t.UTC().Truncate(time.Microsecond).Format(canonical.TimestampFormatMicros)
}

func encText(s *string) string {
	if s == nil {
		return `\N`
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(*s)
	return strings.TrimSuffix(buf.String(), "\n")
}

func encTextVal(s string) string {
	return encText(&s)
}

func encUUID(u *uuid.UUID) string {
	if u == nil {
		return `\N`
	}
	s := u.String()
	return encText(&s)
}

func writeRow(b *strings.Builder, cols ...string) {
	for i, c := range cols {
		if i > 0 {
			b.WriteByte('\t')
		}
		b.WriteString(c)
	}
	b.WriteByte('\n')
}

// ComputeChecksums calculates economic_sha256, read_model_sha256, and event_count
// inside the caller's REPEATABLE READ transaction.
func ComputeChecksums(ctx context.Context, tx pgx.Tx) (Result, error) {
	var res Result
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM ledger_events`).Scan(&res.EventCount); err != nil {
		return res, err
	}

	econHash, err := computeEconomicSHA256(ctx, tx)
	if err != nil {
		return res, err
	}
	readHash, err := computeReadModelSHA256(ctx, tx)
	if err != nil {
		return res, err
	}
	res.EconomicSHA256 = econHash
	res.ReadModelSHA256 = readHash
	return res, nil
}

func computeEconomicSHA256(ctx context.Context, tx pgx.Tx) (string, error) {
	var b strings.Builder

	// 1. #TABLE:economic_store_day_totals
	b.WriteString("#TABLE:economic_store_day_totals\n")
	rows, err := tx.Query(ctx, `
		WITH adj AS (
			SELECT
				store_id,
				origin_business_day,
				COALESCE(SUM(CASE WHEN event_type = 'sale' THEN amount_delta_minor ELSE 0 END), 0) AS adj_sales,
				COALESCE(SUM(CASE WHEN event_type = 'return' THEN -amount_delta_minor ELSE 0 END), 0) AS adj_returns,
				COALESCE(SUM(CASE WHEN event_type = 'void' THEN -amount_delta_minor ELSE 0 END), 0) AS adj_voids,
				COALESCE(SUM(CASE WHEN event_type = 'giftcard_redeem' THEN amount_delta_minor ELSE 0 END), 0) AS adj_gc
			FROM store_day_adjustments
			GROUP BY store_id, origin_business_day
		),
		combined AS (
			SELECT
				COALESCE(t.store_id, a.store_id) AS store_id,
				COALESCE(t.business_day, a.origin_business_day) AS origin_business_day,
				COALESCE(t.sales_minor, 0) + COALESCE(a.adj_sales, 0) AS sales_minor,
				COALESCE(t.returns_minor, 0) + COALESCE(a.adj_returns, 0) AS returns_minor,
				COALESCE(t.voids_minor, 0) + COALESCE(a.adj_voids, 0) AS voids_minor,
				COALESCE(t.giftcard_redeem_minor, 0) + COALESCE(a.adj_gc, 0) AS giftcard_redeem_minor
			FROM store_day_totals t
			FULL OUTER JOIN adj a
			  ON t.store_id = a.store_id AND t.business_day = a.origin_business_day
		)
		SELECT
			store_id,
			origin_business_day,
			sales_minor,
			returns_minor,
			voids_minor,
			giftcard_redeem_minor,
			(sales_minor - returns_minor - voids_minor) AS net_sales_minor
		FROM combined
		WHERE sales_minor <> 0
		   OR returns_minor <> 0
		   OR voids_minor <> 0
		   OR giftcard_redeem_minor <> 0
		ORDER BY store_id ASC, origin_business_day ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var storeID string
		var origDay time.Time
		var salesM, returnsM, voidsM, gcM, netM int64
		if err := rows.Scan(&storeID, &origDay, &salesM, &returnsM, &voidsM, &gcM, &netM); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(storeID), encDate(&origDay), encInt(salesM), encInt(returnsM), encInt(voidsM), encInt(gcM), encInt(netM))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 2. #TABLE:sales
	b.WriteString("#TABLE:sales\n")
	rows, err = tx.Query(ctx, `
		SELECT sale_id, store_id, origin_business_day, status, original_amount_minor, returned_amount_minor
		FROM sales
		ORDER BY sale_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var saleID uuid.UUID
		var storeID *string
		var origDay *time.Time
		var status string
		var origAmt, retAmt int64
		if err := rows.Scan(&saleID, &storeID, &origDay, &status, &origAmt, &retAmt); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encUUID(&saleID), encText(storeID), encDate(origDay), encTextVal(status), encInt(origAmt), encInt(retAmt))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 3. #TABLE:gift_cards
	b.WriteString("#TABLE:gift_cards\n")
	rows, err = tx.Query(ctx, `
		SELECT card_id, balance_minor, status
		FROM gift_cards
		ORDER BY card_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var cardID, status string
		var bal int64
		if err := rows.Scan(&cardID, &bal, &status); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(cardID), encInt(bal), encTextVal(status))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 4. #TABLE:loyalty_accounts
	b.WriteString("#TABLE:loyalty_accounts\n")
	rows, err = tx.Query(ctx, `
		SELECT account_id, earned_total, burned_total, balance, status
		FROM loyalty_accounts
		ORDER BY account_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var accID, status string
		var earned, burned, bal int64
		if err := rows.Scan(&accID, &earned, &burned, &bal, &status); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(accID), encInt(earned), encInt(burned), encInt(bal), encTextVal(status))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

func computeReadModelSHA256(ctx context.Context, tx pgx.Tx) (string, error) {
	var b strings.Builder

	// 1. #TABLE:store_business_days
	b.WriteString("#TABLE:store_business_days\n")
	rows, err := tx.Query(ctx, `
		SELECT store_id, business_day, cutoff_utc, tz_version, status
		FROM store_business_days
		ORDER BY store_id ASC, business_day ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var storeID, tzVer, status string
		var day, cutoff time.Time
		if err := rows.Scan(&storeID, &day, &cutoff, &tzVer, &status); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(storeID), encDate(&day), encTime(&cutoff), encTextVal(tzVer), encTextVal(status))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 2. #TABLE:store_day_totals
	b.WriteString("#TABLE:store_day_totals\n")
	rows, err = tx.Query(ctx, `
		SELECT store_id, business_day, sales_minor, returns_minor, voids_minor, giftcard_redeem_minor, adjustments_net_minor, net_total_minor
		FROM store_day_totals
		ORDER BY store_id ASC, business_day ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var storeID string
		var day time.Time
		var sM, rM, vM, gcM, adjM, netM int64
		if err := rows.Scan(&storeID, &day, &sM, &rM, &vM, &gcM, &adjM, &netM); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(storeID), encDate(&day), encInt(sM), encInt(rM), encInt(vM), encInt(gcM), encInt(adjM), encInt(netM))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 3. #TABLE:store_day_adjustments
	b.WriteString("#TABLE:store_day_adjustments\n")
	rows, err = tx.Query(ctx, `
		SELECT store_id, posting_business_day, origin_business_day, event_id, entry_kind, revision, caused_by_event_id, event_type, amount_delta_minor
		FROM store_day_adjustments
		ORDER BY store_id ASC, posting_business_day ASC, origin_business_day ASC, event_id ASC, entry_kind ASC, revision ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var storeID, entryKind, evType string
		var postDay, origDay time.Time
		var evID, causedByID uuid.UUID
		var rev, amtDelta int64
		if err := rows.Scan(&storeID, &postDay, &origDay, &evID, &entryKind, &rev, &causedByID, &evType, &amtDelta); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(storeID), encDate(&postDay), encDate(&origDay), encUUID(&evID), encTextVal(entryKind), encInt(rev), encUUID(&causedByID), encTextVal(evType), encInt(amtDelta))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 4. #TABLE:sales
	b.WriteString("#TABLE:sales\n")
	rows, err = tx.Query(ctx, `
		SELECT sale_id, store_id, origin_business_day, status, original_amount_minor, returned_amount_minor, revision
		FROM sales
		ORDER BY sale_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var saleID uuid.UUID
		var storeID *string
		var origDay *time.Time
		var status string
		var origAmt, retAmt, rev int64
		if err := rows.Scan(&saleID, &storeID, &origDay, &status, &origAmt, &retAmt, &rev); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encUUID(&saleID), encText(storeID), encDate(origDay), encTextVal(status), encInt(origAmt), encInt(retAmt), encInt(rev))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 5. #TABLE:parked_events
	b.WriteString("#TABLE:parked_events\n")
	rows, err = tx.Query(ctx, `
		SELECT event_id, parent_sale_id, status, resolved_at_utc
		FROM parked_events
		ORDER BY event_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var evID, parentID uuid.UUID
		var status string
		var resolvedAt *time.Time
		if err := rows.Scan(&evID, &parentID, &status, &resolvedAt); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encUUID(&evID), encUUID(&parentID), encTextVal(status), encTime(resolvedAt))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 6. #TABLE:event_application_status
	b.WriteString("#TABLE:event_application_status\n")
	rows, err = tx.Query(ctx, `
		SELECT event_id, status, rejection_reason
		FROM event_application_status
		ORDER BY event_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var evID uuid.UUID
		var status string
		var reason *string
		if err := rows.Scan(&evID, &status, &reason); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encUUID(&evID), encTextVal(status), encText(reason))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 7. #TABLE:gift_cards
	b.WriteString("#TABLE:gift_cards\n")
	rows, err = tx.Query(ctx, `
		SELECT card_id, balance_minor, status
		FROM gift_cards
		ORDER BY card_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var cardID, status string
		var bal int64
		if err := rows.Scan(&cardID, &bal, &status); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(cardID), encInt(bal), encTextVal(status))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 8. #TABLE:giftcard_overspend_flags
	b.WriteString("#TABLE:giftcard_overspend_flags\n")
	rows, err = tx.Query(ctx, `
		SELECT card_id, event_id, store_id, balance_after_minor, overspend_delta_minor
		FROM giftcard_overspend_flags
		ORDER BY card_id ASC, event_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var cardID, storeID string
		var evID uuid.UUID
		var balAfter, delta int64
		if err := rows.Scan(&cardID, &evID, &storeID, &balAfter, &delta); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(cardID), encUUID(&evID), encTextVal(storeID), encInt(balAfter), encInt(delta))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 9. #TABLE:loyalty_accounts
	b.WriteString("#TABLE:loyalty_accounts\n")
	rows, err = tx.Query(ctx, `
		SELECT account_id, earned_total, burned_total, balance, status
		FROM loyalty_accounts
		ORDER BY account_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var accID, status string
		var earned, burned, bal int64
		if err := rows.Scan(&accID, &earned, &burned, &bal, &status); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(accID), encInt(earned), encInt(burned), encInt(bal), encTextVal(status))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	// 10. #TABLE:loyalty_negative_flags
	b.WriteString("#TABLE:loyalty_negative_flags\n")
	rows, err = tx.Query(ctx, `
		SELECT account_id, trigger_event_id, deficit_points, resolved_by_event_id
		FROM loyalty_negative_flags
		ORDER BY account_id ASC, trigger_event_id ASC`)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var accID string
		var trigID uuid.UUID
		var deficit int64
		var resolvedID *uuid.UUID
		if err := rows.Scan(&accID, &trigID, &deficit, &resolvedID); err != nil {
			rows.Close()
			return "", err
		}
		writeRow(&b, encTextVal(accID), encUUID(&trigID), encInt(deficit), encUUID(resolvedID))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	rows.Close()

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

type Handler struct {
	Pools *db.Pools
}

func (h *Handler) GetChecksum(w http.ResponseWriter, r *http.Request) {
	var res Result
	err := db.WithReadOnlyTx(r.Context(), h.Pools.App, func(tx pgx.Tx) error {
		var err error
		res, err = ComputeChecksums(r.Context(), tx)
		return err
	})
	if err != nil {
		ledger.WriteError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	ledger.WriteJSON(w, http.StatusOK, res)
}
