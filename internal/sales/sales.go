package sales

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/clock"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/days"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

type prefixSaleEvent struct {
	EventID           uuid.UUID
	AppliedOrder      int64
	EventType         string
	StoreID           string
	OccurredAt        time.Time
	IngestedAt        time.Time
	OriginBusinessDay time.Time
	AmountMinor       int64
}

type evalResult struct {
	ev          prefixSaleEvent
	newStatus   string // APPLIED or REJECTED_INVALID
	rejectCode  *string
	signedDelta int64 // +amt for sale, -amt for return/void if APPLIED, else 0
}

// evaluateSalePrefix runs the pure, arrival-order-independent per-sale state machine
// over events sorted in canonical (occurred_at ASC, event_id ASC) order.
func evaluateSalePrefix(events []prefixSaleEvent, primarySale *prefixSaleEvent) (string, int64, []evalResult) {
	saleStatus := "ACTIVE"
	origAmount := primarySale.AmountMinor
	var returnedAmount int64
	saleStore := primarySale.StoreID

	results := make([]evalResult, 0, len(events))
	seenPrimarySale := false

	for _, ev := range events {
		switch ev.EventType {
		case "sale":
			if !seenPrimarySale && ev.EventID == primarySale.EventID {
				seenPrimarySale = true
				results = append(results, evalResult{
					ev:          ev,
					newStatus:   "APPLIED",
					signedDelta: ev.AmountMinor,
				})
			} else {
				reason := "DUPLICATE_SALE_ID"
				results = append(results, evalResult{
					ev:          ev,
					newStatus:   "REJECTED_INVALID",
					rejectCode:  &reason,
					signedDelta: 0,
				})
			}

		case "return":
			switch saleStatus {
			case "VOIDED":
				reason := "CANNOT_RETURN_VOIDED_SALE"
				results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
			case "FULLY_RETURNED":
				reason := "ALREADY_FULLY_RETURNED"
				results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
			case "ACTIVE", "PARTIALLY_RETURNED":
				if ev.AmountMinor <= 0 || returnedAmount+ev.AmountMinor > origAmount {
					reason := "RETURN_EXCEEDS_REMAINING"
					results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
				} else {
					returnedAmount += ev.AmountMinor
					if returnedAmount == origAmount {
						saleStatus = "FULLY_RETURNED"
					} else {
						saleStatus = "PARTIALLY_RETURNED"
					}
					results = append(results, evalResult{
						ev:          ev,
						newStatus:   "APPLIED",
						signedDelta: -ev.AmountMinor,
					})
				}
			}

		case "void":
			switch saleStatus {
			case "VOIDED":
				reason := "ALREADY_VOIDED"
				results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
			case "PARTIALLY_RETURNED", "FULLY_RETURNED":
				reason := "CANNOT_VOID_RETURNED_SALE"
				results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
			case "ACTIVE":
				if ev.StoreID != saleStore {
					reason := "VOID_STORE_MISMATCH"
					results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
				} else if ev.AmountMinor != origAmount {
					reason := "INVALID_AMOUNT"
					results = append(results, evalResult{ev: ev, newStatus: "REJECTED_INVALID", rejectCode: &reason})
				} else {
					saleStatus = "VOIDED"
					results = append(results, evalResult{
						ev:          ev,
						newStatus:   "APPLIED",
						signedDelta: -ev.AmountMinor,
					})
				}
			}
		}
	}

	return saleStatus, returnedAmount, results
}

type priorAppState struct {
	Exists         bool
	Status         string
	AppliedAmount  int64
	AppliedStoreID *string
	AppliedPostDay *time.Time
	AppliedIsAdj   bool
	ResolvedAtUTC  *time.Time
}

// ProjectSaleEvent implements the prefix-scoped (applied_order <= e.AppliedOrder),
// arrival-order-independent per-sale state machine, orphan parking/resolution (resolved_at_utc),
// and deterministic RECLASSIFICATION delta accounting.
func ProjectSaleEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	if e.SaleID == nil {
		return fmt.Errorf("missing sale_id for event %s", e.EventID)
	}
	saleID := *e.SaleID

	// 1. Unified parent-key PLACEHOLDER stub + FOR UPDATE lock (eliminates missing-key race across 2 replicas)
	if _, err := tx.Exec(ctx, `
		INSERT INTO sales (sale_id, status)
		VALUES ($1, 'PLACEHOLDER')
		ON CONFLICT (sale_id) DO NOTHING`,
		saleID,
	); err != nil {
		return err
	}

	var currentRevision int
	if err := tx.QueryRow(ctx, `
		SELECT revision FROM sales WHERE sale_id = $1 FOR UPDATE`,
		saleID,
	).Scan(&currentRevision); err != nil {
		return err
	}

	// 2. Prefix-scoped canonical query over ALL events for sale_id (sale, return, void)
	rows, err := tx.Query(ctx, `
		SELECT event_id, applied_order, event_type, store_id, occurred_at, ingested_at,
		       origin_business_day, amount_minor
		FROM ledger_events
		WHERE sale_id = $1
		  AND applied_order <= $2
		ORDER BY occurred_at ASC, event_id ASC`,
		saleID, e.AppliedOrder,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var events []prefixSaleEvent
	var primarySale *prefixSaleEvent
	for rows.Next() {
		var ev prefixSaleEvent
		if err := rows.Scan(
			&ev.EventID, &ev.AppliedOrder, &ev.EventType, &ev.StoreID,
			&ev.OccurredAt, &ev.IngestedAt, &ev.OriginBusinessDay, &ev.AmountMinor,
		); err != nil {
			return err
		}
		events = append(events, ev)
		if ev.EventType == "sale" && primarySale == nil {
			copyEv := ev
			primarySale = &copyEv
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// 3. Orphan case: no parent sale in prefix yet -> park cleanly with zero totals impact.
	if primarySale == nil {
		if _, err := tx.Exec(ctx, `
			INSERT INTO parked_events (event_id, parent_sale_id, status)
			VALUES ($1, $2, 'PENDING_PARENT')
			ON CONFLICT (event_id) DO NOTHING`,
			e.EventID, saleID,
		); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO event_application_status (event_id, status, rejection_reason, applied_amount_minor)
			VALUES ($1, 'PARKED', NULL, 0)
			ON CONFLICT (event_id) DO UPDATE
			SET status = 'PARKED', rejection_reason = NULL, applied_amount_minor = 0`,
			e.EventID,
		)
		return err
	}

	// 4. Primary sale exists: if triggering event is the primary sale, resolve all pending parked orphans
	//    persisting resolved_at_utc = primarySale.IngestedAt (Monday orphan / Tuesday sale rule).
	if e.EventType == "sale" && e.EventID == primarySale.EventID {
		if _, err := tx.Exec(ctx, `
			UPDATE parked_events
			SET status = 'RESOLVED',
			    resolved_at_utc = $2,
			    resolved_by_sale_event_id = $3
			WHERE parent_sale_id = $1 AND status = 'PENDING_PARENT'`,
			saleID, primarySale.IngestedAt.UTC().Truncate(time.Microsecond), primarySale.EventID,
		); err != nil {
			return err
		}
	}

	// 5. Walk canonical (occurred_at ASC, event_id ASC) prefix through per-sale state machine.
	origAmount := primarySale.AmountMinor
	saleStore := primarySale.StoreID
	saleOriginDay := primarySale.OriginBusinessDay
	saleStatus, returnedAmount, results := evaluateSalePrefix(events, primarySale)

	// 6. Compare each event's desired effect against event_application_status and book
	//    initial postings or RECLASSIFICATION deltas without ever mutating closed days.
	reclassRevisionBumped := false
	locCache := make(map[string]*time.Location)
	getLoc := func(storeID string) (*time.Location, error) {
		if loc, ok := locCache[storeID]; ok {
			return loc, nil
		}
		loc, _, err := days.LoadStoreLocation(ctx, tx, storeID)
		if err != nil {
			return nil, err
		}
		locCache[storeID] = loc
		return loc, nil
	}

	for _, res := range results {
		prior, err := loadPriorAppState(ctx, tx, res.ev.EventID)
		if err != nil {
			return err
		}

		// Case 6a: Brand new event or previously PARKED orphan now being evaluated.
		if !prior.Exists || prior.Status == "PARKED" {
			var postDay *time.Time
			isAdj := false
			if res.newStatus == "APPLIED" {
				loc, err := getLoc(res.ev.StoreID)
				if err != nil {
					return err
				}
				// Resolved orphan uses resolved_at_utc (= parent_sale.ingested_at); normal event uses its own ingested_at.
				effectiveClock := res.ev.IngestedAt
				if prior.Status == "PARKED" && prior.ResolvedAtUTC != nil {
					effectiveClock = *prior.ResolvedAtUTC
				}
				pd, adj, err := days.ResolvePostingDay(ctx, tx, res.ev.StoreID, loc, res.ev.OriginBusinessDay, effectiveClock)
				if err != nil {
					return err
				}
				postDay = &pd
				isAdj = adj
				kind := "DIRECT"
				if isAdj {
					kind = "LATE_POSTING"
				}
				if err := days.ApplyTotalsDelta(
					ctx, tx, res.ev.StoreID, pd, res.ev.OriginBusinessDay,
					res.ev.EventID, e.EventID, res.ev.EventType, kind, 0, res.signedDelta, isAdj,
				); err != nil {
					return err
				}
			}

			if _, err := tx.Exec(ctx, `
				INSERT INTO event_application_status (
					event_id, status, rejection_reason,
					applied_amount_minor, applied_store_id, applied_posting_day, applied_is_adjustment
				) VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (event_id) DO UPDATE
				SET status = EXCLUDED.status,
				    rejection_reason = EXCLUDED.rejection_reason,
				    applied_amount_minor = EXCLUDED.applied_amount_minor,
				    applied_store_id = EXCLUDED.applied_store_id,
				    applied_posting_day = EXCLUDED.applied_posting_day,
				    applied_is_adjustment = EXCLUDED.applied_is_adjustment`,
				res.ev.EventID, res.newStatus, res.rejectCode,
				res.signedDelta, res.ev.StoreID, postDay, isAdj,
			); err != nil {
				return err
			}
			continue
		}

		// Case 6b: Previously evaluated event whose applied effect flipped -> RECLASSIFICATION adjustment!
		if prior.AppliedAmount != res.signedDelta {
			if !reclassRevisionBumped {
				currentRevision++
				reclassRevisionBumped = true
			}
			delta := res.signedDelta - prior.AppliedAmount
			loc, err := getLoc(res.ev.StoreID)
			if err != nil {
				return err
			}
			// Always post reclassification into the store's current open business day at triggering event's ingested_at.
			currDay := clock.BusinessDay(e.IngestedAt, loc)
			pd, _, err := days.ResolvePostingDay(ctx, tx, res.ev.StoreID, loc, currDay, e.IngestedAt)
			if err != nil {
				return err
			}
			if err := days.ApplyTotalsDelta(
				ctx, tx, res.ev.StoreID, pd, res.ev.OriginBusinessDay,
				res.ev.EventID, e.EventID, res.ev.EventType, "RECLASSIFICATION", currentRevision, delta, true,
			); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				UPDATE event_application_status
				SET status = $2,
				    rejection_reason = $3,
				    applied_amount_minor = $4
				WHERE event_id = $1`,
				res.ev.EventID, res.newStatus, res.rejectCode, res.signedDelta,
			); err != nil {
				return err
			}
		} else if prior.Status != res.newStatus {
			// Status/reason metadata update with zero delta.
			if _, err := tx.Exec(ctx, `
				UPDATE event_application_status
				SET status = $2, rejection_reason = $3
				WHERE event_id = $1`,
				res.ev.EventID, res.newStatus, res.rejectCode,
			); err != nil {
				return err
			}
		}
	}

	// 7. Persist updated canonical sales row.
	_, err = tx.Exec(ctx, `
		UPDATE sales
		SET store_id              = $2,
		    origin_business_day   = $3,
		    status                = $4,
		    original_amount_minor = $5,
		    returned_amount_minor = $6,
		    revision              = $7,
		    primary_event_id      = $8
		WHERE sale_id = $1`,
		saleID, saleStore, saleOriginDay, saleStatus, origAmount, returnedAmount, currentRevision, primarySale.EventID,
	)
	return err
}

func loadPriorAppState(ctx context.Context, tx pgx.Tx, eventID uuid.UUID) (priorAppState, error) {
	var st priorAppState
	err := tx.QueryRow(ctx, `
		SELECT eas.status, eas.applied_amount_minor, eas.applied_store_id,
		       eas.applied_posting_day, eas.applied_is_adjustment, pe.resolved_at_utc
		FROM event_application_status eas
		LEFT JOIN parked_events pe ON pe.event_id = eas.event_id
		WHERE eas.event_id = $1`,
		eventID,
	).Scan(&st.Status, &st.AppliedAmount, &st.AppliedStoreID, &st.AppliedPostDay, &st.AppliedIsAdj, &st.ResolvedAtUTC)
	if errors.Is(err, pgx.ErrNoRows) {
		return priorAppState{Exists: false}, nil
	}
	if err != nil {
		return priorAppState{}, err
	}
	st.Exists = true
	return st, nil
}
