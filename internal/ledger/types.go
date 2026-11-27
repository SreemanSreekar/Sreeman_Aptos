package ledger

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// EventRecord is the normalized, persisted ledger_events row passed to every projector.
type EventRecord struct {
	EventID           uuid.UUID
	AppliedOrder      int64
	EventType         string
	Channel           string
	EpochGen          *int64
	TerminalEpochSeq  *int32
	StoreID           string
	TerminalID        string
	OccurredAt        time.Time
	IngestedAt        time.Time
	OriginBusinessDay time.Time
	OriginCutoffUTC   time.Time
	TZVersion         string
	SaleID            *uuid.UUID
	ParentSaleID      *uuid.UUID
	CardID            *string
	AccountID         *string
	AmountMinor       *int64
	Points            *int64
	PayloadJSON       json.RawMessage
	PayloadHash       []byte
}

// Projector defines the shared event-by-event projection boundary between Person 1 (sales/days)
// and Person 2 (giftcards/loyalty). Both live ingest and full/snapshot rebuild call these
// identically in ORDER BY applied_order ASC with prefix visibility (applied_order <= e.AppliedOrder).
type Projector interface {
	ProjectSaleEvent(ctx context.Context, tx pgx.Tx, e EventRecord) error
	ProjectGiftCardEvent(ctx context.Context, tx pgx.Tx, e EventRecord) error
	ProjectLoyaltyEvent(ctx context.Context, tx pgx.Tx, e EventRecord) error
	ProjectDayCloseEvent(ctx context.Context, tx pgx.Tx, e EventRecord) error
}

// RouteHandlers allows Person 1 and Person 2 to register HTTP handlers cleanly without touching main.go.
type RouteHandlers struct {
	// Person 1
	IngestBatch   http.HandlerFunc
	CloseDay      http.HandlerFunc
	GetStoreTotal http.HandlerFunc

	// Person 2
	OnlineGiftCardRedeem http.HandlerFunc
	GetGiftCard          http.HandlerFunc
	OnlineLoyaltyBurn    http.HandlerFunc
	GetLoyalty           http.HandlerFunc
	TerminalSync         http.HandlerFunc
	TerminalFence        http.HandlerFunc
	GetChecksum          http.HandlerFunc
	AdminSeed            http.HandlerFunc
	AdminSnapshot        http.HandlerFunc
	AdminRebuild         http.HandlerFunc
}

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func WriteError(w http.ResponseWriter, status int, code, msg string) {
	WriteJSON(w, status, map[string]string{
		"error":   code,
		"message": msg,
	})
}

func NotImplementedHandler(name string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", name+" is not wired yet")
	}
}
