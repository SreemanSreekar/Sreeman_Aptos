package main

import (
	"net/http"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

// NewRouter constructs the HTTP mux from injected RouteHandlers.
// Person 2 only plugs their concrete handler funcs into RouteHandlers in main.go
// without touching route paths or Person 1's handlers.
func NewRouter(cfg *config.Config, pools *db.Pools, h ledger.RouteHandlers) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pools.App.Ping(r.Context()); err != nil {
			ledger.WriteError(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE", err.Error())
			return
		}
		ledger.WriteJSON(w, http.StatusOK, map[string]any{
			"status":     "ok",
			"replica_id": cfg.ReplicaID,
		})
	})

	// Person 1 routes
	mux.HandleFunc("POST /v1/batches", h.IngestBatch)
	mux.HandleFunc("POST /v1/stores/{id}/days/{day}/close", h.CloseDay)
	mux.HandleFunc("GET /v1/stores/{id}/totals", h.GetStoreTotal)

	// Person 2 routes
	mux.HandleFunc("POST /v1/giftcards/redeem", h.OnlineGiftCardRedeem)
	mux.HandleFunc("GET /v1/giftcards/{id}", h.GetGiftCard)
	mux.HandleFunc("POST /v1/loyalty/burn", h.OnlineLoyaltyBurn)
	mux.HandleFunc("GET /v1/loyalty/{id}", h.GetLoyalty)
	mux.HandleFunc("POST /v1/stores/{id}/terminals/{tid}/sync", h.TerminalSync)
	mux.HandleFunc("POST /v1/stores/{id}/terminals/{tid}/fence", h.TerminalFence)
	mux.HandleFunc("GET /v1/ledger/checksum", h.GetChecksum)
	mux.HandleFunc("POST /v1/admin/seed", h.AdminSeed)
	mux.HandleFunc("POST /v1/admin/snapshots", h.AdminSnapshot)
	mux.HandleFunc("POST /v1/admin/rebuild", h.AdminRebuild)

	return mux
}
