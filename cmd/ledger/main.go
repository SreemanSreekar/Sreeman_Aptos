package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	pools, err := db.NewPools(ctx, cfg.DatabaseURL, cfg.AdminDatabaseURL)
	if err != nil {
		log.Fatalf("db init error: %v", err)
	}
	defer pools.Close()

	// Sync admin-owned ledger_config.allow_simulated_clock with deployment env.
	if _, err := pools.Admin.Exec(ctx,
		`UPDATE ledger_config SET allow_simulated_clock = $1, updated_at = now() WHERE id = 1`,
		cfg.AllowSimulatedClock,
	); err != nil {
		log.Fatalf("sync ledger_config.allow_simulated_clock: %v", err)
	}

	handlers := ledger.RouteHandlers{
		IngestBatch:          ledger.NotImplementedHandler("POST /v1/batches"),
		CloseDay:             ledger.NotImplementedHandler("POST /v1/stores/{id}/days/{day}/close"),
		GetStoreTotal:        ledger.NotImplementedHandler("GET /v1/stores/{id}/totals"),
		OnlineGiftCardRedeem: ledger.NotImplementedHandler("POST /v1/giftcards/redeem"),
		GetGiftCard:          ledger.NotImplementedHandler("GET /v1/giftcards/{id}"),
		OnlineLoyaltyBurn:    ledger.NotImplementedHandler("POST /v1/loyalty/burn"),
		GetLoyalty:           ledger.NotImplementedHandler("GET /v1/loyalty/{id}"),
		TerminalSync:         ledger.NotImplementedHandler("POST /v1/stores/{id}/terminals/{tid}/sync"),
		TerminalFence:        ledger.NotImplementedHandler("POST /v1/stores/{id}/terminals/{tid}/fence"),
		GetChecksum:          ledger.NotImplementedHandler("GET /v1/ledger/checksum"),
		AdminSeed:            ledger.NotImplementedHandler("POST /v1/admin/seed"),
		AdminSnapshot:        ledger.NotImplementedHandler("POST /v1/admin/snapshots"),
		AdminRebuild:         ledger.NotImplementedHandler("POST /v1/admin/rebuild"),
	}

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
	mux.HandleFunc("POST /v1/batches", handlers.IngestBatch)
	mux.HandleFunc("POST /v1/stores/{id}/days/{day}/close", handlers.CloseDay)
	mux.HandleFunc("GET /v1/stores/{id}/totals", handlers.GetStoreTotal)

	// Person 2 routes
	mux.HandleFunc("POST /v1/giftcards/redeem", handlers.OnlineGiftCardRedeem)
	mux.HandleFunc("GET /v1/giftcards/{id}", handlers.GetGiftCard)
	mux.HandleFunc("POST /v1/loyalty/burn", handlers.OnlineLoyaltyBurn)
	mux.HandleFunc("GET /v1/loyalty/{id}", handlers.GetLoyalty)
	mux.HandleFunc("POST /v1/stores/{id}/terminals/{tid}/sync", handlers.TerminalSync)
	mux.HandleFunc("POST /v1/stores/{id}/terminals/{tid}/fence", handlers.TerminalFence)
	mux.HandleFunc("GET /v1/ledger/checksum", handlers.GetChecksum)
	mux.HandleFunc("POST /v1/admin/seed", handlers.AdminSeed)
	mux.HandleFunc("POST /v1/admin/snapshots", handlers.AdminSnapshot)
	mux.HandleFunc("POST /v1/admin/rebuild", handlers.AdminRebuild)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Printf("[%s] ledger API listening on :%s", cfg.ReplicaID, cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}
