package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/days"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ingest"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/sales"
)

// compositeProjector wires Person 1's sales & day_close projectors with Person 2's giftcard & loyalty hooks.
type compositeProjector struct{}

func (compositeProjector) ProjectSaleEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	return sales.ProjectSaleEvent(ctx, tx, e)
}

func (compositeProjector) ProjectDayCloseEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	return days.ProjectDayCloseEvent(ctx, tx, e)
}

// Person 2 replaces these two stub methods with internal/giftcards and internal/loyalty projectors on merge.
func (compositeProjector) ProjectGiftCardEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_application_status (event_id, status, applied_amount_minor)
		VALUES ($1, 'APPLIED', 0)
		ON CONFLICT (event_id) DO NOTHING`, e.EventID)
	return err
}

func (compositeProjector) ProjectLoyaltyEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO event_application_status (event_id, status, applied_amount_minor)
		VALUES ($1, 'APPLIED', 0)
		ON CONFLICT (event_id) DO NOTHING`, e.EventID)
	return err
}

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

	if _, err := pools.Admin.Exec(ctx,
		`UPDATE ledger_config SET allow_simulated_clock = $1, updated_at = now() WHERE id = 1`,
		cfg.AllowSimulatedClock,
	); err != nil {
		log.Fatalf("sync ledger_config.allow_simulated_clock: %v", err)
	}

	proj := compositeProjector{}
	daysHandler := &days.Handler{Cfg: cfg, Pools: pools}
	ingestHandler := &ingest.Handler{Cfg: cfg, Pools: pools, Projector: proj}

	handlers := ledger.RouteHandlers{
		// Person 1 (live)
		IngestBatch:   ingestHandler.IngestBatch,
		CloseDay:      daysHandler.CloseDay,
		GetStoreTotal: daysHandler.GetStoreTotals,

		// Person 2 (wired on merge of branch 2)
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
