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

	"github.com/SreemanSreekar/Sreeman_Aptos/internal/admin"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/checksum"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/config"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/days"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/db"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/epochs"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/giftcards"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ingest"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/ledger"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/loyalty"
	"github.com/SreemanSreekar/Sreeman_Aptos/internal/sales"
)

// compositeProjector dispatches event-by-event projections across all event domains.
type compositeProjector struct{}

func (compositeProjector) ProjectSaleEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	return sales.ProjectSaleEvent(ctx, tx, e)
}

func (compositeProjector) ProjectDayCloseEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	return days.ProjectDayCloseEvent(ctx, tx, e)
}

func (compositeProjector) ProjectGiftCardEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	return giftcards.ProjectGiftCardEvent(ctx, tx, e)
}

func (compositeProjector) ProjectLoyaltyEvent(ctx context.Context, tx pgx.Tx, e ledger.EventRecord) error {
	return loyalty.ProjectLoyaltyEvent(ctx, tx, e)
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
	giftcardsHandler := &giftcards.Handler{Cfg: cfg, Pools: pools}
	loyaltyHandler := &loyalty.Handler{Cfg: cfg, Pools: pools}
	epochsHandler := &epochs.Handler{Cfg: cfg, Pools: pools, Projector: proj}
	checksumHandler := &checksum.Handler{Pools: pools}
	adminHandler := &admin.Handler{Cfg: cfg, Pools: pools, Projector: proj}

	handlers := ledger.RouteHandlers{
		IngestBatch:          ingestHandler.IngestBatch,
		CloseDay:             daysHandler.CloseDay,
		GetStoreTotal:        daysHandler.GetStoreTotals,
		OnlineGiftCardRedeem: giftcardsHandler.OnlineRedeem,
		GetGiftCard:          giftcardsHandler.GetGiftCard,
		OnlineLoyaltyBurn:    loyaltyHandler.OnlineBurn,
		GetLoyalty:           loyaltyHandler.GetLoyalty,
		TerminalSync:         epochsHandler.TerminalSync,
		TerminalFence:        epochsHandler.TerminalFence,
		GetChecksum:          checksumHandler.GetChecksum,
		AdminSeed:            adminHandler.Seed,
		AdminSnapshot:        adminHandler.CaptureSnapshot,
		AdminRebuild:         adminHandler.Rebuild,
	}

	mux := NewRouter(cfg, pools, handlers)

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
