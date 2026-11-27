package db

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pools struct {
	App   *pgxpool.Pool // ledger_app runtime role
	Admin *pgxpool.Pool // ledger_admin privileged role
}

func NewPools(ctx context.Context, appURL, adminURL string) (*Pools, error) {
	appCfg, err := pgxpool.ParseConfig(appURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	appCfg.MaxConns = 32
	appCfg.MinConns = 4

	appPool, err := pgxpool.NewWithConfig(ctx, appCfg)
	if err != nil {
		return nil, fmt.Errorf("connect ledger_app pool: %w", err)
	}
	if err := appPool.Ping(ctx); err != nil {
		appPool.Close()
		return nil, fmt.Errorf("ping ledger_app pool: %w", err)
	}

	adminCfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		appPool.Close()
		return nil, fmt.Errorf("parse ADMIN_DATABASE_URL: %w", err)
	}
	adminCfg.MaxConns = 8
	adminCfg.MinConns = 1

	adminPool, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		appPool.Close()
		return nil, fmt.Errorf("connect ledger_admin pool: %w", err)
	}
	if err := adminPool.Ping(ctx); err != nil {
		appPool.Close()
		adminPool.Close()
		return nil, fmt.Errorf("ping ledger_admin pool: %w", err)
	}

	return &Pools{App: appPool, Admin: adminPool}, nil
}

func (p *Pools) Close() {
	if p.App != nil {
		p.App.Close()
	}
	if p.Admin != nil {
		p.Admin.Close()
	}
}

// WithTxRetry executes fn inside a READ COMMITTED transaction, retrying up to 5 times
// with exponential backoff + jitter on PostgreSQL deadlock (40P01) or serialization failure (40001).
func WithTxRetry(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	const maxAttempts = 5
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			return err
		}
		err = fn(tx)
		if err == nil {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				if isRetryablePgErr(commitErr) && attempt < maxAttempts {
					lastErr = commitErr
					sleepBackoff(ctx, attempt)
					continue
				}
				return commitErr
			}
			return nil
		}
		_ = tx.Rollback(ctx)
		if isRetryablePgErr(err) && attempt < maxAttempts {
			lastErr = err
			sleepBackoff(ctx, attempt)
			continue
		}
		return err
	}
	return lastErr
}

// WithReadOnlyTx executes fn inside a REPEATABLE READ READ ONLY transaction (50 RPS read isolation).
func WithReadOnlyTx(ctx context.Context, pool *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func isRetryablePgErr(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40P01" || pgErr.Code == "40001"
	}
	return false
}

func sleepBackoff(ctx context.Context, attempt int) {
	base := time.Duration(10*(1<<(attempt-1))) * time.Millisecond
	jitter := time.Duration(rand.IntN(15)) * time.Millisecond
	select {
	case <-time.After(base + jitter):
	case <-ctx.Done():
	}
}
