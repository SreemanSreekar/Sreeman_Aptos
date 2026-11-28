# Rebuild Preserves Seeded Loyalty Accounts

## Issue summary

The integrated full-rebuild and snapshot-rebuild checks diverged after earlier tests seeded a loyalty account that had no corresponding ledger events. The live read model included that zero-balance account, but `admin_reset_read_models()` truncated it, so a full replay could not recreate it from `ledger_events`.

## Codebase location

- `migrations/001_init.sql:1239`, `admin_reset_read_models()`
- `migrations/001_init.sql:1298`, `admin_restore_snapshot(BIGINT)`
- `harness/test_idempotency_and_reclassification.py:211`, `test_reversed_batch_and_separate_tx_late_void_rebuilds`

## Debugging process

The issue reproduced in the clean integrated suite: live and rebuilt `read_model_sha256` values differed in both the reclassification/rebuild test and the concurrent snapshot/rebuild test. The regression test now seeds an unused loyalty account before capturing the live checksum. The existing reset function truncated `loyalty_accounts`, although admin-seeded accounts are not represented by ledger events.

## Resolution

`admin_reset_read_models()` now resets loyalty projection values to their initial active/zero state without deleting account rows. Snapshot restoration upserts snapshot account values, so it remains correct when reset preserves existing account rows. The regression test asserts full-rebuild and snapshot-tail checksum parity and verifies that the untouched seeded account remains readable with balance zero.

## Verification

- Full non-chaos integration harness on a fresh isolated database: `33 passed`.
- 100K chaos replay on a separate fresh isolated database: `1 passed`.
- Go regression in Linux container: `go test ./...` passed all packages.
