# Offline-First POS Distributed Ledger (`Sreeman_Aptos`)

Production-clean distributed ledger and deterministic POS offline ruleset for a 300-store, 2,400-terminal grocery chain enduring up to 6-hour connectivity outages, out-of-order/duplicated batches (up to 48h late), immutable `02:00` store-local business day closes, bounded offline gift-card spend (\$125 chain-wide hard upper bound), convergent loyalty accounting, and 10M-event consistent-watermark incremental snapshotting.

## Locked Specifications (Read Before Coding)

Both Phase 1 and Phase 2 specs are **locked (v10)** and committed in `docs/`:
- **Phase 1 (v10) — Architecture & Trade-Off Specification**: [`docs/phase1_architecture_spec.md`](docs/phase1_architecture_spec.md)
- **Phase 2 (v10) — Data Schemas, Triggers & State Machines**: [`docs/phase2_schemas_and_state_machines.md`](docs/phase2_schemas_and_state_machines.md)

## Stack

- **Load Balancer**: `nginx:1.27-alpine` (`least_conn`, keepalive) → `api-1:8080`, `api-2:8080`
- **API / Ledger (x2 stateless replicas)**: Go 1.23 (`net/http`, `pgx/v5`, embedded `tzdata`)
- **Database**: `postgres:16-alpine` (dual roles: `ledger_app` + `ledger_admin`)
- **Verification Harness**: Python 3.12 (`httpx`, `asyncio`, `psycopg`) in isolated `harness/` directory

---

## Team Work Division (3-Way Parallel Split)

To avoid merge conflicts, each owner has strictly disjoint directories and a dedicated feature branch.

| Role | Branch | Owned Directories & Files |
| :--- | :--- | :--- |
| **Person 1 (Infra, SQL & Core Ledger)** | `feat/core-ledger` | `docker-compose.yml`, `nginx/`, `Dockerfile`, `migrations/`, `go.mod`, `go.sum`, `cmd/ledger/`, `internal/db/`, `internal/clock/`, `internal/canonical/`, `internal/ingest/`, `internal/sales/`, `internal/days/` |
| **Person 2 (Gift Cards, Loyalty, Epochs, Snapshots & `pos_ruleset`)** | `feat/cards-loyalty-admin` | `internal/giftcards/`, `internal/loyalty/`, `internal/epochs/`, `internal/checksum/`, `internal/admin/`, `pos_ruleset/` |
| **Person 3 (Standalone `harness/`, Oracle & Docs)** | `feat/harness-docs` | `harness/`, `DESIGN.md` |

---

### Person 1 — Infra, SQL Migrations & Core Event/Sale/Day-Close Ledger (Go + SQL)

**Deliverables:**
1. **Docker Compose & Edge Infra**:
   - `docker-compose.yml` running `postgres:16-alpine`, `api-1`, `api-2` (both stateless Go replicas using `ledger_app` + `ledger_admin` DSNs), and `nginx:1.27-alpine` on `:8080`.
   - Multi-stage `Dockerfile` (`golang:1.23-alpine` → `alpine:3.20` with embedded `tzdata`).
   - `nginx/nginx.conf` with `least_conn`, passive health checks, and upstream keepalive.
2. **Database Migration (`migrations/001_init.sql`)**:
   - Exact Phase 2 (v10) DDL: `ledger_admin` & `ledger_app` roles/grants, `ledger_config`, `stores`, `store_terminals`, `ledger_events` (append-only trigger, `PRIMARY KEY (event_id)`, normalized `sale_id`/`card_id`/`account_id` prefix indexes, `day_close` `UUIDv5` + `payload_hash` checks), all 10 projection tables, closed-day `OLD`+`NEW` `FOR SHARE` immutability triggers, `store_offline_epochs`/`store_epoch_history`/`store_epoch_roster`/`terminal_offline_slices` + deferred roster-count triggers, all 10 `snapshot_*` tables, and all `SECURITY DEFINER` procedures (`admin_seed_store`, `admin_seed_gift_card`, `admin_seed_loyalty_account`, `admin_reset_read_models`, `admin_capture_snapshot`, `admin_restore_snapshot`, `reconcile_terminal_slice`, `fence_terminal_slice`, `advance_store_epoch`).
3. **Go Foundation & Core Ledger**:
   - Server entrypoint (`cmd/ledger/main.go`), `pgx/v5` pools (`ledger_app` & `ledger_admin`), `X-Simulated-Now` clock helper (gated by `ALLOW_SIMULATED_CLOCK`), IANA `02:00` business-day & DST cutoff helper, canonical `payload_hash` (SHA-256) helper, and the `Projector` dispatch interface so Person 2 plugs in cleanly.
   - `GET /healthz`.
   - `POST /v1/batches`: reject client-supplied `channel` (`400 CLIENT_CHANNEL_FORBIDDEN`, force `'OFFLINE'`), validate clock skew (`+5m`) & lateness (`48h`), live-only check `terminal_offline_slices.state IN ('ACTIVE', 'FENCED')`, topological pre-sort (`sale` → `return`/`void` → `giftcard_redeem`/`loyalty_earn`/`loyalty_burn`), claim-first `online_action_receipts` (`PENDING` → finalize after projection), canonical lock ordering + `40P01`/`40001` retry loop, post-lock `applied_order`, and event-by-event `projectEvent`.
   - `internal/sales`: unified `PLACEHOLDER` parent-key locking, `parked_events` parking & `resolved_at_utc = parent_sale.ingested_at` resolution, prefix-scoped (`applied_order <= e.applied_order`) per-sale recomputation, duplicate `sale_id` (`DUPLICATE_SALE_ID`), and `RECLASSIFICATION` delta booking (`revision`, `caused_by_event_id`).
   - `internal/days`: `POST /v1/stores/{id}/days/{day}/close` (reject `< cutoff_utc` with `409 BUSINESS_DAY_CUTOFF_NOT_REACHED`, deterministic `UUIDv5` `day_close` event, idempotent retries), lazy close emitting the same `day_close` event, and `GET /v1/stores/{id}/totals?day=YYYY-MM-DD` (`REPEATABLE READ READ ONLY`).

---

### Person 2 — Gift Cards, Loyalty, Offline Epochs, Snapshots & `pos_ruleset` (Go)

**Deliverables:**
1. **Online Gift-Card & Loyalty Endpoints**:
   - `POST /v1/giftcards/redeem` and `POST /v1/loyalty/burn` (force `channel = 'ONLINE'`, `amount_minor > 0` / `points > 0`, claim-first `online_action_receipts`, explicit `ON CONFLICT (event_id) DO NOTHING RETURNING event_id` online-vs-batch race branch finalizing the receipt without double-debiting).
   - `GET /v1/giftcards/{id}` and `GET /v1/loyalty/{id}` (`REPEATABLE READ READ ONLY`).
2. **Gift-Card & Loyalty Projectors (`projectEvent` hooks)**:
   - Prefix-scoped (`applied_order <= e.applied_order`) canonical `(occurred_at ASC, event_id ASC)` recomputation for `gift_cards` + `giftcard_overspend_flags` (validating `store_id = ANY(eligible_stores)` and per-`(card_id, store_id, epoch_gen)` cap of `2500` minor units for `OFFLINE` events).
   - Prefix-scoped canonical recomputation for `loyalty_accounts` (PN-Counter CRDT) + `loyalty_negative_flags` (enforcing store/epoch total `5000`-point cap when `loyalty_offline_store_burn_cap > 0`).
3. **Offline Epoch Sync & Fencing**:
   - `POST /v1/stores/{id}/terminals/{tid}/sync` (single ACID tx: ingest buffered events with contiguous `terminal_epoch_seq`, call `reconcile_terminal_slice(...)`, call `advance_store_epoch(...)`, return `LocalSliceGrant` + `ACTIVE` card snapshot or `BLOCKED_PENDING_SIBLINGS`).
   - `POST /v1/stores/{id}/terminals/{tid}/fence` (call DB-anchored `fence_terminal_slice(...)`).
4. **Standalone `pos_ruleset/` Offline Module**:
   - Zero-dependency Go package implementing `LocalSliceGrant`, `CLOCK_BOOTTIME` TTL expiry (6h), `boot_id` offline-reboot fail-closed guard, `hwm_utc` backward clock-rollback detection, 5-store eligibility cluster check, per-card `slice_minor` cap, and store/epoch `total_loyalty_burned_points` check (`nil` = unlimited).
5. **Checksums, Seeding, Snapshots & Deterministic Rebuilds**:
   - `GET /v1/ledger/checksum` (`REPEATABLE READ READ ONLY`) implementing exact Phase 1 (v10) `economic_sha256` and `read_model_sha256` column lists and encodings.
   - `POST /v1/admin/seed` (calls `admin_seed_store`, `admin_seed_gift_card`, `admin_seed_loyalty_account`).
   - `POST /v1/admin/snapshots` (Reader Tx `REPEATABLE READ READ ONLY` + `pg_export_snapshot()`, Writer Tx `REPEATABLE READ` + `SET TRANSACTION SNAPSHOT` + `admin_capture_snapshot()`).
   - `POST /v1/admin/rebuild?mode=full|snapshot` (`admin_reset_read_models()` / `admin_restore_snapshot()` + prefix-scoped `ORDER BY applied_order ASC` replay).

---

### Person 3 — Standalone `harness/` (Python 3.12), Reference Oracle & Docs

**Deliverables:**
1. **Independent Reference Oracle & Offline Simulator (`harness/`)**:
   - Completely separate from Go application code (zero shared imports).
   - Independent Python implementation of the `economic_sha256` reference oracle (in-order evaluation) and offline terminal simulator mirroring `pos_ruleset` (including `terminal_epoch_seq`, 5-store eligibility, multi-terminal slices, and outage windows).
2. **All 7 Verification Suites (Phase 1 §5 Matrix)**:
   - `harness/test_100k_chaos_replay.py`: 100,000 unique events + 12% duplicates (112,000 sent), shuffled within 48h, 3% orphan returns/voids, 20 store outages across both replicas → bit-for-bit `economic_sha256` match vs. Python oracle; 0 unresolved valid orphans.
   - `harness/test_idempotency_and_reclassification.py`: `409 EVENT_ID_PAYLOAD_MISMATCH`, `DUPLICATE_SALE_ID`, reversed single batch `[return(10:05), void(10:02)]`, separate-tx late-void (`Tx1 sale`, `Tx2 return(10:05)`, `Tx3 void(10:02)`) live == full rebuild == snapshot+tail rebuild, and concurrent batch + online action race on same `event_id`.
   - `harness/test_business_day_immutability.py`: `X-Simulated-Now` early close (`409 BUSINESS_DAY_CUTOFF_NOT_REACHED`), idempotent close retries (1 `day_close` event), Monday 10:00 orphan / Tuesday 09:00 sale adjustment routing, DST spring-forward/fall-back, `ledger_app` closed-day SQL mutation rejected by trigger, empty closed-day rebuild.
   - `harness/test_concurrent_reads_50rps.py`: Sustained 50 RPS concurrent reads during multi-batch ingest → 0 partial batch observations.
   - `harness/test_giftcard_overspend_and_online.py`: 40 gift cards redeemed across 2–5 eligible offline stores (multi-terminal) → overspend $\le \$125.00$ & 100% flagged/frozen; 6th disconnected store rejected (`OFFLINE_STORE_NOT_ELIGIBLE`); 2-replica concurrent sync/renewal + `FENCED -> RECONCILED` + multi-epoch unreconciled liability reservation; concurrent online redeems (0 overspend, 0 double-debit).
   - `harness/test_loyalty_convergence.py`: Opposite-permutation offline earn/burn convergence, claim-first online burn, `NEGATIVE_RESTRICTED` online block & earn-back recovery.
   - `harness/test_rebuild_and_snapshot.py`: Capture snapshot *during* live 2-replica writes → verify `live read_model_sha256 == full rebuild read_model_sha256 == snapshot + tail rebuild read_model_sha256` bit-for-bit.
3. **Documentation**:
   - `DESIGN.md` (concise 1–2 page architecture & trade-off summary distilled from Phase 1/2 v10).
   - Finalize `README.md` with verified run/test commands and benchmark results.

---

## Parallelization & Merge Order

1. **Step 0 (Person 1 Foundation PR → `main`)**: Merge `docker-compose.yml`, `migrations/001_init.sql`, `go.mod`, server skeleton, shared helpers, and the `Projector` interface stub.
2. **Parallel Feature Development**: All 3 owners build concurrently on `feat/core-ledger`, `feat/cards-loyalty-admin`, and `feat/harness-docs`.
3. **Integration & Verification**: Merge Person 1 + Person 2 into `main`, run `docker compose up --build`, and execute Person 3's 7-suite `harness/` against `http://localhost:8080`.
