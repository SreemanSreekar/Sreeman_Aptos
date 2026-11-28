# Offline-First POS Distributed Ledger (`Sreeman_Aptos`)

An offline-first POS distributed ledger and deterministic terminal ruleset for a 300-store, 2,400-terminal grocery chain enduring up to 6-hour connectivity outages. Supports idempotent at-least-once batch ingestion across two stateless replicas, out-of-order and up-to-48h-late event resolution, immutable `02:00` store-local business day closes with linked adjustments, bounded offline gift-card exposure (\$125 chain-wide hard limit) with exact online redemption, convergent PN-Counter loyalty balances with a negative-balance policy, and consistent-watermark snapshots for fast rebuilds at 10 million events.

## Stack & Repository Layout

- **Load Balancer**: `nginx:1.27-alpine` (`least_conn`, keepalive) → `api-1:8080`, `api-2:8080`
- **API / Ledger (2 stateless replicas)**: Go 1.23 (`net/http`, `pgx/v5`, embedded `tzdata`) in `cmd/ledger/` and `internal/`
- **Standalone Offline Terminal Ruleset**: Zero-dependency Go package in `pos_ruleset/`
- **Database**: `postgres:16-alpine` with dual role isolation (`ledger_app` runtime role + `ledger_admin` owner role) and SQL immutability triggers in `migrations/`
- **Standalone Verification Harness**: Pure Python 3.12 (`httpx`, `asyncio`, `psycopg`, `pytest`) in `harness/` (zero application imports)
- **Design Documentation**:
  - Concise 1–2 page design summary: [`docs/design-submission.md`](docs/design-submission.md)
  - Detailed architecture & trade-offs: [`DESIGN.md`](DESIGN.md)
  - Locked specs: [`docs/phase1_architecture_spec.md`](docs/phase1_architecture_spec.md), [`docs/phase2_schemas_and_state_machines.md`](docs/phase2_schemas_and_state_machines.md)

---

## How to Run

### 1. Start the Application Stack (`docker compose up`)

```bash
docker compose up -d --build
curl -s http://localhost:8080/healthz
```

This starts PostgreSQL 16 (host port `5433`), two stateless Go API replicas (`api-1`, `api-2`), and Nginx (`http://localhost:8080`).

> **Note:** `ALLOW_SIMULATED_CLOCK` defaults to `false` in `docker-compose.yml` for production safety. To run simulated-clock tests (`X-Simulated-Now`), pass `ALLOW_SIMULATED_CLOCK=true` or use the isolated test Compose file below.

### 2. Run Go Unit & Race Tests

```bash
# With Docker (no local Go installation required)
docker run --rm -v "$(pwd)":/src -w /src golang:1.23-alpine go test -v -race ./...

# Or with local Go 1.23+
go test -v -race ./...
```

### 3. Run the Standalone Verification Harness (`harness/`)

Use the isolated test Compose stack (`docker-compose.test.yml`), which maps Nginx to `:8081`, direct API replicas to `:8082` / `:8083`, PostgreSQL to `:5434`, and enables `ALLOW_SIMULATED_CLOCK=true`.

#### Linux / macOS

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# 1. Start a fresh isolated test stack
docker compose -p sreeman-acceptance -f docker-compose.test.yml down -v
docker compose -p sreeman-acceptance -f docker-compose.test.yml up -d --build

# 2. Run Suites 2-7 + unit tests (34 tests) and core API suites (18 tests)
export LEDGER_INTEGRATION=1 LEDGER_P1_MERGED=1 LEDGER_P2_MERGED=1
export LEDGER_BASE_URL=http://localhost:8081
export LEDGER_REPLICA1_URL=http://localhost:8082
export LEDGER_REPLICA2_URL=http://localhost:8083
export POSTGRES_HOST_PORT=5434

.venv/bin/pytest -v harness --ignore=harness/test_100k_chaos_replay.py
.venv/bin/pytest -v tests/test_person1_core.py tests/test_cards_loyalty_admin.py

# 3. Reset the test volume and run Suite 1 (100K chaos replay, 112K submitted events)
docker compose -p sreeman-acceptance -f docker-compose.test.yml down -v
docker compose -p sreeman-acceptance -f docker-compose.test.yml up -d
.venv/bin/pytest -v harness/test_100k_chaos_replay.py

# 4. Tear down when finished
docker compose -p sreeman-acceptance -f docker-compose.test.yml down -v
```

#### Windows (PowerShell)

```powershell
docker compose -p sreeman-acceptance -f docker-compose.test.yml down -v
docker compose -p sreeman-acceptance -f docker-compose.test.yml up -d --build

$env:LEDGER_INTEGRATION = "1"
$env:LEDGER_P1_MERGED = "1"
$env:LEDGER_P2_MERGED = "1"
$env:LEDGER_BASE_URL = "http://localhost:8081"
$env:LEDGER_REPLICA1_URL = "http://localhost:8082"
$env:LEDGER_REPLICA2_URL = "http://localhost:8083"
$env:POSTGRES_HOST_PORT = "5434"

# psycopg async connections on Windows require the Selector event loop
python -c "import asyncio, pytest; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()); raise SystemExit(pytest.main(['-v', 'harness', '--ignore=harness/test_100k_chaos_replay.py']))"

docker compose -p sreeman-acceptance -f docker-compose.test.yml down -v
docker compose -p sreeman-acceptance -f docker-compose.test.yml up -d
python -c "import asyncio, pytest; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()); raise SystemExit(pytest.main(['-v', 'harness/test_100k_chaos_replay.py']))"
```

> `harness/conftest.py` also orders `test_100k_chaos_replay.py` last if `pytest harness` is invoked directly without `--ignore`. Override default test ports with `LEDGER_TEST_POSTGRES_PORT`, `LEDGER_TEST_NGINX_PORT`, `LEDGER_TEST_API1_PORT`, and `LEDGER_TEST_API2_PORT` if needed.

---

## Verification & Benchmark Evidence

All checks run automatically in GitHub Actions (`.github/workflows/ci.yml`):

- **Go Quality & Unit/Race Tests**: `gofmt`, `go vet`, `staticcheck`, and `go test -v -race ./...` pass across all 12 packages.
- **50+ RPS Concurrent Read Isolation**: Sustained `>= 50 RPS` `REPEATABLE READ` queries across totals, gift cards, and loyalty during multi-batch ingestion with **0 partial batch observations**.
- **All 7 Harness Suites & 8 Worked Examples**: **34 / 34** non-chaos harness tests + **18 / 18** core/cards/loyalty/admin tests + **100K chaos replay** pass with 0 skipped.
- **100K Chaos Replay**: 100,000 unique events + 12% duplicates (112,000 submitted) across 300 stores / 2,400 terminals with 3% front-loaded orphans and 20 simulated 6-hour store outages completed in **`31.85 s`**, matching the independent Python reference oracle bit-for-bit on `economic_sha256` with **0 pending orphans**.
- **10,001,000-Event Physical On-Disk Snapshot & Rebuild Benchmark**: On a `5,119 MB` PostgreSQL 16 database (10,000,000 pre-snapshot events + 1,000 live post-snapshot tail events):
  - `POST /v1/admin/snapshots`: **`84.44 s`** with **`16.77 MiB`** Go API RAM (streaming `sha256.New()`).
  - `EXPLAIN (ANALYZE, BUFFERS)` on the tail query: `Index Scan using idx_ledger_events_commit_xid` in **`0.691 ms`** (50 buffer pages touched out of 10M rows).
  - `POST /v1/admin/rebuild?mode=snapshot`: **`17.91 s`** (replaying only the 1,000 tail events, ~165× faster than a full 10M-event replay) with **bit-for-bit identical `read_model_sha256` and `economic_sha256`**.

---

## Known Limits

- **Simulated vs. physical network partitions**: The chaos harness models 6-hour store outages, buffering, and reconnect waves in software rather than injecting physical packet loss or TCP partitions between containers.
- **Single-primary PostgreSQL topology**: All writes serialize through a single PostgreSQL 16 primary. This guarantees strict ACID consistency and zero-pause MVCC snapshots, but caps write throughput to one primary node and does not provide active-active multi-region writes.
- **Snapshot restore lock window**: `POST /v1/admin/rebuild?mode=snapshot` runs `admin_restore_snapshot()` inside a single transaction (`TRUNCATE` takes `ACCESS EXCLUSIVE` on read-model tables while bulk-loading `snapshot_*` rows). At 10M events this takes ~18s of rebuild downtime.
- **Offline gift-card friction trade-off**: Bounding chain-wide offline fraud to `$125` ($K_{\max}=5$ eligible stores × `$25`/store/epoch) yields a `$3.12` per-terminal slice on an 8-terminal store and declines offline gift-card use at a 6th store.
- **Non-functional production concerns omitted**: TLS/mTLS termination, API authentication/RBAC, secrets management, automated backups, and cloud provisioning are out of scope for the local Docker Compose deliverable.

---

## What I Would Do Next With Another Week

1. **Zero-downtime shadow-table snapshot rebuild**: Restore `snapshot_*` tables into a shadow schema, replay the `commit_xid` tail there, verify `read_model_sha256`, and perform a sub-millisecond schema/table swap to eliminate the ~18s `ACCESS EXCLUSIVE` restore window at 10M+ events.
2. **Physical fault injection & soak testing**: Add Toxiproxy / Chaos Mesh between Nginx, the Go replicas, and PostgreSQL to test mid-transaction TCP drops, replica kills (`SIGKILL`) under 50+ RPS load, and PostgreSQL restarts (`pg_postmaster_start_time()` fencing guard).
3. **Partitioned `ledger_events` and `event_application_status`**: Range-partition by `applied_order` / `origin_business_day` so multi-year event histories remain compact and snapshot retention/pruning is instant (`DROP TABLE`).
4. **Observability & operational runbooks**: Export OpenTelemetry traces and Prometheus metrics (deadlock retry rate, orphan age distribution, fenced-slice liability per store, rebuild duration) with alerting on stale (>72h) unresolved orphans.
