  # `Sreeman_Aptos` — Offline-First POS Distributed Ledger Architecture & Trade-Offs

This document summarizes the locked **Phase 1 & Phase 2 (v10)** architecture, correctness invariants, and engineering trade-offs for the 300-store, 2,400-terminal offline-first grocery POS ledger.

---

## 1. System Topology & Stack

```
POS Terminals (Online + Offline Buffers)
        │
        ▼
  Nginx 1.27 (least_conn, keepalive) :8080
     ├──▶ Go 1.23 API Replica 1 (stateless, ledger_app)
     └──▶ Go 1.23 API Replica 2 (stateless, ledger_app)
                │
                ▼
         PostgreSQL 16 (ACID Source of Truth + Projections + Snapshots)
```

- **PostgreSQL 16 as Single ACID Engine**: Rather than introducing Kafka/Redis alongside Postgres (which reintroduces dual-write race conditions and operational overhead), all event persistence, row-level serialization, read-model projections, and snapshot watermarks execute inside PostgreSQL ACID transactions.
- **Stateless Go 1.23 Replicas (`net/http` + `pgx/v5`)**: Strictly `int64` minor-unit/points arithmetic (zero floats), embedded `tzdata`, and canonical SHA-256 serialization.
- **Dual Database Roles**:
  - `ledger_app`: Runtime API role (`SELECT, INSERT` only on `ledger_events`; zero trigger-bypass rights; zero access to `admin_*` procedures or `snapshot_*` tables).
  - `ledger_admin`: Schema owner executing `SECURITY DEFINER` seed, snapshot, reset, reconcile, fence, and epoch-advance procedures.
- **Zero-Dependency `pos_ruleset` & Isolated Python `harness/`**: Offline terminal enforcement mirrors `pos_ruleset` independently in Python with an in-order `economic_sha256` reference oracle and 7-suite verification matrix.

---

## 2. Core Invariants & Trade-Off Resolutions

### 2.1 Exactly-Once Multi-Replica Ingestion & Canonical Ordering
- **Deterministic `payload_hash`**: Canonicalizes `(event_type, store_id, terminal_id, occurred_at_utc_us, compact_sorted_payload_json)` with SHA-256. Any re-submitted `event_id` with a mismatched `payload_hash` aborts the entire transaction (`409 EVENT_ID_PAYLOAD_MISMATCH`); identical `(event_id, payload_hash)` is an idempotent no-op.
- **Pre-Insert Topological Sort**: Batches are sorted prior to DB insertion (`sale` → `return`/`void` → `giftcard_redeem`/`loyalty_earn`/`loyalty_burn`, sub-sorted by `occurred_at ASC, event_id ASC`). Client-supplied `channel` is rejected (`400 CLIENT_CHANNEL_FORBIDDEN`) and forced server-side (`OFFLINE` for `/v1/batches`, `ONLINE` for online redeem/burn, `SYSTEM` for `day_close`).
- **Canonical Lock Ordering + Post-Lock `applied_order`**: Locks are acquired in fixed global table order (`sales` → `store_business_days` → `gift_cards` → `loyalty_accounts` → `store_offline_epochs` → `terminal_offline_slices`), sorted by primary key within each table, with bounded retry (up to 5x with jitter) on `40P01`/`40001`. `applied_order = nextval('ledger_applied_order_seq')` is allocated *after* locks so live projection order and rebuild order always match.

### 2.2 Serialized Orphan Resolution & Prefix-Scoped Reclassification
- **Missing-Key Race Elimination**: Every `sale`, `return`, and `void` executes `INSERT INTO sales (sale_id, status) VALUES ($S, 'PLACEHOLDER') ON CONFLICT DO NOTHING; SELECT * FROM sales WHERE sale_id = $S FOR UPDATE;`, serializing parent and child events on the exact same row across both replicas.
- **Orphan Posting Rule (`resolved_at_utc`)**: Parked orphans store zero totals while `PENDING_PARENT`. When the parent `sale` arrives, `resolved_at_utc = parent_sale.ingested_at` determines whether `origin_business_day` is already closed and routes both the late sale and resolved orphan to the open `posting_business_day` (`LATE_POSTING` in `store_day_adjustments`).
- **Prefix-Scoped Recomputation (`applied_order <= e.applied_order`)**: Live projection, full rebuild, and snapshot+tail rebuild call the identical `projectEvent(tx, e)` over the prefix `applied_order <= e.applied_order` ordered by `(occurred_at ASC, event_id ASC)`. If a separate transaction injects an earlier `void(10:02)` after `return(10:05)`, `sales.revision` increments and a signed `RECLASSIFICATION` adjustment posts to the open `posting_business_day` without mutating closed days—and replays identically during rebuilds.

### 2.3 Immutable `02:00` Store-Local Business Day Close
- **Half-Open Intervals & DST**: Business day $D$ spans $[\text{cutoff}(D-1), \text{cutoff}(D))$ at `02:00:00.000000` store-local time on $D+1$. Spring-forward gaps advance to the first valid instant $\ge 02:00$; fall-back folds place both `01:xx` hours in $D-1$.
- **Timing & Idempotent `UUIDv5` Close**: Closing before `cutoff_utc(D)` fails with `409 BUSINESS_DAY_CUTOFF_NOT_REACHED`. Retries across replicas emit exactly one deterministic `UUIDv5` `day_close` event.
- **Trigger-Enforced Immutability**: `BEFORE INSERT OR UPDATE OR DELETE` triggers lock `store_business_days` `FOR SHARE` and reject any `ledger_app` mutation to `store_day_totals` or `store_day_adjustments` on `CLOSED` days (`BUSINESS_DAY_CLOSED_IMMUTABLE`).

### 2.4 Unified Claim-First Online Idempotency & Batch Race Resolution
- `POST /v1/giftcards/redeem` and `POST /v1/loyalty/burn` claim `online_action_receipts` (`PENDING`) **first** via `ON CONFLICT (event_id) DO NOTHING RETURNING event_id`.
- `POST /v1/batches` likewise claims `PENDING` receipts before inserting `ledger_events` and finalizes `APPROVED`/`DECLINED` afterward, preventing `READ COMMITTED` uncommitted-row blind spots.
- If a batch commits `event_id` between an online tx's `PENDING` claim and `ledger_events` insert, `ON CONFLICT (event_id) DO NOTHING` routes the online tx to finalize its receipt (`APPROVED`/`200`) **without** double-debiting balances.

### 2.5 Gift-Card Chain-Wide Hard Upper Bound ($L = \$125.00$)
Bounded offline exposure across any sequence of epochs and outages rests on three composable mechanisms:
1. **Immutable 5-Store Eligibility Cluster ($K_{\max} = 5$)**: `gift_cards.eligible_stores` is immutable (DB trigger); any 6th disconnected store rejects offline redemptions (`OFFLINE_STORE_NOT_ELIGIBLE`).
2. **Per-Store / Per-Epoch Cap ($C_{\text{store}} = 2500$ minor / $\$25.00$) & Multi-Terminal Slicing**: Each terminal $t \in \{1,\dots,T_s\}$ receives $C_{\text{term}}(s, t) = \lfloor C_{\text{store,avail}} / T_s \rfloor$, applied as a per-card quota template (`spent_by_card_minor[card_id] <= slice_minor`).
3. **Store-Epoch Renewal Barrier & Trusted Clocks**:
   - Terminals enforce `local_fenced`, unchanged `boot_id` (fail-closed on offline reboot), `wall_clock >= hwm_utc` (fail-closed on rollback), sync RTT $\le 30\text{s}$, and suspend-aware `CLOCK_BOOTTIME < 21{,}600{,}000\text{ ms}` (6h TTL).
   - Under worst-case opposite oscillator drift ($\rho_{\max} = \rho_{\text{hq}} = 1000\text{ ppm}$) + $\epsilon_{\text{rtt}} = 30\text{s}$, max terminal expiry delay is $\approx \text{TTL} + 74\text{s}$. HQ fences unresponsive `ACTIVE` slices only after $\Delta_{\text{hq\_fence}} = \text{TTL} + 600\text{s} = 22{,}200\text{s}$ using DB-anchored `pg_postmaster_start_time()` (failing closed on DB/HQ restart).
   - `advance_store_epoch()` enforces exact set-coverage over immutable `store_epoch_roster` (`RECONCILED` or `FENCED`) and reserves `SUM(slice_minor)` for all `FENCED` slices out of `C_store` before pre-inserting `epoch_gen + 1` slices. Hence:
     $$\sum_{t \in \text{ACTIVE}(s)} C_{\text{term}}(s,t) + \sum_{t \in \text{FENCED}(s)} C_{\text{term}}(s,t) \le \$25.00 \implies \text{Chain-Wide Exposure} \le 5 \times \$25.00 = \$125.00$$
   - Overspending cards automatically transition to `OVERSPENT_FROZEN` with 100% event flagging in `giftcard_overspend_flags`.

### 2.6 Convergent Loyalty, `REPEATABLE READ` Reads & Consistent Snapshots
- **Loyalty PN-Counter CRDT**: `balance = earned_total - burned_total` converges across any upload permutation; offline burns obey a store/epoch cap (`5000` points default, `0` = unlimited); online burns reject `NEGATIVE_RESTRICTED` accounts until earn-back restores `balance >= 0`.
- **50+ RPS Read Isolation**: All read endpoints run inside `REPEATABLE READ READ ONLY`, eliminating partial-batch visibility with zero write locks.
- **Consistent-Watermark Snapshots**: `POST /v1/admin/snapshots` coordinates a `REPEATABLE READ READ ONLY` Reader Tx (`pg_export_snapshot()`) with a `REPEATABLE READ` Writer Tx (`SET TRANSACTION SNAPSHOT`) to bulk-copy all 10 read-model tables atomically. `mode=snapshot` rebuilds restore `snapshot_*` rows and replay only `commit_xid >= xmin AND NOT pg_visible_in_snapshot(commit_xid, pg_snap)` in `ORDER BY applied_order ASC`.
- **Canonical Verification**: `economic_sha256` (4 order-invariant economic tables rolled back to `origin_business_day`) and `read_model_sha256` (all 10 projection tables) use strict tab-delimited UTF-8 + UTC microsecond + RFC 8259 JSON encoding.

---

## 3. Verification Matrix (`harness/`)

| Suite | File | Scope |
| :--- | :--- | :--- |
| Unit Tests | `harness/test_oracle_and_simulator_unit.py` | Canonical encoding, DST cutoffs, oracle invariance, offline `pos_ruleset` simulator |
| Suite 1 | `harness/test_100k_chaos_replay.py` | 100k unique + 12% dupes, 3% orphans, 48h shuffle, 20 outages vs. Python oracle |
| Suite 2 | `harness/test_idempotency_and_reclassification.py` | Payload collision, duplicate `sale_id`, reversed batch, late-void rebuilds, batch/online race |
| Suite 3 | `harness/test_business_day_immutability.py` | Early close `409`, idempotent retry, Mon orphan/Tue sale, DST, DB triggers, empty day rebuild |
| Suite 4 | `harness/test_concurrent_reads_50rps.py` | Sustained 50+ RPS `REPEATABLE READ` reads during batch ingest |
| Suite 5 | `harness/test_giftcard_overspend_and_online.py` | 40 offline cards ($\le \$125$), 6th-store rejection, 2-replica sync/fence race, online exactness |
| Suite 6 | `harness/test_loyalty_convergence.py` | Opposite-permutation convergence, claim-first online burn, negative block & earn-back |
| Suite 7 | `harness/test_rebuild_and_snapshot.py` | Live mid-write snapshot capture (`SET TRANSACTION SNAPSHOT`) + full vs. snapshot+tail equality |
