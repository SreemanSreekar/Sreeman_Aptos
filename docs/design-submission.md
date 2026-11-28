# Offline-First POS Ledger — Design

## Assumptions and topology

The central service accepts at-least-once uploads from terminals that can be offline for up to six hours. Events can be duplicated, arrive out of order, or be up to 48 hours late. Money is integer minor units. Store business days close at 02:00 local time; after close, the reported day is immutable. Offline gift-card exposure is bounded by the configured store/epoch rules, while online redemption remains exact.

Two stateless Go API replicas sit behind Nginx and share PostgreSQL as the event log, transaction coordinator, projection store, and snapshot store. The independent Python harness generates traffic, simulates terminals, and checks results against an oracle.

```text
POS terminals -> Nginx -> Go API replicas -> PostgreSQL
                                  ^
                      Python verification harness
```

## Data model and consistency

`ledger_events` is the append-only source of truth. Event IDs are unique; a canonical payload hash distinguishes a safe retry from reuse of an ID with different content. `applied_order` defines the persisted processing sequence used by both live projection and rebuild. The main read models are store-day totals and adjustments, sales and parked returns/voids, gift-card balances and overspend flags, loyalty accounts and negative-balance flags, and offline epoch/slice state. Snapshot tables support a consistent-watermark rebuild.

Ingestion and projection run in one PostgreSQL transaction. Entity rows are locked in a fixed order; transient deadlock or serialization failures are retried. Duplicate events have no second effect. A return or void without its sale is parked and applied when the parent arrives. Events that belong to a closed business day are posted as linked adjustments to an open day rather than mutating the closed total. Read endpoints use repeatable-read snapshots so a query sees a complete transaction, not a partially applied batch.

Gift-card offline redemptions consume bounded store/epoch terminal slices. Store eligibility, epoch fencing, and reserved capacity constrain worst-case exposure; online redemptions serialize against the authoritative card balance. Loyalty earned and burned totals form a PN-counter, so the merged balance converges regardless of upload order. Full rebuild and snapshot-plus-tail rebuild reuse the same projector and order, with checksums used to compare results.

## Failure handling and trade-offs

- **Duplicate/retried uploads:** event-ID uniqueness and payload-hash comparison make retries idempotent and reject conflicting reuse.
- **Out-of-order parent events:** child events remain durably parked until the sale is available; resolution and projection occur transactionally.
- **Concurrent writes or transient database errors:** canonical lock ordering reduces deadlocks; bounded retries handle deadlocks and serialization conflicts.
- **Late events after close:** closed-day rows stay unchanged; linked adjustment entries are recorded on a later open day.
- **Terminal outage, reboot, or clock rollback:** offline rules expire grants and fail closed on invalid boot/clock state; reconciliation and fencing prevent unsafe slice renewal.
- **Rebuild or snapshot interruption:** the event log remains authoritative; rebuild can be repeated and checksum-checked.

PostgreSQL is used instead of adding a broker/cache because the project needs atomic event/projection writes and consistent snapshots; this trades independent scaling and broker-based buffering for simpler transactional correctness. Offline gift-card availability is intentionally constrained to bound fraud exposure ($K_{\max}=5$ eligible stores $\times \$25$/store/epoch $= \$125$ max chain-wide overdraft): splitting the \$25 store cap across 8 terminals yields a \$3.12 per-terminal slice and declines offline gift-card use at a 6th store, trading higher-ticket offline convenience for a provable worst-case loss bound. Physical on-disk benchmarking at **10,001,000 events** (5.1 GB DB) verified zero-pause snapshot capture (`84.44 s`, `16.77 MiB` Go API RAM via streaming SHA-256), `0.691 ms` `idx_ledger_events_commit_xid` tail index scan, and **`17.91 s`** incremental snapshot+tail rebuild with bit-for-bit SHA-256 parity. This implementation deliberately leaves out multi-region active-active databases, external payment-processor integration, Store-LAN peer gossip, and production cloud/observability provisioning; those remain operational follow-ups.

Detailed specifications and implementation rationale are in [Phase 1](phase1_architecture_spec.md), [Phase 2](phase2_schemas_and_state_machines.md), and the root [DESIGN.md](../DESIGN.md).
