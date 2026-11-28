# Architecture Specification Review

## Scope and result

Reviewed the delivered integration branch against the locked Phase 1 and Phase 2 v10 specifications. Static inspection was supplemented by live acceptance runs against the isolated test database; the remaining regression-environment limitations are listed below.

## Phase 1 — Architecture and trade-offs

| Requirement area | Delivered-code evidence | Review |
| --- | --- | --- |
| Two stateless Go API replicas and Nginx routing | `docker-compose.yml`, `nginx/nginx.conf`, `cmd/ledger/server.go` | Present; route registration includes health, batch ingest, day close/totals, gift cards, loyalty, sync/fence, checksums, and admin seed/snapshot/rebuild. |
| Append-only event source and transactional projections | `migrations/001_init.sql`; `internal/ingest/batch.go`, `internal/sales/sales.go`, `internal/days/days.go` | Schema and projection modules are present. |
| Deterministic business-day cutoff and closed-day rules | `internal/days/days.go`, `internal/clock/clock.go`, migration triggers | Present in the implementation surface. |
| Gift-card caps, offline epochs, fencing, and loyalty convergence | `internal/giftcards/giftcards.go`, `internal/epochs/epochs.go`, `internal/loyalty/loyalty.go`, `pos_ruleset/ruleset.go` | Present; the 100K replay validated applied-order cap decisions and the accepted-event economic projection. |
| Repeatable-read reads and checksums | `internal/db/db.go`, `internal/checksum/checksum.go`, read handlers | Present in the implementation surface. |
| Seven verification suites and independent oracle | `harness/` | All seven named suite files plus oracle/simulator unit tests are present. |

## Phase 2 — Schemas, triggers, and state machines

- The migration defines the ten projection tables, append-only event enforcement, closed-day immutability, gift-card master-field immutability, operational epoch/slice tables, and ten snapshot tables.
- It includes the specified seed/reset/snapshot/restore procedures and the terminal-slice reconcile/fence/epoch-advance procedures.
- The API router exposes all specified public and administrative endpoints.
- The independent harness tests exercise payload conflicts, late-sale resolution, day-close and DST behavior, concurrent reads, offline gift-card exposure, loyalty recovery, and rebuild/snapshot parity.

## Acceptance results and limitations

- Both 50 RPS checks passed on the isolated stack: totals-only reads and concurrent totals/gift-card/loyalty reads each met the threshold with zero partial-batch observations.
- All seven integrated verification suites passed on fresh isolated databases: 33 non-chaos harness tests and the 100K chaos replay.
- The 100K replay verified 100,000 unique non-system events, 6,962 applied-order gift-card cap rejections, matching database/oracle economic checksum `263b7df19528536d69cd18313581f4033726fe9709d9fe4027b01c51236e16bd`, and zero unresolved parent orphans.
- Phase 2 defines cap acceptance by `applied_order`; the harness independently validates the persisted decisions and projects only accepted gift-card events through its economic oracle.
- Full and snapshot/tail rebuild parity passed, including a regression for an admin-seeded loyalty account with no corresponding ledger event. The reset routine now preserves such account rows while resetting their projections before replay.
- `go test ./...` passed all packages in a Linux container, avoiding the local Windows Application Control block. The isolated test Compose stack built and started successfully; existing development stacks were not modified.

## P2 requirement and evaluation review

| P2 criterion | Evidence and result |
| --- | --- |
| Duplicate, out-of-order, late events; orphan return parking/resolution; integer money | Implemented in the event ingest/projector; coverage is included in the idempotency/reclassification and 100K replay suites. |
| Immutable 02:00 business-day close and linked late adjustments | Covered by the business-day immutability suite, including close retries, late-event routing, DST, and database immutability. |
| Bounded offline gift-card exposure; exact online redemption | Gift-card suite passed for the configured limit, eligible-store restrictions, overspend flagging, sync/fencing, and concurrent online redemption. |
| Convergent offline loyalty and negative-balance behavior | Loyalty convergence suite passed across upload permutations and online negative-balance recovery. |
| Deterministic read-model rebuild | Full and snapshot/tail rebuild hashes matched in the rebuild suite; 100K replay matched its independent economic checksum. |
| Definition of done: 100K replay, 50 RPS reads, no partial batches, closed-day behavior, gift-card scenario, rebuild hash | Acceptance evidence above records each as passed on isolated clean test databases. The 100K replay runs separately because it leaves its workload in the database. |
| Submission: design, tested code, Compose deployment, README | Present: `docs/design-submission.md`, `harness/` and Go tests, `docker-compose.yml`, and `README.md`. The isolated test Compose startup was verified. |

### Evaluation criteria (evidence-based, not a panel score)

- **Correctness:** Acceptance evidence covers the P2 hard core, chaos replay, concurrent reads, and full/snapshot rebuild parity. The chaos replay models buffering/reconnect delivery rather than a physical network outage.
- **Design and architecture:** Two-replica topology, transactional consistency, failure handling, gift-card exposure policy, and trade-offs are documented in the concise design and locked specifications. The candidate must review/personalize the design rationale and be able to explain it.
- **Code quality and testing:** Application code is separated into Go packages; the independent oracle and verification harness live under `harness/`. Focused and integration tests target retries, ordering, concurrency, rebuilds, and failure cases.
- **Trade-offs and scope:** PostgreSQL transactionality, constrained offline redemption, and explicitly deferred cloud/10M-event work are stated in the design and README.
- **Documentation and usability:** README includes setup/run instructions, acceptance commands, measured results, limitations, and next-week work.

The coding-questions PDF assigns weights of 25, 30, 15, and 15 to its four listed dimensions (85 total) while describing a score out of 100. This review therefore avoids inventing a numeric score; final scoring and pass status belong to the evaluators. The guidelines' originality, candidate ownership of the design reasoning, private handling, email submission, and deadline compliance cannot be established from repository evidence.

## Team contribution integration

The current `integration/person2-harness` branch contains the Person 1 core-ledger history, the Person 2 feature/fix history, and the Person 3 harness/docs commit. This confirms integration in the current branch; it does not claim that the branch has been merged or pushed to `main`.
