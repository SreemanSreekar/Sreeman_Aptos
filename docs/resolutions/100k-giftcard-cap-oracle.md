# 100K Replay Gift-Card Cap Oracle Alignment

## Issue summary

The first 100K replay completed but produced different economic checksums: the database returned `a637d283d63db0a61ba507a43127771f1260a15bba7f83f1ef5734b8751e5fb4`, while the Python oracle expected `ec7d77825bda1322ed741b1dea4b5ffb8941958ebaa30561966ad1fd36c6fe97`.

## Code and specification locations

- `internal/giftcards/giftcards.go:79` projects offline gift-card redemptions and applies the cap using prior accepted events in `applied_order`.
- `docs/phase2_schemas_and_state_machines.md:1518` specifies the same applied-order cap rule.
- `harness/oracle.py:491` originally evaluated cap acceptance in occurred-at order.
- `harness/test_100k_chaos_replay.py:118` now verifies the cap contract and passes the accepted set to the economic oracle.

## Investigation

Recreated the deterministic workload and compared database projections to the oracle. Sales and loyalty rows matched; gift-card statuses differed for 112 events across 24 cards. The discrepancy was a harness/specification ordering mismatch, not a runtime projection defect: Phase 2 makes cap admission depend on `applied_order`, while the oracle had used occurred-at order to select accepted gift-card events.

## Resolution

Preserved the explicit Phase 2 runtime contract. The replay test now loads gift-card events in applied order, independently checks each accepted and rejected event against the configured per-card/store/epoch cap, and computes the economic checksum with the independent oracle over that accepted event set. The test also verifies all 10,000 expected gift-card event IDs are present. No ledger behavior or cap semantics changed.

## Verification

- The clean-database 100K replay passed in 69.29 seconds.
- Focused Python oracle/simulator regression: 19 passed.
- Final replay checksum: `a637d283d63db0a61ba507a43127771f1260a15bba7f83f1ef5734b8751e5fb4`.
- The replay contained 100,000 unique non-system events and 576 system day-close events; 6,945 gift-card redemptions were rejected by the applied-order cap, and no parent orphans remained pending.
- The 12 non-chaos integrated test cases had passed on clean test-database runs. Final regression limitations are recorded in `docs/acceptance-review.md`.
