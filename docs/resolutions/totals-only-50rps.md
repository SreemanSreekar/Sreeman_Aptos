# Totals-Only 50 RPS Resolution

## Issue

The totals-only read-isolation load check did not reliably meet its 50 RPS target while paired sale/return batches were being written. The endpoint must return a consistent snapshot with no partial batch totals.

## Location

- `internal/days/days.go:402`, `Handler.GetStoreTotals`
- `harness/test_concurrent_reads_50rps.py:30`, `TestConcurrentReads50RPSPerson1.test_sustained_50rps_totals_zero_partial_batch_observations`

## Cause and resolution

The totals endpoint previously issued separate reads for store existence, business-day metadata, and the totals row before reading adjustments. Those extra round trips increased request latency under concurrent load. The handler now gets store existence, day metadata, and totals in one `LEFT JOIN` query inside the existing read-only repeatable-read transaction; adjustment rows remain in the same transaction. The load test warms the HTTP keep-alive connection and uses 24 readers so its measured window tests the endpoint rather than connection setup.

The first local invocation failed before reaching the endpoint because Windows' default Proactor event loop is unsupported by psycopg's async connection helper. Re-running with `WindowsSelectorEventLoopPolicy` allowed the integration test to execute.

## Verification

Both integrated 50 RPS variants passed on a fresh isolated stack: the totals-only check and the Person 2 totals/gift-card/loyalty check. Each asserts at least 50 RPS and zero partial-batch observations; the full non-chaos harness run passed all 33 tests.
