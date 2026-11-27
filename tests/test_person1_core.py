import asyncio
import os
import uuid
import httpx
import psycopg
import pytest

BASE = os.getenv("LEDGER_BASE_URL", "http://localhost:8080")
PG_PORT = os.getenv("POSTGRES_HOST_PORT", "5433")
ADMIN_PW = os.getenv("LEDGER_ADMIN_PASSWORD", "ledger_admin_dev_pw")
APP_PW = os.getenv("LEDGER_APP_PASSWORD", "ledger_app_dev_pw")
ADMIN_DSN = f"postgresql://ledger_admin:{ADMIN_PW}@127.0.0.1:{PG_PORT}/ledger"
APP_DSN = f"postgresql://ledger_app:{APP_PW}@127.0.0.1:{PG_PORT}/ledger"


@pytest.fixture(scope="module", autouse=True)
def seed_stores():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute("UPDATE ledger_config SET allow_simulated_clock = true WHERE id = 1")
        conn.execute("SELECT admin_seed_store('store-p1-a', 'America/New_York', ARRAY['term-1', 'term-2'], '2026-11-20T12:00:00Z'::timestamptz)")
        conn.execute("SELECT admin_seed_store('store-p1-b', 'America/New_York', ARRAY['term-1'], '2026-11-20T12:00:00Z'::timestamptz)")
        conn.execute("SELECT admin_seed_store('store-p1-dst', 'America/New_York', ARRAY['term-1'], '2026-03-07T12:00:00Z'::timestamptz)")
        conn.execute("SELECT admin_seed_store('store-p1-50rps', 'America/New_York', ARRAY['term-1', 'term-2'], '2026-11-20T12:00:00Z'::timestamptz)")


def test_healthz_and_client_channel_rejected():
    with httpx.Client(base_url=BASE, timeout=15.0) as c:
        r = c.get("/healthz")
        assert r.status_code == 200 and r.json()["status"] == "ok"

        r = c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-23T15:00:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{
                "event_id": str(uuid.uuid4()), "event_type": "sale", "channel": "ONLINE",
                "occurred_at": "2026-11-23T14:55:00Z", "amount_minor": 1000,
            }],
        })
        assert r.status_code == 400 and r.json()["error"] == "CLIENT_CHANNEL_FORBIDDEN"


def test_idempotency_payload_collision_and_reclassification():
    wed_now = "2026-11-25T16:00:00Z"
    sale1 = str(uuid.uuid4())
    ret1 = str(uuid.uuid4())
    void1 = str(uuid.uuid4())

    with httpx.Client(base_url=BASE, timeout=15.0) as c:
        r = c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": sale1, "event_type": "sale", "occurred_at": "2026-11-25T15:00:00Z", "amount_minor": 10000}],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1

        # Duplicate exact match
        r = c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": sale1, "event_type": "sale", "occurred_at": "2026-11-25T15:00:00Z", "amount_minor": 10000}],
        })
        assert r.status_code == 200 and r.json()["deduplicated"] == 1

        # Payload collision -> 409
        r = c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": sale1, "event_type": "sale", "occurred_at": "2026-11-25T15:00:00Z", "amount_minor": 9999}],
        })
        assert r.status_code == 409 and r.json()["error"] == "EVENT_ID_PAYLOAD_MISMATCH"

        # Separate-tx late-void reclassification
        r = c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": ret1, "event_type": "return", "parent_sale_id": sale1, "occurred_at": "2026-11-25T15:05:00Z", "amount_minor": 3000}],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1

        r = c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": void1, "event_type": "void", "parent_sale_id": sale1, "occurred_at": "2026-11-25T15:02:00Z", "amount_minor": 10000}],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1

        tot = c.get("/v1/stores/store-p1-a/totals?day=2026-11-25").json()
        assert tot["totals"]["net_total_minor"] == 0
        reclass = [a for a in tot["adjustments"] if a["entry_kind"] == "RECLASSIFICATION" and a["event_id"] == ret1]
        assert len(reclass) == 1 and reclass[0]["amount_delta_minor"] == 3000

        # Reversed single-batch [return(10:05), void(10:02)] -> 0 new reclassification rows
        sale2 = str(uuid.uuid4())
        assert c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": sale2, "event_type": "sale", "occurred_at": "2026-11-25T15:10:00Z", "amount_minor": 5000}],
        }).status_code == 200
        r = c.post("/v1/batches", headers={"X-Simulated-Now": wed_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [
                {"event_id": str(uuid.uuid4()), "event_type": "return", "parent_sale_id": sale2, "occurred_at": "2026-11-25T15:15:00Z", "amount_minor": 2000},
                {"event_id": str(uuid.uuid4()), "event_type": "void", "parent_sale_id": sale2, "occurred_at": "2026-11-25T15:12:00Z", "amount_minor": 5000},
            ],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1 and r.json()["rejected_invalid"] == 1


def test_monday_orphan_tuesday_sale_day_close_and_dst():
    orphan_sale = str(uuid.uuid4())
    orphan_ret = str(uuid.uuid4())

    with httpx.Client(base_url=BASE, timeout=15.0) as c:
        # Mon 10:00 EST orphan return
        r = c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-23T15:00:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": orphan_ret, "event_type": "return", "parent_sale_id": orphan_sale, "occurred_at": "2026-11-23T14:58:00Z", "amount_minor": 2500}],
        })
        assert r.status_code == 200 and r.json()["parked"] == 1

        # Early close rejected
        r = c.post("/v1/stores/store-p1-a/days/2026-11-23/close", headers={"X-Simulated-Now": "2026-11-24T04:00:00Z"})
        assert r.status_code == 409 and r.json()["error"] == "BUSINESS_DAY_CUTOFF_NOT_REACHED"

        # Valid close + idempotent retry
        r1 = c.post("/v1/stores/store-p1-a/days/2026-11-23/close", headers={"X-Simulated-Now": "2026-11-24T07:05:00Z"})
        r2 = c.post("/v1/stores/store-p1-a/days/2026-11-23/close", headers={"X-Simulated-Now": "2026-11-24T07:10:00Z"})
        assert r1.status_code == 200 and r1.json()["idempotent"] is False
        assert r2.status_code == 200 and r2.json()["idempotent"] is True and r2.json()["close_event_id"] == r1.json()["close_event_id"]

        # Tue 09:00 EST parent sale arrives -> both post as adjustments to Tuesday
        r = c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-24T14:00:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": orphan_sale, "event_type": "sale", "occurred_at": "2026-11-23T14:50:00Z", "amount_minor": 5000}],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1

        mon = c.get("/v1/stores/store-p1-a/totals?day=2026-11-23").json()
        tue = c.get("/v1/stores/store-p1-a/totals?day=2026-11-24").json()
        assert mon["business_day"]["status"] == "CLOSED" and mon["totals"]["net_total_minor"] == 0
        assert tue["totals"]["adjustments_net_minor"] == 2500 and tue["totals"]["net_total_minor"] == 2500

        # DST Spring-Forward (2026-03-08 02:00 EST -> 03:00 EDT = 07:00Z):
        # Closing 2026-03-07 at 06:59Z fails (409); closing at 07:00Z succeeds (200).
        r_early = c.post("/v1/stores/store-p1-dst/days/2026-03-07/close", headers={"X-Simulated-Now": "2026-03-08T06:59:59Z"})
        assert r_early.status_code == 409 and r_early.json()["error"] == "BUSINESS_DAY_CUTOFF_NOT_REACHED"
        r_ok = c.post("/v1/stores/store-p1-dst/days/2026-03-07/close", headers={"X-Simulated-Now": "2026-03-08T07:00:00Z"})
        assert r_ok.status_code == 200 and r_ok.json()["status"] == "CLOSED"

    # Closed-day DB trigger guard via ledger_app
    with psycopg.connect(APP_DSN, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="BUSINESS_DAY_CLOSED_IMMUTABLE"):
            conn.execute("INSERT INTO store_day_totals (store_id, business_day, sales_minor) VALUES ('store-p1-a', '2026-11-23', 100)")


@pytest.mark.asyncio
async def test_concurrent_two_replica_duplicates_and_orphan_race():
    """Stress-test simultaneous duplicate batches + concurrent sale/orphan-return race + concurrent reads."""
    thu_now = "2026-11-26T16:00:00Z"
    pairs = [(str(uuid.uuid4()), str(uuid.uuid4())) for _ in range(25)]

    async with httpx.AsyncClient(base_url=BASE, timeout=20.0) as ac:
        tasks = []
        # Fire sale and its return concurrently (plus duplicates) across Nginx -> api-1 & api-2
        for s_id, r_id in pairs:
            sale_batch = {
                "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
                "events": [{"event_id": s_id, "event_type": "sale", "occurred_at": "2026-11-26T15:00:00Z", "amount_minor": 1000}],
            }
            ret_batch = {
                "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-2",
                "events": [{"event_id": r_id, "event_type": "return", "parent_sale_id": s_id, "occurred_at": "2026-11-26T15:01:00Z", "amount_minor": 400}],
            }
            tasks.append(ac.post("/v1/batches", headers={"X-Simulated-Now": thu_now}, json=sale_batch))
            tasks.append(ac.post("/v1/batches", headers={"X-Simulated-Now": thu_now}, json=sale_batch))  # concurrent duplicate
            tasks.append(ac.post("/v1/batches", headers={"X-Simulated-Now": thu_now}, json=ret_batch))
            tasks.append(ac.get("/v1/stores/store-p1-a/totals?day=2026-11-26"))

        responses = await asyncio.gather(*tasks)
        for resp in responses:
            assert resp.status_code == 200, f"unexpected status {resp.status_code}: {resp.text}"

        tot = (await ac.get("/v1/stores/store-p1-a/totals?day=2026-11-26")).json()
        # 25 sales * 1000 - 25 returns * 400 = 15000 exactly (zero double-counting, zero missed orphans)
        assert tot["totals"]["sales_minor"] == 25000, tot
        assert tot["totals"]["returns_minor"] == 10000, tot
        assert tot["totals"]["net_total_minor"] == 15000, tot


def test_all_per_sale_state_transitions():
    """Verify DUPLICATE_SALE_ID, multiple partial returns -> FULLY_RETURNED, RETURN_EXCEEDS_REMAINING,
    CANNOT_VOID_RETURNED_SALE, and VOID_STORE_MISMATCH."""
    fri_now = "2026-11-27T16:00:00Z"
    s1 = str(uuid.uuid4())
    s2 = str(uuid.uuid4())

    with httpx.Client(base_url=BASE, timeout=15.0) as c:
        # Primary sale + duplicate sale_id with distinct event_id
        r = c.post("/v1/batches", headers={"X-Simulated-Now": fri_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [
                {"event_id": s1, "sale_id": s1, "event_type": "sale", "occurred_at": "2026-11-27T15:00:00Z", "amount_minor": 1000},
                {"event_id": str(uuid.uuid4()), "sale_id": s1, "event_type": "sale", "occurred_at": "2026-11-27T15:01:00Z", "amount_minor": 1000},
            ],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1 and r.json()["rejected_invalid"] == 1

        # Partial return 400 -> PARTIALLY_RETURNED, attempt void (CANNOT_VOID_RETURNED_SALE),
        # attempt over-return 700 (RETURN_EXCEEDS_REMAINING), exact remaining 600 -> FULLY_RETURNED
        r = c.post("/v1/batches", headers={"X-Simulated-Now": fri_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [
                {"event_id": str(uuid.uuid4()), "event_type": "return", "parent_sale_id": s1, "occurred_at": "2026-11-27T15:02:00Z", "amount_minor": 400},
                {"event_id": str(uuid.uuid4()), "event_type": "void", "parent_sale_id": s1, "occurred_at": "2026-11-27T15:03:00Z", "amount_minor": 1000},
                {"event_id": str(uuid.uuid4()), "event_type": "return", "parent_sale_id": s1, "occurred_at": "2026-11-27T15:04:00Z", "amount_minor": 700},
                {"event_id": str(uuid.uuid4()), "event_type": "return", "parent_sale_id": s1, "occurred_at": "2026-11-27T15:05:00Z", "amount_minor": 600},
            ],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 2 and r.json()["rejected_invalid"] == 2

        # Cross-store void rejected (VOID_STORE_MISMATCH)
        assert c.post("/v1/batches", headers={"X-Simulated-Now": fri_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [{"event_id": s2, "event_type": "sale", "occurred_at": "2026-11-27T15:10:00Z", "amount_minor": 800}],
        }).status_code == 200
        r = c.post("/v1/batches", headers={"X-Simulated-Now": fri_now}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-a", "terminal_id": "term-1",
            "events": [{"event_id": str(uuid.uuid4()), "event_type": "void", "parent_sale_id": s2, "occurred_at": "2026-11-27T15:11:00Z", "amount_minor": 800}],
        })
        assert r.status_code == 200 and r.json()["rejected_invalid"] == 1


def test_cross_closed_day_reclassification_and_lazy_close():
    """Monday sale+return posted while Monday is OPEN; Tuesday 09:00 late earlier void arrives
    (without explicit /close -> triggers lazy close of Monday!) -> Monday stays untouched (+7000),
    Tuesday gets +3000 RECLASSIFICATION and -10000 LATE_POSTING void (net = -7000, overall economic net = 0)."""
    s_id = str(uuid.uuid4())
    r_id = str(uuid.uuid4())
    v_id = str(uuid.uuid4())

    with httpx.Client(base_url=BASE, timeout=15.0) as c:
        # Mon 10:00 EST: sale 10000 + return 3000 -> Mon net = 7000
        assert c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-23T15:10:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [
                {"event_id": s_id, "event_type": "sale", "occurred_at": "2026-11-23T15:00:00Z", "amount_minor": 10000},
                {"event_id": r_id, "event_type": "return", "parent_sale_id": s_id, "occurred_at": "2026-11-23T15:05:00Z", "amount_minor": 3000},
            ],
        }).status_code == 200

        # Tue 09:00 EST (no explicit /close called -> lazy close must fire!): earlier void at Mon 10:02 EST arrives
        r = c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-24T14:00:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [{"event_id": v_id, "event_type": "void", "parent_sale_id": s_id, "occurred_at": "2026-11-23T15:02:00Z", "amount_minor": 10000}],
        })
        assert r.status_code == 200 and r.json()["accepted"] == 1

        mon = c.get("/v1/stores/store-p1-b/totals?day=2026-11-23").json()
        tue = c.get("/v1/stores/store-p1-b/totals?day=2026-11-24").json()
        assert mon["business_day"]["status"] == "CLOSED" and mon["totals"]["net_total_minor"] == 7000, mon
        assert tue["totals"]["adjustments_net_minor"] == -7000 and tue["totals"]["net_total_minor"] == -7000, tue


@pytest.mark.asyncio
async def test_50_rps_atomic_batch_read_isolation():
    """Sustain >=50 RPS concurrent REPEATABLE READ totals queries for >=1.5s while paired zero-net batches
    [sale(+1000), return(-1000)] ingest concurrently; assert ZERO partial-batch observations (net == 0 always)."""
    now_ts = "2026-11-26T16:00:00Z"
    stop = asyncio.Event()
    read_count = 0
    partial_observations = 0
    t0 = asyncio.get_running_loop().time()

    async with httpx.AsyncClient(base_url=BASE, timeout=20.0) as ac:
        async def reader_worker():
            nonlocal read_count, partial_observations
            while not stop.is_set():
                resp = await ac.get("/v1/stores/store-p1-50rps/totals?day=2026-11-26")
                assert resp.status_code == 200
                net = resp.json()["totals"]["net_total_minor"]
                read_count += 1
                if net != 0:
                    partial_observations += 1

        async def writer_worker():
            try:
                while (asyncio.get_running_loop().time() - t0) < 1.5:
                    s_id = str(uuid.uuid4())
                    r_id = str(uuid.uuid4())
                    resp = await ac.post("/v1/batches", headers={"X-Simulated-Now": now_ts}, json={
                        "batch_id": str(uuid.uuid4()), "store_id": "store-p1-50rps", "terminal_id": "term-1",
                        "events": [
                            {"event_id": s_id, "event_type": "sale", "occurred_at": "2026-11-26T15:00:00Z", "amount_minor": 1000},
                            {"event_id": r_id, "event_type": "return", "parent_sale_id": s_id, "occurred_at": "2026-11-26T15:01:00Z", "amount_minor": 1000},
                        ],
                    })
                    assert resp.status_code == 200 and resp.json()["accepted"] == 2
            finally:
                stop.set()

        readers = [asyncio.create_task(reader_worker()) for _ in range(8)]
        try:
            await writer_worker()
        finally:
            stop.set()
            await asyncio.gather(*readers, return_exceptions=True)
        elapsed = asyncio.get_running_loop().time() - t0
        rps = read_count / elapsed

        assert rps >= 50.0, f"expected >=50 RPS sustained reads, got {rps:.1f} RPS ({read_count} reads in {elapsed:.2f}s)"
        assert partial_observations == 0, f"saw {partial_observations} partial batch reads!"


def test_idempotent_retry_on_new_day_creates_no_business_day_row():
    """Retrying an already-ingested event on a later day with no store_business_days row must be a
    true no-op and MUST NOT create an empty OPEN store_business_days row for the retry day."""
    ev_id = str(uuid.uuid4())
    with httpx.Client(base_url=BASE, timeout=15.0) as c:
        # Initial ingest on 2026-11-21 (Saturday)
        r1 = c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-21T15:00:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [{"event_id": ev_id, "event_type": "sale", "occurred_at": "2026-11-21T14:55:00Z", "amount_minor": 1234}],
        })
        assert r1.status_code == 200 and r1.json()["accepted"] == 1

        # Retry the exact same event on 2026-11-22 (Sunday — which has NO store_business_days row yet)
        r2 = c.post("/v1/batches", headers={"X-Simulated-Now": "2026-11-22T15:00:00Z"}, json={
            "batch_id": str(uuid.uuid4()), "store_id": "store-p1-b", "terminal_id": "term-1",
            "events": [{"event_id": ev_id, "event_type": "sale", "occurred_at": "2026-11-21T14:55:00Z", "amount_minor": 1234}],
        })
        assert r2.status_code == 200 and r2.json()["deduplicated"] == 1 and r2.json()["accepted"] == 0

    with psycopg.connect(APP_DSN, autocommit=True) as conn:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM store_business_days WHERE store_id = 'store-p1-b' AND business_day = '2026-11-22'"
        ).fetchone()[0]
        assert cnt == 0, f"expected 0 store_business_days rows for retry day 2026-11-22, got {cnt}"
