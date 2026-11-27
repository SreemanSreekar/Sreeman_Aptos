"""Suite 4: 50 RPS Concurrent Read Isolation.

Phase 1 (v10) §5 Invariant 4:
  - Person 1 compatible (requires_p2=False):
      Sustained >= 50 RPS REPEATABLE READ GET /v1/stores/{id}/totals during paired
      zero-net [sale(+1000), return(-1000)] batch ingest -> 0 partial batch observations.
  - Person 2 dependent (requires_p2=True):
      Sustained >= 50 RPS across totals + giftcards + loyalty during multi-batch ingest.
"""

from __future__ import annotations

import asyncio
import time
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

from harness.client import DbInspector, HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import make_return_event, make_sale_event

_P1_SKIP = integration_skip_reason(requires_p2=False)
_P2_SKIP = integration_skip_reason(requires_p2=True)


@unittest.skipIf(_P1_SKIP is not None, _P1_SKIP or "")
class TestConcurrentReads50RPSPerson1(unittest.IsolatedAsyncioTestCase):
    """Person 1-compatible 50 RPS REPEATABLE READ isolation test on /v1/stores/{id}/totals."""

    async def test_sustained_50rps_totals_zero_partial_batch_observations(self) -> None:
        cfg = HarnessConfig()
        store_id = f"store-p3-50rps-{uuid.uuid4().hex[:6]}"
        tz = "America/New_York"
        now = datetime(2026, 6, 15, 16, 0, 0, tzinfo=timezone.utc)
        day = date(2026, 6, 15)

        admin_db = DbInspector(cfg.admin_dsn)
        await admin_db.seed_store_sql(
            store_id=store_id, iana_timezone=tz, terminal_ids=["term-1"], simulated_now=now
        )

        async with LedgerClient(cfg.base_url, timeout=30.0) as client:
            stop_reading = asyncio.Event()
            partial_observations: list[int] = []
            read_count = 0

            async def reader_worker() -> None:
                nonlocal read_count
                while not stop_reading.is_set():
                    resp = await client.get_store_totals(store_id=store_id, day=day)
                    self.assertEqual(resp.status_code, 200, resp.text)
                    net = int(resp.json()["totals"]["net_total_minor"])
                    read_count += 1
                    if net != 0:
                        partial_observations.append(net)

            async def writer_worker() -> None:
                t_start = time.monotonic()
                try:
                    while time.monotonic() - t_start < 1.5:
                        sid = str(uuid.uuid4())
                        s_ev = make_sale_event(
                            store_id=store_id,
                            terminal_id="term-1",
                            occurred_at=now - timedelta(minutes=5),
                            amount_minor=1000,
                            sale_id=sid,
                            event_id=sid,
                        )
                        r_ev = make_return_event(
                            store_id=store_id,
                            terminal_id="term-1",
                            occurred_at=now - timedelta(minutes=4),
                            parent_sale_id=sid,
                            amount_minor=1000,
                        )
                        resp = await client.post_batch(
                            store_id=store_id,
                            terminal_id="term-1",
                            events=[s_ev, r_ev],
                            simulated_now=now,
                        )
                        self.assertEqual(resp.status_code, 200, resp.text)
                        self.assertEqual(resp.json()["accepted"], 2)
                finally:
                    stop_reading.set()

            t_begin = time.monotonic()
            readers = [asyncio.create_task(reader_worker()) for _ in range(8)]
            await writer_worker()
            await asyncio.gather(*readers)
            duration = max(0.001, time.monotonic() - t_begin)

            self.assertEqual(partial_observations, [])
            self.assertGreaterEqual(read_count / duration, 50.0)


@unittest.skipIf(_P2_SKIP is not None, _P2_SKIP or "")
class TestConcurrentReads50RPS(unittest.IsolatedAsyncioTestCase):
    """Full 50 RPS test including giftcards + loyalty endpoints (Person 2)."""

    async def test_sustained_50rps_zero_partial_batch_observations(self) -> None:
        cfg = HarnessConfig()
        store_id = "store-readiso-1"
        tz = "America/New_York"
        now = datetime(2026, 6, 15, 16, 0, 0, tzinfo=timezone.utc)
        day = date(2026, 6, 15)

        batch_size = 10
        sale_amount = 1000
        num_batches = 25

        async with LedgerClient(cfg.base_url, timeout=30.0) as client:
            await client.admin_seed(
                stores=[
                    {"store_id": store_id, "iana_timezone": tz, "terminal_ids": ["term-1"]}
                ],
                gift_cards=[
                    {
                        "card_id": "gc-readiso-1",
                        "initial_balance_minor": 10000,
                        "eligible_stores": [store_id],
                    }
                ],
                loyalty_accounts=[{"account_id": "loy-readiso-1"}],
                simulated_now=now,
            )

            stop_reading = asyncio.Event()
            partial_observations: list[int] = []
            read_count = 0

            async def reader_worker() -> None:
                nonlocal read_count
                while not stop_reading.is_set():
                    t_start = time.monotonic()
                    r_tot, r_gc, r_loy = await asyncio.gather(
                        client.get_store_totals(store_id=store_id, day=day),
                        client.get_giftcard("gc-readiso-1"),
                        client.get_loyalty("loy-readiso-1"),
                    )
                    self.assertEqual(r_tot.status_code, 200)
                    self.assertEqual(r_gc.status_code, 200)
                    self.assertEqual(r_loy.status_code, 200)
                    sales_minor = int(r_tot.json()["totals"]["sales_minor"])
                    if sales_minor % (batch_size * sale_amount) != 0:
                        partial_observations.append(sales_minor)
                    read_count += 3
                    elapsed = time.monotonic() - t_start
                    if elapsed < 0.05:
                        await asyncio.sleep(0.05 - elapsed)

            async def writer_worker() -> None:
                for b_idx in range(num_batches):
                    events = [
                        make_sale_event(
                            store_id=store_id,
                            terminal_id="term-1",
                            occurred_at=now - timedelta(minutes=10, seconds=b_idx * 20 + i),
                            amount_minor=sale_amount,
                        )
                        for i in range(batch_size)
                    ]
                    resp = await client.post_batch(
                        store_id=store_id,
                        terminal_id="term-1",
                        events=events,
                        simulated_now=now,
                    )
                    self.assertEqual(resp.status_code, 200, resp.text)
                stop_reading.set()

            t_begin = time.monotonic()
            readers = [asyncio.create_task(reader_worker()) for _ in range(4)]
            await writer_worker()
            await asyncio.gather(*readers)
            duration = max(0.001, time.monotonic() - t_begin)

            self.assertEqual(
                partial_observations,
                [],
                f"Observed partial batch sales_minor values: {partial_observations}",
            )
            achieved_rps = read_count / duration
            self.assertGreaterEqual(achieved_rps, 50.0)


if __name__ == "__main__":
    unittest.main()
