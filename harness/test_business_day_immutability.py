"""Suite 3: Monday 10:00 Orphan / Tuesday 09:00 Sale, Early/Idempotent Close & Immutability.

Phase 1 (v10) §5 Invariant 3:
  - Person 1 compatible (requires_p2=False):
      (a) Closing before 02:00 local returns 409 BUSINESS_DAY_CUTOFF_NOT_REACHED
      (b) Concurrent close retries after 02:00 return 200 with exactly 1 UUIDv5 day_close event
      (c) Monday 10:00 orphan return + Tuesday 09:00 sale post both to Tuesday adjustments,
          leaving Monday's closed totals untouched
      (d) DST spring-forward (2026-03-08) and fall-back (2026-11-01) boundary cutoffs verified
      (e) ledger_app closed-day SQL mutation rejected by trigger (BUSINESS_DAY_CLOSED_IMMUTABLE)
  - Person 2 dependent (requires_p2=True):
      (f) Full rebuild reconstructs empty closed days bit-for-bit (/v1/admin/rebuild + /checksum)
"""

from __future__ import annotations

import asyncio
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

from harness.canonical import business_day_cutoff_utc, day_close_event_id
from harness.client import DbInspector, HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import make_return_event, make_sale_event

_P1_SKIP = integration_skip_reason(requires_p2=False)
_P2_SKIP = integration_skip_reason(requires_p2=True)


@unittest.skipIf(_P1_SKIP is not None, _P1_SKIP or "")
class TestBusinessDayImmutabilityPerson1(unittest.IsolatedAsyncioTestCase):
    """Person 1-compatible day-close, orphan routing, DST, and SQL immutability tests."""

    async def test_early_close_idempotent_retry_orphan_routing_and_trigger_guard(self) -> None:
        cfg = HarnessConfig()
        u1, u2, _ = cfg.endpoint_pair()
        store_id = f"store-p3-day-{uuid.uuid4().hex[:6]}"
        tz = "America/New_York"
        monday = date(2026, 6, 15)
        tuesday = date(2026, 6, 16)
        cutoff_mon = business_day_cutoff_utc(monday, tz)

        mon_1000 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        tue_0159 = cutoff_mon - timedelta(seconds=1)
        tue_0200 = cutoff_mon
        tue_0900 = datetime(2026, 6, 16, 13, 0, 0, tzinfo=timezone.utc)

        admin_db = DbInspector(cfg.admin_dsn)
        await admin_db.seed_store_sql(
            store_id=store_id, iana_timezone=tz, terminal_ids=["term-1"], simulated_now=mon_1000
        )

        sale_id = str(uuid.uuid4())
        orphan_ret = make_return_event(
            store_id=store_id,
            terminal_id="term-1",
            occurred_at=mon_1000 + timedelta(minutes=5),
            parent_sale_id=sale_id,
            amount_minor=1500,
        )
        late_sale = make_sale_event(
            store_id=store_id,
            terminal_id="term-1",
            occurred_at=mon_1000,
            amount_minor=4000,
            sale_id=sale_id,
        )

        async with (
            LedgerClient(u1) as r1,
            LedgerClient(u2) as r2,
        ):
            r_orp = await r1.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[orphan_ret],
                simulated_now=mon_1000 + timedelta(minutes=6),
            )
            self.assertEqual(r_orp.status_code, 200, r_orp.text)
            self.assertEqual(r_orp.json()["parked"], 1)

            # (a) Early close before 02:00 -> 409 BUSINESS_DAY_CUTOFF_NOT_REACHED
            r_early = await r1.close_business_day(
                store_id=store_id, business_day=monday, simulated_now=tue_0159
            )
            self.assertEqual(r_early.status_code, 409)
            self.assertEqual(r_early.json()["error"], "BUSINESS_DAY_CUTOFF_NOT_REACHED")

            # (b) Concurrent close retries after 02:00 -> 200 with identical deterministic close_event_id
            c1, c2 = await asyncio.gather(
                r1.close_business_day(store_id=store_id, business_day=monday, simulated_now=tue_0200),
                r2.close_business_day(store_id=store_id, business_day=monday, simulated_now=tue_0200),
            )
            self.assertEqual(c1.status_code, 200, c1.text)
            self.assertEqual(c2.status_code, 200, c2.text)
            self.assertEqual(c1.json()["close_event_id"], c2.json()["close_event_id"])

            mon_totals_before = (await r1.get_store_totals(store_id=store_id, day=monday)).json()

            # (c) Parent sale arrives Tuesday 09:00 -> both post as adjustments in Tuesday
            r_sale = await r1.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[late_sale],
                simulated_now=tue_0900,
            )
            self.assertEqual(r_sale.status_code, 200, r_sale.text)

            mon_totals_after = (await r1.get_store_totals(store_id=store_id, day=monday)).json()
            self.assertEqual(mon_totals_before, mon_totals_after)

            tue_totals = (await r1.get_store_totals(store_id=store_id, day=tuesday)).json()
            self.assertGreaterEqual(len(tue_totals.get("adjustments", [])), 2)
            self.assertEqual(tue_totals["totals"]["adjustments_net_minor"], 2500)

        app_db = DbInspector(cfg.app_dsn)
        expected_close_id = str(day_close_event_id(store_id, monday))
        close_count = await app_db.fetch_scalar(
            "SELECT COUNT(*) FROM ledger_events WHERE event_id = %s", (expected_close_id,)
        )
        self.assertEqual(close_count, 1)

        # (e) Direct SQL mutation as ledger_app on closed day must raise BUSINESS_DAY_CLOSED_IMMUTABLE
        with self.assertRaises(Exception) as exc_ctx:
            await app_db.execute(
                "INSERT INTO store_day_totals (store_id, business_day, sales_minor) "
                "VALUES (%s, %s, 100)",
                (store_id, monday),
            )
        self.assertIn("BUSINESS_DAY_CLOSED_IMMUTABLE", str(exc_ctx.exception))

    async def test_dst_spring_forward_and_fall_back_boundaries(self) -> None:
        cfg = HarnessConfig()
        store_id = f"store-p3-dst-{uuid.uuid4().hex[:6]}"
        tz = "America/New_York"

        spring_day = date(2026, 3, 7)
        spring_cutoff = business_day_cutoff_utc(spring_day, tz)
        fall_day = date(2026, 10, 31)
        fall_cutoff = business_day_cutoff_utc(fall_day, tz)

        admin_db = DbInspector(cfg.admin_dsn)
        await admin_db.seed_store_sql(
            store_id=store_id, iana_timezone=tz, terminal_ids=["term-1"], simulated_now=spring_cutoff - timedelta(hours=12)
        )

        async with LedgerClient(cfg.base_url) as client:
            s_before = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=spring_cutoff - timedelta(seconds=1),
                amount_minor=1100,
            )
            s_at = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=spring_cutoff,
                amount_minor=2200,
            )
            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[s_before, s_at],
                simulated_now=spring_cutoff + timedelta(minutes=5),
            )
            r_early_sp = await client.close_business_day(
                store_id=store_id,
                business_day=spring_day,
                simulated_now=spring_cutoff - timedelta(seconds=1),
            )
            self.assertEqual(r_early_sp.status_code, 409)
            r_ok_sp = await client.close_business_day(
                store_id=store_id, business_day=spring_day, simulated_now=spring_cutoff
            )
            self.assertEqual(r_ok_sp.status_code, 200)
            tot_sp = (await client.get_store_totals(store_id=store_id, day=spring_day)).json()
            self.assertEqual(tot_sp["totals"]["sales_minor"], 1100)

            f_fold0 = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=datetime(2026, 11, 1, 5, 30, 0, tzinfo=timezone.utc),
                amount_minor=1300,
            )
            f_fold1 = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=datetime(2026, 11, 1, 6, 30, 0, tzinfo=timezone.utc),
                amount_minor=1700,
            )
            f_after = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=fall_cutoff,
                amount_minor=2900,
            )
            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[f_fold0, f_fold1, f_after],
                simulated_now=fall_cutoff + timedelta(minutes=5),
            )
            r_early_fb = await client.close_business_day(
                store_id=store_id,
                business_day=fall_day,
                simulated_now=fall_cutoff - timedelta(seconds=1),
            )
            self.assertEqual(r_early_fb.status_code, 409)
            r_ok_fb = await client.close_business_day(
                store_id=store_id, business_day=fall_day, simulated_now=fall_cutoff
            )
            self.assertEqual(r_ok_fb.status_code, 200)
            tot_fb = (await client.get_store_totals(store_id=store_id, day=fall_day)).json()
            self.assertEqual(tot_fb["totals"]["sales_minor"], 3000)


@unittest.skipIf(_P2_SKIP is not None, _P2_SKIP or "")
class TestBusinessDayImmutability(unittest.IsolatedAsyncioTestCase):
    """Person 2-dependent empty closed-day rebuild parity test."""

    async def test_empty_closed_day_full_rebuild_parity(self) -> None:
        cfg = HarnessConfig()
        store_id = f"store-p3-emptyday-{uuid.uuid4().hex[:6]}"
        tz = "America/New_York"
        empty_day = date(2026, 6, 14)
        cutoff = business_day_cutoff_utc(empty_day, tz)

        async with LedgerClient(cfg.base_url) as client:
            await client.admin_seed(
                stores=[{"store_id": store_id, "iana_timezone": tz, "terminal_ids": ["term-1"]}],
                simulated_now=cutoff,
            )
            r_close = await client.close_business_day(
                store_id=store_id, business_day=empty_day, simulated_now=cutoff
            )
            self.assertEqual(r_close.status_code, 200, r_close.text)
            sha_before = (await client.get_checksum()).json()["read_model_sha256"]
            await client.admin_rebuild(mode="full")
            sha_after = (await client.get_checksum()).json()["read_model_sha256"]
            self.assertEqual(sha_before, sha_after)


if __name__ == "__main__":
    unittest.main()
