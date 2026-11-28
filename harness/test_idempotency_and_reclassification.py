"""Suite 2: Payload Collision, Reversed Batch, Separate-Tx Late-Void & Race Tests.

Phase 1 (v10) §5 Invariant 2:
  - Person 1 compatible (requires_p2=False):
      * 400 CLIENT_CHANNEL_FORBIDDEN on client-supplied channel
      * 409 EVENT_ID_PAYLOAD_MISMATCH on payload collision
      * DUPLICATE_SALE_ID marked REJECTED_INVALID
      * Reversed single batch [return(10:05), void(10:02)]
      * Separate-tx late-void (Tx1 sale, Tx2 return(10:05), Tx3 void(10:02)) -> RECLASSIFICATION
  - Person 2 dependent (requires_p2=True):
      * live == full rebuild == snapshot+tail rebuild read_model_sha256 parity
      * Concurrent batch + online action race (giftcard_redeem & loyalty_burn)
"""

from __future__ import annotations

import asyncio
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from harness.client import DbInspector, HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import (
    make_giftcard_redeem_event,
    make_loyalty_event,
    make_return_event,
    make_sale_event,
    make_void_event,
)
from harness.oracle import EconomicOracle

_P1_SKIP = integration_skip_reason(requires_p2=False)
_P2_SKIP = integration_skip_reason(requires_p2=True)


@unittest.skipIf(_P1_SKIP is not None, _P1_SKIP or "")
class TestIdempotencyAndReclassificationPerson1(unittest.IsolatedAsyncioTestCase):
    """Person 1-compatible tests (seeds via admin_seed_store; uses /v1/batches & /totals)."""

    async def test_payload_collision_duplicate_sale_and_client_channel_forbidden(self) -> None:
        cfg = HarnessConfig()
        now = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        store_id = f"store-p3-idem-{uuid.uuid4().hex[:6]}"
        admin_db = DbInspector(cfg.admin_dsn)
        await admin_db.seed_store_sql(
            store_id=store_id,
            iana_timezone="America/New_York",
            terminal_ids=["term-1"],
            simulated_now=now,
        )

        async with LedgerClient(cfg.base_url) as client:
            bad_ev = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(minutes=10),
                amount_minor=1000,
            )
            bad_ev["channel"] = "ONLINE"
            r_chan = await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[bad_ev],
                simulated_now=now,
            )
            self.assertEqual(r_chan.status_code, 400)
            self.assertEqual(r_chan.json()["error"], "CLIENT_CHANNEL_FORBIDDEN")

            # (a) Conflicting event_id -> 409 EVENT_ID_PAYLOAD_MISMATCH
            eid = str(uuid.uuid4())
            sid = str(uuid.uuid4())
            ev1 = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(minutes=10),
                amount_minor=2500,
                sale_id=sid,
                event_id=eid,
            )
            r1 = await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[ev1],
                simulated_now=now,
            )
            self.assertEqual(r1.status_code, 200, r1.text)
            self.assertEqual(r1.json()["accepted"], 1)

            ev_conflict = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(minutes=10),
                amount_minor=9999,
                sale_id=sid,
                event_id=eid,
            )
            r_conf = await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[ev_conflict],
                simulated_now=now,
            )
            self.assertEqual(r_conf.status_code, 409)
            self.assertEqual(r_conf.json()["error"], "EVENT_ID_PAYLOAD_MISMATCH")

            # (b) Duplicate sale_id with distinct event_id -> DUPLICATE_SALE_ID
            dup_eid = str(uuid.uuid4())
            dup_ev = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(minutes=5),
                amount_minor=7777,
                sale_id=sid,
                event_id=dup_eid,
            )
            r_dup = await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[dup_ev],
                simulated_now=now,
            )
            self.assertEqual(r_dup.status_code, 200, r_dup.text)
            self.assertEqual(r_dup.json()["rejected_invalid"], 1)

        app_db = DbInspector(cfg.app_dsn)
        row = await app_db.fetch_all(
            "SELECT status, rejection_reason FROM event_application_status WHERE event_id = %s",
            (dup_eid,),
        )
        self.assertEqual(row, [("REJECTED_INVALID", "DUPLICATE_SALE_ID")])

    async def test_reversed_batch_and_separate_tx_late_void_reclassification(self) -> None:
        cfg = HarnessConfig()
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = t0 + timedelta(minutes=20)
        store_id = f"store-p3-reclass-{uuid.uuid4().hex[:6]}"
        admin_db = DbInspector(cfg.admin_dsn)
        await admin_db.seed_store_sql(
            store_id=store_id,
            iana_timezone="America/New_York",
            terminal_ids=["term-1"],
            simulated_now=t0,
        )

        async with LedgerClient(cfg.base_url) as client:
            # (c) Reversed single batch [return(10:05), void(10:02)]
            sid_c = str(uuid.uuid4())
            sale_c = make_sale_event(
                store_id=store_id, terminal_id="term-1", occurred_at=t0, amount_minor=5000, sale_id=sid_c
            )
            ret_c = make_return_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=5),
                parent_sale_id=sid_c,
                amount_minor=2000,
            )
            void_c = make_void_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=2),
                parent_sale_id=sid_c,
                amount_minor=5000,
            )
            await client.post_batch(store_id=store_id, terminal_id="term-1", events=[sale_c], simulated_now=now)
            r_rev = await client.post_batch(
                store_id=store_id, terminal_id="term-1", events=[ret_c, void_c], simulated_now=now
            )
            self.assertEqual(r_rev.status_code, 200)
            self.assertEqual(r_rev.json()["accepted"], 1)
            self.assertEqual(r_rev.json()["rejected_invalid"], 1)

            # (d) Separate-tx late-void: Tx1 sale -> Tx2 return(10:05) -> Tx3 void(10:02)
            sid_d = str(uuid.uuid4())
            sale_d = make_sale_event(
                store_id=store_id, terminal_id="term-1", occurred_at=t0, amount_minor=6000, sale_id=sid_d
            )
            ret_d = make_return_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=5),
                parent_sale_id=sid_d,
                amount_minor=2500,
            )
            void_d = make_void_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=2),
                parent_sale_id=sid_d,
                amount_minor=6000,
            )
            await client.post_batch(store_id=store_id, terminal_id="term-1", events=[sale_d], simulated_now=now)
            await client.post_batch(store_id=store_id, terminal_id="term-1", events=[ret_d], simulated_now=now)
            await client.post_batch(store_id=store_id, terminal_id="term-1", events=[void_d], simulated_now=now)

            tot = (await client.get_store_totals(store_id=store_id, day="2026-06-15")).json()
            self.assertEqual(tot["totals"]["net_total_minor"], 0)
            reclass = [
                a
                for a in tot["adjustments"]
                if a["entry_kind"] == "RECLASSIFICATION" and a["event_id"] == ret_d["event_id"]
            ]
            self.assertEqual(len(reclass), 1)
            self.assertEqual(reclass[0]["amount_delta_minor"], 2500)


@unittest.skipIf(_P2_SKIP is not None, _P2_SKIP or "")
class TestIdempotencyAndReclassification(unittest.IsolatedAsyncioTestCase):
    """Person 2-dependent rebuild parity & online-vs-batch race tests."""

    async def test_reversed_batch_and_separate_tx_late_void_rebuilds(self) -> None:
        cfg = HarnessConfig()
        store_id = f"store-reclass-{uuid.uuid4().hex[:8]}"
        empty_account_id = f"loy-rebuild-{uuid.uuid4().hex[:8]}"
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = t0 + timedelta(minutes=20)
        async with LedgerClient(cfg.base_url) as client:
            await client.admin_seed(
                stores=[
                    {
                        "store_id": store_id,
                        "iana_timezone": "America/New_York",
                        "terminal_ids": ["term-1"],
                    }
                ],
                loyalty_accounts=[{"account_id": empty_account_id}],
                simulated_now=now,
            )

            sid_c = str(uuid.uuid4())
            sale_c = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0,
                amount_minor=5000,
                sale_id=sid_c,
            )
            ret_c = make_return_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=5),
                parent_sale_id=sid_c,
                amount_minor=2000,
            )
            void_c = make_void_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=2),
                parent_sale_id=sid_c,
                amount_minor=5000,
            )
            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[sale_c],
                simulated_now=now,
            )
            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[ret_c, void_c],
                simulated_now=now,
            )

            sid_d = str(uuid.uuid4())
            sale_d = make_sale_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0,
                amount_minor=6000,
                sale_id=sid_d,
            )
            ret_d = make_return_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=5),
                parent_sale_id=sid_d,
                amount_minor=2500,
            )
            void_d = make_void_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=t0 + timedelta(minutes=2),
                parent_sale_id=sid_d,
                amount_minor=6000,
            )
            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[sale_d],
                simulated_now=now,
            )
            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[ret_d],
                simulated_now=now,
            )
            snap_resp = await client.admin_capture_snapshot()
            self.assertEqual(snap_resp.status_code, 200, snap_resp.text)

            await client.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[void_d],
                simulated_now=now,
            )

            live_sha = (await client.get_checksum()).json()["read_model_sha256"]
            await client.admin_rebuild(mode="full")
            full_sha = (await client.get_checksum()).json()["read_model_sha256"]
            await client.admin_rebuild(mode="snapshot")
            snap_tail_sha = (await client.get_checksum()).json()["read_model_sha256"]
            account_resp = await client.get_loyalty(empty_account_id)

            self.assertEqual(live_sha, full_sha)
            self.assertEqual(live_sha, snap_tail_sha)
            self.assertEqual(account_resp.status_code, 200, account_resp.text)
            self.assertEqual(account_resp.json()["balance"], 0)

    async def test_concurrent_batch_vs_online_race_same_event_id(self) -> None:
        cfg = HarnessConfig()
        u1, u2, _ = cfg.endpoint_pair(require_distinct=True)
        run_id = uuid.uuid4().hex[:8]
        store_id = f"store-race-{run_id}"
        card_id = f"gc-race-{run_id}"
        account_id = f"loy-race-{run_id}"
        now = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        race_gc_eid = str(uuid.uuid4())
        race_loy_eid = str(uuid.uuid4())

        async with (
            LedgerClient(u1) as r1,
            LedgerClient(u2) as r2,
        ):
            await r1.admin_seed(
                stores=[
                    {
                        "store_id": store_id,
                        "iana_timezone": "America/New_York",
                        "terminal_ids": ["term-1"],
                    }
                ],
                gift_cards=[
                    {
                        "card_id": card_id,
                        "initial_balance_minor": 5000,
                        "eligible_stores": [store_id],
                    }
                ],
                loyalty_accounts=[{"account_id": account_id}],
                simulated_now=now,
            )

            earn_ev = make_loyalty_event(
                event_type="loyalty_earn",
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(minutes=2),
                account_id=account_id,
                points=1000,
            )
            await r1.post_batch(
                store_id=store_id,
                terminal_id="term-1",
                events=[earn_ev],
                simulated_now=now,
            )

            gc_ev = make_giftcard_redeem_event(
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(seconds=10),
                card_id=card_id,
                amount_minor=1500,
                epoch_gen=1,
                terminal_epoch_seq=1,
                event_id=race_gc_eid,
            )
            loy_ev = make_loyalty_event(
                event_type="loyalty_burn",
                store_id=store_id,
                terminal_id="term-1",
                occurred_at=now - timedelta(seconds=5),
                account_id=account_id,
                points=300,
                epoch_gen=1,
                terminal_epoch_seq=2,
                event_id=race_loy_eid,
            )

            gc_batch, gc_online, loy_batch, loy_online = await asyncio.gather(
                r1.post_batch(
                    store_id=store_id,
                    terminal_id="term-1",
                    events=[gc_ev],
                    simulated_now=now,
                ),
                r2.online_redeem_giftcard(
                    event_id=race_gc_eid,
                    store_id=store_id,
                    terminal_id="term-1",
                    card_id=card_id,
                    amount_minor=1500,
                    occurred_at=now - timedelta(seconds=10),
                    simulated_now=now,
                ),
                r1.post_batch(
                    store_id=store_id,
                    terminal_id="term-1",
                    events=[loy_ev],
                    simulated_now=now,
                ),
                r2.online_burn_loyalty(
                    event_id=race_loy_eid,
                    store_id=store_id,
                    terminal_id="term-1",
                    account_id=account_id,
                    points=300,
                    occurred_at=now - timedelta(seconds=5),
                    simulated_now=now,
                ),
            )
            self.assertEqual(gc_batch.status_code, 200, gc_batch.text)
            self.assertEqual(gc_online.status_code, 200, gc_online.text)
            self.assertEqual(loy_batch.status_code, 200, loy_batch.text)
            self.assertEqual(loy_online.status_code, 200, loy_online.text)

            gc = (await r1.get_giftcard(card_id)).json()
            self.assertEqual(gc["balance_minor"], 3500)
            loy = (await r1.get_loyalty(account_id)).json()
            self.assertEqual(loy["balance"], 700)

        oracle = EconomicOracle()
        oracle.seed_store(store_id, "America/New_York", ["term-1"])
        oracle.seed_gift_card(card_id, 5000, [store_id])
        oracle.seed_loyalty_account(account_id)
        oracle.add_events([earn_ev, gc_ev, loy_ev])
        oracle.evaluate()
        self.assertEqual(gc["balance_minor"], oracle.gift_cards[card_id].balance_minor)
        self.assertEqual(loy["balance"], oracle.loyalty_accounts[account_id].balance)

        db = DbInspector(cfg.app_dsn)
        persisted_race_events = await db.fetch_scalar(
            "SELECT COUNT(*) FROM ledger_events WHERE event_id = ANY(%s::uuid[])",
            ([race_gc_eid, race_loy_eid],),
        )
        self.assertEqual(persisted_race_events, 2)


if __name__ == "__main__":
    unittest.main()
