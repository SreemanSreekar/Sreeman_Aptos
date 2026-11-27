"""Suite 5: Gift-Card Offline Bound ($125), 6th-Store Rejection, 2-Replica Sync/Fence & Online Exactness.

Phase 1 (v10) §5 Invariant 5:
  (a) 40 gift cards redeemed across 2-5 eligible offline stores (multi-terminal) ->
      every card's incremental overdraft <= $125.00 (12500 minor), 100% flagged & OVERSPENT_FROZEN
  (b) 6th disconnected store rejected offline (OFFLINE_STORE_NOT_ELIGIBLE)
  (c) 2-replica concurrent sync/renewal, FENCED -> RECONCILED race, and multi-epoch multi-card
      unreconciled liability reservation: sum(ACTIVE + FENCED) <= $25.00 per (card_id, store_id),
      zero double-allocated slices, and chain-wide overdraft <= $125.00 per card
  (d) Concurrent online redeems enforce 0 overspend and 0 double-debit

Integration execution pending Person 1 + Person 2 merge into main.
Also includes an independent offline simulator + oracle proof test that runs without live stack.
"""

from __future__ import annotations

import asyncio
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from harness.client import DbInspector, HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import make_giftcard_redeem_event
from harness.oracle import EconomicOracle
from harness.simulator import (
    CardSnapshot,
    OfflineRuleError,
    OfflineTerminalClock,
    OfflineTerminalSimulator,
    partition_store_slices,
)

_SKIP_REASON = integration_skip_reason(requires_p2=True)


class TestGiftCardOfflineSimulatorProof(unittest.TestCase):
    """Independent offline simulator + oracle proof of 40-card $125 bound and 6th-store rejection."""

    def test_40_cards_5_stores_multi_terminal_and_6th_store_rejection(self) -> None:
        t0 = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        stores = [f"store-{i:03d}" for i in range(1, 7)]
        oracle = EconomicOracle()
        for sid in stores:
            oracle.seed_store(sid, "America/New_York", [f"{sid}-t1", f"{sid}-t2"])

        cards = []
        for idx in range(1, 41):
            cid = f"gc-sim-{idx:02d}"
            init_bal = 1000 + (idx % 5) * 500  # 1000..3000 minor
            elig = stores[: 2 + (idx % 4)]  # 2..5 eligible stores (store-006 always excluded)
            oracle.seed_gift_card(cid, init_bal, elig)
            cards.append((cid, init_bal, elig))

        snaps = {
            cid: CardSnapshot(cid, "ACTIVE", tuple(elig), init_bal)
            for cid, init_bal, elig in cards
        }
        for sid in stores:
            grants = partition_store_slices(
                store_id=sid, terminal_ids=[f"{sid}-t1", f"{sid}-t2"], epoch_gen=1, issued_at_utc=t0
            )
            for tid, grant in grants.items():
                clock = OfflineTerminalClock(
                    wall_utc=t0, boottime_ms=grant.grant_boottime_ms, boot_id=grant.boot_id
                )
                grant.card_snapshots = snaps
                sim = OfflineTerminalSimulator(store_id=sid, terminal_id=tid, clock=clock, grant=grant)
                for cid, _, elig in cards:
                    if sid == "store-006":
                        with self.assertRaises(OfflineRuleError) as ctx:
                            sim.redeem_giftcard_offline(card_id=cid, amount_minor=500)
                        self.assertEqual(ctx.exception.code, "OFFLINE_STORE_NOT_ELIGIBLE")
                    elif sid in elig:
                        amt = min(
                            grant.slice_minor,
                            snaps[cid].snapshot_balance_minor or grant.slice_minor,
                        )
                        sim.redeem_giftcard_offline(card_id=cid, amount_minor=amt)
                        clock.advance(1)
                oracle.add_events(sim.buffer)

        oracle.evaluate()
        for cid, init_bal, elig in cards:
            card = oracle.gift_cards[cid]
            overdraft = max(0, init_bal - card.balance_minor)
            self.assertLessEqual(overdraft, 12500)
            self.assertLessEqual(init_bal - card.balance_minor, len(elig) * 2500)
            if card.balance_minor < 0:
                self.assertEqual(card.status, "OVERSPENT_FROZEN")
                flags = [f for f in oracle.giftcard_overspend_flags if f.card_id == cid]
                self.assertGreaterEqual(len(flags), 1)


@unittest.skipIf(_SKIP_REASON is not None, _SKIP_REASON or "")
class TestGiftCardOverspendAndOnline(unittest.IsolatedAsyncioTestCase):
    async def test_40_cards_offline_6th_store_concurrent_sync_and_online(self) -> None:
        cfg = HarnessConfig()
        t0 = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        stores = [
            {"store_id": f"store-gc-{i}", "iana_timezone": "America/New_York", "terminal_ids": ["t1", "t2"]}
            for i in range(1, 7)
        ]
        cards = [
            {
                "card_id": f"gc-live-{i:02d}",
                "initial_balance_minor": 1000,
                "eligible_stores": [f"store-gc-{k}" for k in range(1, 2 + (i % 5))],
            }
            for i in range(1, 41)
        ]

        u1, u2, _ = cfg.endpoint_pair()
        async with (
            LedgerClient(u1) as r1,
            LedgerClient(u2) as r2,
        ):
            await r1.admin_seed(stores=stores, gift_cards=cards, simulated_now=t0)

            # (a) Redeem across eligible stores (2 terminals per store) and upload via /sync
            for s_idx in range(1, 6):
                sid = f"store-gc-{s_idx}"
                for tid in ("t1", "t2"):
                    evs = []
                    seq = 0
                    for c in cards:
                        if sid in c["eligible_stores"]:
                            seq += 1
                            evs.append(
                                make_giftcard_redeem_event(
                                    store_id=sid,
                                    terminal_id=tid,
                                    occurred_at=t0 + timedelta(minutes=seq),
                                    card_id=c["card_id"],
                                    amount_minor=1000,
                                    epoch_gen=1,
                                    terminal_epoch_seq=seq,
                                )
                            )
                    client = r1 if tid == "t1" else r2
                    resp = await client.sync_terminal(
                        store_id=sid,
                        terminal_id=tid,
                        epoch_gen=1,
                        events=evs,
                        expected_event_ids=[e["event_id"] for e in evs],
                        submitted_count=len(evs),
                        simulated_now=t0 + timedelta(hours=1),
                    )
                    self.assertEqual(resp.status_code, 200, resp.text)

            for c in cards:
                gc = (await r1.get_giftcard(c["card_id"])).json()
                overdraft = max(0, c["initial_balance_minor"] - gc["balance_minor"])
                self.assertLessEqual(overdraft, 12500)
                if gc["balance_minor"] < 0:
                    self.assertEqual(gc["status"], "OVERSPENT_FROZEN")
                    self.assertGreaterEqual(len(gc.get("overspend_flags", [])), 1)

            # (b) 6th store attempt rejected centrally if submitted in batch
            sixth_ev = make_giftcard_redeem_event(
                store_id="store-gc-6",
                terminal_id="t1",
                occurred_at=t0 + timedelta(minutes=5),
                card_id="gc-live-01",
                amount_minor=500,
                epoch_gen=1,
                terminal_epoch_seq=1,
            )
            await r1.post_batch(
                store_id="store-gc-6",
                terminal_id="t1",
                events=[sixth_ev],
                simulated_now=t0 + timedelta(minutes=10),
            )
            db = DbInspector(cfg.app_dsn)
            rej = await db.fetch_all(
                "SELECT status, rejection_reason FROM event_application_status WHERE event_id = %s",
                (sixth_ev["event_id"],),
            )
            self.assertEqual(rej, [("REJECTED_INVALID", "OFFLINE_STORE_NOT_ELIGIBLE")])

            # (c) Multi-epoch sequential liability reservation + 2-replica concurrent sync & FENCED -> RECONCILED race
            # Epoch 2: fence t2 -> t1 syncs alone -> advances to epoch 3 while t2's epoch-2 slice stays FENCED
            fence_now = t0 + timedelta(hours=1, seconds=22_201)
            f_resp = await r1.fence_terminal(
                store_id="store-gc-1", terminal_id="t2", epoch_gen=2, simulated_now=fence_now
            )
            self.assertEqual(f_resp.status_code, 200, f_resp.text)

            # Spend on multiple cards in epoch 2 on t1 before sync advances to epoch 3
            e2_evs = [
                make_giftcard_redeem_event(
                    store_id="store-gc-1",
                    terminal_id="t1",
                    occurred_at=fence_now - timedelta(hours=2),
                    card_id=f"gc-live-{k:02d}",
                    amount_minor=250,
                    epoch_gen=2,
                    terminal_epoch_seq=k,
                )
                for k in (1, 2, 3)
            ]
            s_e2_t1 = await r1.sync_terminal(
                store_id="store-gc-1",
                terminal_id="t1",
                epoch_gen=2,
                events=e2_evs,
                expected_event_ids=[e["event_id"] for e in e2_evs],
                submitted_count=len(e2_evs),
                simulated_now=fence_now + timedelta(seconds=5),
            )
            self.assertEqual(s_e2_t1.status_code, 200, s_e2_t1.text)

            # Verify ACTIVE(epoch 3) + FENCED(epoch 2) <= 2500
            active_fenced_mid = await db.fetch_scalar(
                "SELECT COALESCE(SUM(slice_minor), 0) FROM terminal_offline_slices "
                "WHERE store_id = 'store-gc-1' AND state IN ('ACTIVE', 'FENCED')"
            )
            self.assertLessEqual(active_fenced_mid, 2500)

            # Concurrent FENCED -> RECONCILED (t2 epoch 2) + ACTIVE sync (t1 epoch 3) across both replicas
            s_t1_e3, s_t2_e2 = await asyncio.gather(
                r1.sync_terminal(
                    store_id="store-gc-1",
                    terminal_id="t1",
                    epoch_gen=3,
                    events=[],
                    expected_event_ids=[],
                    submitted_count=0,
                    simulated_now=fence_now + timedelta(seconds=10),
                ),
                r2.sync_terminal(
                    store_id="store-gc-1",
                    terminal_id="t2",
                    epoch_gen=2,
                    events=[],
                    expected_event_ids=[],
                    submitted_count=0,
                    simulated_now=fence_now + timedelta(seconds=10),
                ),
            )
            self.assertEqual(s_t1_e3.status_code, 200, s_t1_e3.text)
            self.assertEqual(s_t2_e2.status_code, 200, s_t2_e2.text)

            active_fenced_sum = await db.fetch_scalar(
                "SELECT COALESCE(SUM(slice_minor), 0) FROM terminal_offline_slices "
                "WHERE store_id = 'store-gc-1' AND state IN ('ACTIVE', 'FENCED')"
            )
            self.assertLessEqual(active_fenced_sum, 2500)

            # Assert zero double-allocated slices across epochs
            dup_slices = await db.fetch_scalar(
                "SELECT COUNT(*) - COUNT(DISTINCT (store_id, terminal_id, epoch_gen)) "
                "FROM terminal_offline_slices WHERE store_id = 'store-gc-1'"
            )
            self.assertEqual(dup_slices, 0)

            # Assert chain-wide overdraft <= $125.00 after multi-epoch activity
            for c in cards:
                gc = (await r1.get_giftcard(c["card_id"])).json()
                self.assertLessEqual(max(0, c["initial_balance_minor"] - gc["balance_minor"]), 12500)

            # (d) Concurrent online redeems: 0 overspend & 0 double-debit
            await r1.admin_seed(
                gift_cards=[
                    {
                        "card_id": "gc-online-only",
                        "initial_balance_minor": 1000,
                        "eligible_stores": ["store-gc-1"],
                    }
                ]
            )
            online_reqs = [
                (r1 if i % 2 == 0 else r2).online_redeem_giftcard(
                    event_id=str(uuid.uuid4()),
                    store_id="store-gc-1",
                    terminal_id="t1",
                    card_id="gc-online-only",
                    amount_minor=600,
                    occurred_at=fence_now + timedelta(seconds=20 + i),
                    simulated_now=fence_now + timedelta(seconds=25),
                )
                for i in range(4)
            ]
            resps = await asyncio.gather(*online_reqs)
            ok_count = sum(1 for r in resps if r.status_code == 200)
            self.assertEqual(ok_count, 1)
            gc_on = (await r1.get_giftcard("gc-online-only")).json()
            self.assertEqual(gc_on["balance_minor"], 400)


if __name__ == "__main__":
    unittest.main()
