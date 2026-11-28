"""Suite 1: 100k Chaos Replay (12% Dupes, 3% Orphans, 48h Shuffle, 20 Outages).

Phase 1 (v10) §5 Invariant 1:
  - 100,000 unique events + 12% duplicates (112,000 sent)
  - Shuffled within 48h, 3% orphan returns/voids, 20 store outages across 2 replicas
  - applied-order offline caps and economic_sha256 match the Phase 2 contract and oracle
  - 0 unresolved valid orphans in parked_events

Integration execution requires:
  - Person 1 core backend merged & running (batches, day close, totals)
  - Person 2 features merged & running (/v1/admin/seed, /v1/ledger/checksum, offline epoch provenance)
"""

from __future__ import annotations

import asyncio
import unittest
from collections import defaultdict

from harness.client import DbInspector, HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import generate_chaos_workload
from harness.oracle import EconomicOracle

_SKIP_REASON = integration_skip_reason(requires_p2=True)


@unittest.skipIf(_SKIP_REASON is not None, _SKIP_REASON or "")
class Test100kChaosReplay(unittest.IsolatedAsyncioTestCase):
    async def test_100k_chaos_replay_matches_oracle_bit_for_bit(self) -> None:
        cfg = HarnessConfig()
        workload = generate_chaos_workload(
            unique_count=100_000,
            duplicate_rate=0.12,
            orphan_rate=0.03,
            outage_stores=20,
            store_count=300,
            terminals_per_store=8,
        )

        oracle = EconomicOracle()
        for st in workload.stores:
            oracle.seed_store(st["store_id"], st["iana_timezone"], st["terminal_ids"])
        for gc in workload.gift_cards:
            oracle.seed_gift_card(
                gc["card_id"], gc["initial_balance_minor"], gc["eligible_stores"]
            )
        for la in workload.loyalty_accounts:
            oracle.seed_loyalty_account(la["account_id"])
        # Partition submitted_events into per-(store_id, terminal_id) batches while preserving
        # orphan-before-parent arrival order (orphans are already front-loaded in submitted_events).
        grouped_batches: list[tuple[str, str, list[dict]]] = []
        orphan_stream = [e for e in workload.submitted_events if e["event_id"] in workload.orphan_event_ids]
        rest_stream = [e for e in workload.submitted_events if e["event_id"] not in workload.orphan_event_ids]

        for stream in (orphan_stream, rest_stream):
            buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
            for ev in stream:
                key = (ev["store_id"], ev["terminal_id"])
                buckets[key].append(ev)
                if len(buckets[key]) >= 200:
                    grouped_batches.append((key[0], key[1], buckets.pop(key)))
            for (sid, tid), chunk in buckets.items():
                grouped_batches.append((sid, tid, chunk))

        u1, u2, _ = cfg.endpoint_pair(require_distinct=True)
        async with (
            LedgerClient(u1, timeout=60.0) as r1,
            LedgerClient(u2, timeout=60.0) as r2,
        ):
            seed_resp = await r1.admin_seed(
                stores=workload.stores,
                gift_cards=workload.gift_cards,
                loyalty_accounts=workload.loyalty_accounts,
                simulated_now=workload.simulated_now,
            )
            self.assertEqual(seed_resp.status_code, 200, seed_resp.text)

            sem = asyncio.Semaphore(16)

            async def send_batch(idx: int, sid: str, tid: str, chunk: list[dict]) -> None:
                client = r1 if idx % 2 == 0 else r2
                async with sem:
                    resp = await client.post_batch(
                        store_id=sid,
                        terminal_id=tid,
                        events=chunk,
                        simulated_now=workload.simulated_now,
                    )
                    self.assertEqual(resp.status_code, 200, resp.text)

            # Normal stores remain connected; outage-store events stay queued until the
            # reconnect wave. Orphans are still sent before parent-bearing batches per wave.
            outage_stores = set(workload.outage_store_ids)
            for is_outage_wave in (False, True):
                wave_batches = [
                    batch
                    for batch in grouped_batches
                    if (batch[0] in outage_stores) == is_outage_wave
                ]
                self.assertTrue(wave_batches)
                await asyncio.gather(
                    *(
                        send_batch(i, sid, tid, chunk)
                        for i, (sid, tid, chunk) in enumerate(wave_batches)
                    )
                )

            chk_resp = await r1.get_checksum()
            self.assertEqual(chk_resp.status_code, 200, chk_resp.text)
            chk = chk_resp.json()

        db = DbInspector(cfg.app_dsn)
        event_count = await db.fetch_scalar(
            "SELECT COUNT(*) FROM ledger_events WHERE event_type <> 'day_close'"
        )
        self.assertEqual(event_count, 100_000)

        # Phase 2 defines offline cap acceptance by applied_order, not occurred_at.
        # Validate those decisions independently, then give only accepted gift-card
        # events to the economic oracle, which recomputes their canonical balances.
        giftcard_rows = await db.fetch_all(
            """
            SELECT le.event_id::text, le.card_id, le.store_id, le.epoch_gen,
                   le.amount_minor, le.channel, eas.status, eas.rejection_reason
            FROM ledger_events le
            JOIN event_application_status eas ON eas.event_id = le.event_id
            WHERE le.event_type = 'giftcard_redeem'
            ORDER BY le.applied_order ASC
            """
        )
        self.assertEqual(len(giftcard_rows), 10_000)
        expected_gc_ids = {
            ev["event_id"]
            for ev in workload.unique_events
            if ev["event_type"] == "giftcard_redeem"
        }
        self.assertEqual({row[0] for row in giftcard_rows}, expected_gc_ids)
        offline_cap = await db.fetch_scalar(
            "SELECT giftcard_offline_store_cap_minor FROM ledger_config WHERE id = 1"
        )
        self.assertIsNotNone(offline_cap)
        applied_gc_ids: set[str] = set()
        spend_by_epoch: dict[tuple[str, str, int], int] = {}
        for event_id, card_id, store_id, epoch_gen, amount, channel, status, reason in giftcard_rows:
            self.assertEqual(channel, "OFFLINE")
            self.assertIsNotNone(epoch_gen)
            cap_key = (card_id, store_id, epoch_gen)
            prior_spend = spend_by_epoch.get(cap_key, 0)
            if status == "APPLIED":
                self.assertLessEqual(prior_spend + amount, offline_cap)
                spend_by_epoch[cap_key] = prior_spend + amount
                applied_gc_ids.add(event_id)
            else:
                self.assertEqual((status, reason), ("REJECTED_INVALID", "OFFLINE_CAP_EXCEEDED"))
                self.assertGreater(prior_spend + amount, offline_cap)

        oracle.add_events(
            ev
            for ev in workload.unique_events
            if ev["event_type"] != "giftcard_redeem" or ev["event_id"] in applied_gc_ids
        )
        oracle.evaluate()
        self.assertTrue(
            all(
                oracle.event_status[event_id] == ("APPLIED", None)
                for event_id in applied_gc_ids
            )
        )
        self.assertEqual(chk["economic_sha256"], oracle.compute_economic_sha256())

        pending_orphans = await db.fetch_scalar(
            "SELECT COUNT(*) FROM parked_events WHERE status = 'PENDING_PARENT'"
        )
        self.assertEqual(pending_orphans, 0)


if __name__ == "__main__":
    unittest.main()
