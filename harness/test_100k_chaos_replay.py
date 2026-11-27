"""Suite 1: 100k Chaos Replay (12% Dupes, 3% Orphans, 48h Shuffle, 20 Outages).

Phase 1 (v10) §5 Invariant 1:
  - 100,000 unique events + 12% duplicates (112,000 sent)
  - Shuffled within 48h, 3% orphan returns/voids, 20 store outages across 2 replicas
  - economic_sha256 matches independent in-order Python oracle bit-for-bit
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
        oracle.add_events(workload.submitted_events)
        expected_economic_sha = oracle.compute_economic_sha256()

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

        u1, u2, _ = cfg.endpoint_pair()
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

            # Send orphan-bearing batches before parent-bearing batches, parallelized within each wave.
            orphan_batch_count = sum(
                1 for _, _, c in grouped_batches if any(e["event_id"] in workload.orphan_event_ids for e in c)
            )
            await asyncio.gather(
                *(
                    send_batch(i, sid, tid, chunk)
                    for i, (sid, tid, chunk) in enumerate(grouped_batches[:orphan_batch_count])
                )
            )
            await asyncio.gather(
                *(
                    send_batch(i, sid, tid, chunk)
                    for i, (sid, tid, chunk) in enumerate(grouped_batches[orphan_batch_count:])
                )
            )

            chk_resp = await r1.get_checksum()
            self.assertEqual(chk_resp.status_code, 200, chk_resp.text)
            chk = chk_resp.json()
            self.assertEqual(chk["economic_sha256"], expected_economic_sha)

        db = DbInspector(cfg.app_dsn)
        pending_orphans = await db.fetch_scalar(
            "SELECT COUNT(*) FROM parked_events WHERE status = 'PENDING_PARENT'"
        )
        self.assertEqual(pending_orphans, 0)


if __name__ == "__main__":
    unittest.main()
