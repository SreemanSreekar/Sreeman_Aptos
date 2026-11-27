"""Suite 7: Drop & Rebuild + SET TRANSACTION SNAPSHOT Tail Replay.

Phase 1 (v10) §5 Invariant 7:
  Capture consistent watermark snapshot via pg_export_snapshot() + SET TRANSACTION SNAPSHOT
  *during* live concurrent 2-replica writes -> finish writes -> verify:
    live read_model_sha256 == full drop & rebuild read_model_sha256 == snapshot + tail rebuild read_model_sha256
  bit-for-bit, and economic_sha256 == Python reference oracle.

Integration execution pending Person 1 + Person 2 merge into main.
"""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from harness.client import HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import make_loyalty_event, make_sale_event
from harness.oracle import EconomicOracle

_SKIP_REASON = integration_skip_reason(requires_p2=True)


@unittest.skipIf(_SKIP_REASON is not None, _SKIP_REASON or "")
class TestRebuildAndSnapshot(unittest.IsolatedAsyncioTestCase):
    async def test_live_snapshot_capture_and_full_vs_snapshot_tail_rebuild(self) -> None:
        cfg = HarnessConfig()
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = t0 + timedelta(hours=1)
        store_id = "store-snap-1"
        tz = "America/New_York"

        oracle = EconomicOracle()
        oracle.seed_store(store_id, tz, ["t1", "t2"])
        oracle.seed_loyalty_account("loy-snap-1")

        batches_r1 = [
            [
                make_sale_event(
                    store_id=store_id,
                    terminal_id="t1",
                    occurred_at=t0 + timedelta(seconds=i * 10 + j),
                    amount_minor=500 + j * 100,
                )
                for j in range(5)
            ]
            for i in range(20)
        ]
        batches_r2 = [
            [
                make_loyalty_event(
                    event_type="loyalty_earn",
                    store_id=store_id,
                    terminal_id="t2",
                    occurred_at=t0 + timedelta(seconds=i * 10 + j),
                    account_id="loy-snap-1",
                    points=100 + j,
                )
                for j in range(5)
            ]
            for i in range(20)
        ]
        for b in batches_r1 + batches_r2:
            oracle.add_events(b)

        u1, u2, _ = cfg.endpoint_pair()
        async with (
            LedgerClient(u1) as r1,
            LedgerClient(u2) as r2,
        ):
            await r1.admin_seed(
                stores=[{"store_id": store_id, "iana_timezone": tz, "terminal_ids": ["t1", "t2"]}],
                loyalty_accounts=[{"account_id": "loy-snap-1"}],
                simulated_now=now,
            )

            async def send_stream(client: LedgerClient, batches: list[list[dict]], tid: str) -> None:
                for b in batches:
                    resp = await client.post_batch(
                        store_id=store_id, terminal_id=tid, events=b, simulated_now=now
                    )
                    self.assertEqual(resp.status_code, 200, resp.text)

            async def capture_mid_flight() -> None:
                await asyncio.sleep(0.05)
                snap = await r1.admin_capture_snapshot()
                self.assertEqual(snap.status_code, 200, snap.text)

            await asyncio.gather(
                send_stream(r1, batches_r1, "t1"),
                send_stream(r2, batches_r2, "t2"),
                capture_mid_flight(),
            )

            live_chk = (await r1.get_checksum()).json()
            live_rm_sha = live_chk["read_model_sha256"]
            self.assertEqual(live_chk["economic_sha256"], oracle.compute_economic_sha256())

            r_full = await r1.admin_rebuild(mode="full")
            self.assertEqual(r_full.status_code, 200, r_full.text)
            full_sha = (await r1.get_checksum()).json()["read_model_sha256"]
            self.assertEqual(live_rm_sha, full_sha)

            r_snap = await r1.admin_rebuild(mode="snapshot")
            self.assertEqual(r_snap.status_code, 200, r_snap.text)
            snap_sha = (await r1.get_checksum()).json()["read_model_sha256"]
            self.assertEqual(live_rm_sha, snap_sha)


if __name__ == "__main__":
    unittest.main()
