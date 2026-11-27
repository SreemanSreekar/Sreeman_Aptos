"""Suite 6: Convergent Loyalty Accounting, Claim-First Online Burn & Negative Recovery.

Phase 1 (v10) §5 Invariant 6:
  - Concurrent offline earn/burn batches uploaded in opposite permutations converge to
    identical loyalty_accounts, loyalty_negative_flags, and economic_sha256
  - Online burn uses online_action_receipts claim-first idempotency
  - Negative balance blocks online burn (409 ACCOUNT_NEGATIVE_RESTRICTED) and recovers
    to ACTIVE on earn-back (stamping resolved_by_event_id)

Integration execution pending Person 1 + Person 2 merge into main.
Includes an independent oracle permutation-convergence test that runs offline.
"""

from __future__ import annotations

import asyncio
import unittest
import uuid
from datetime import datetime, timedelta, timezone

from harness.client import HarnessConfig, LedgerClient, integration_skip_reason
from harness.fixtures import make_loyalty_event
from harness.oracle import EconomicOracle

_SKIP_REASON = integration_skip_reason(requires_p2=True)


class TestLoyaltyOracleConvergenceUnit(unittest.TestCase):
    """Independent PN-Counter CRDT convergence & negative recovery check."""

    def test_opposite_permutations_converge_identically(self) -> None:
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        burn1 = make_loyalty_event(
            event_type="loyalty_burn",
            store_id="store-001",
            terminal_id="t1",
            occurred_at=t0,
            account_id="acct-1",
            points=400,
            epoch_gen=1,
            terminal_epoch_seq=1,
        )
        burn2 = make_loyalty_event(
            event_type="loyalty_burn",
            store_id="store-002",
            terminal_id="t1",
            occurred_at=t0 + timedelta(minutes=2),
            account_id="acct-1",
            points=300,
            epoch_gen=1,
            terminal_epoch_seq=1,
        )
        earn1 = make_loyalty_event(
            event_type="loyalty_earn",
            store_id="store-001",
            terminal_id="t2",
            occurred_at=t0 + timedelta(minutes=5),
            account_id="acct-1",
            points=800,
        )

        o1 = EconomicOracle()
        o2 = EconomicOracle()
        for o in (o1, o2):
            o.seed_store("store-001", "America/New_York", ["t1", "t2"])
            o.seed_store("store-002", "America/Chicago", ["t1"])
            o.seed_loyalty_account("acct-1")

        o1.add_events([burn1, burn2, earn1])
        o2.add_events([earn1, burn2, burn1])

        self.assertEqual(o1.compute_economic_sha256(), o2.compute_economic_sha256())
        self.assertEqual(o1.loyalty_accounts["acct-1"].balance, 100)
        self.assertEqual(o1.loyalty_accounts["acct-1"].status, "ACTIVE")
        self.assertEqual(len(o1.loyalty_negative_flags), 2)
        self.assertTrue(
            all(f.resolved_by_event_id == earn1["event_id"] for f in o1.loyalty_negative_flags)
        )


@unittest.skipIf(_SKIP_REASON is not None, _SKIP_REASON or "")
class TestLoyaltyConvergence(unittest.IsolatedAsyncioTestCase):
    async def test_opposite_permutation_convergence_online_block_and_recovery(self) -> None:
        cfg = HarnessConfig()
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        now = t0 + timedelta(minutes=20)

        stores = [
            {"store_id": "store-loy-1", "iana_timezone": "America/New_York", "terminal_ids": ["t1", "t2"]},
            {"store_id": "store-loy-2", "iana_timezone": "America/Chicago", "terminal_ids": ["t1"]},
        ]

        burn_a = make_loyalty_event(
            event_type="loyalty_burn",
            store_id="store-loy-1",
            terminal_id="t1",
            occurred_at=t0,
            account_id="loy-conv-1",
            points=500,
            epoch_gen=1,
            terminal_epoch_seq=1,
        )
        burn_b = make_loyalty_event(
            event_type="loyalty_burn",
            store_id="store-loy-2",
            terminal_id="t1",
            occurred_at=t0 + timedelta(minutes=1),
            account_id="loy-conv-1",
            points=300,
            epoch_gen=1,
            terminal_epoch_seq=1,
        )
        earn_small = make_loyalty_event(
            event_type="loyalty_earn",
            store_id="store-loy-1",
            terminal_id="t2",
            occurred_at=t0 + timedelta(minutes=2),
            account_id="loy-conv-1",
            points=200,
        )

        u1, u2, _ = cfg.endpoint_pair()
        async with (
            LedgerClient(u1) as r1,
            LedgerClient(u2) as r2,
        ):
            await r1.admin_seed(
                stores=stores,
                loyalty_accounts=[{"account_id": "loy-conv-1"}],
                simulated_now=now,
            )

            # Upload opposite permutations across replicas
            resp_a, resp_b = await asyncio.gather(
                r1.post_batch(
                    store_id="store-loy-1",
                    terminal_id="t1",
                    events=[burn_a, earn_small],
                    simulated_now=now,
                ),
                r2.post_batch(
                    store_id="store-loy-2",
                    terminal_id="t1",
                    events=[burn_b],
                    simulated_now=now,
                ),
            )
            self.assertEqual(resp_a.status_code, 200, resp_a.text)
            self.assertEqual(resp_b.status_code, 200, resp_b.text)

            acct = (await r1.get_loyalty("loy-conv-1")).json()
            self.assertEqual(acct["balance"], -600)
            self.assertEqual(acct["status"], "NEGATIVE_RESTRICTED")

            # Negative balance blocks online burn with 409 ACCOUNT_NEGATIVE_RESTRICTED
            blocked_eid = str(uuid.uuid4())
            r_block = await r1.online_burn_loyalty(
                event_id=blocked_eid,
                store_id="store-loy-1",
                terminal_id="t1",
                account_id="loy-conv-1",
                points=50,
                occurred_at=now + timedelta(seconds=5),
                simulated_now=now + timedelta(seconds=5),
            )
            self.assertEqual(r_block.status_code, 409)
            self.assertIn("ACCOUNT_NEGATIVE_RESTRICTED", r_block.text)

            # Replay exact same online request -> claim-first receipt returns stored 409
            r_block_retry = await r2.online_burn_loyalty(
                event_id=blocked_eid,
                store_id="store-loy-1",
                terminal_id="t1",
                account_id="loy-conv-1",
                points=50,
                occurred_at=now + timedelta(seconds=5),
                simulated_now=now + timedelta(seconds=5),
            )
            self.assertEqual(r_block_retry.status_code, 409)

            # Earn-back restores ACTIVE
            earn_back = make_loyalty_event(
                event_type="loyalty_earn",
                store_id="store-loy-1",
                terminal_id="t2",
                occurred_at=now + timedelta(seconds=10),
                account_id="loy-conv-1",
                points=1000,
            )
            await r1.post_batch(
                store_id="store-loy-1",
                terminal_id="t2",
                events=[earn_back],
                simulated_now=now + timedelta(seconds=15),
            )
            acct_after = (await r1.get_loyalty("loy-conv-1")).json()
            self.assertEqual(acct_after["balance"], 400)
            self.assertEqual(acct_after["status"], "ACTIVE")

            # Online burn now succeeds and is idempotent on replay
            ok_eid = str(uuid.uuid4())
            b1 = await r1.online_burn_loyalty(
                event_id=ok_eid,
                store_id="store-loy-1",
                terminal_id="t1",
                account_id="loy-conv-1",
                points=150,
                occurred_at=now + timedelta(seconds=20),
                simulated_now=now + timedelta(seconds=20),
            )
            b2 = await r2.online_burn_loyalty(
                event_id=ok_eid,
                store_id="store-loy-1",
                terminal_id="t1",
                account_id="loy-conv-1",
                points=150,
                occurred_at=now + timedelta(seconds=20),
                simulated_now=now + timedelta(seconds=20),
            )
            self.assertEqual(b1.status_code, 200, b1.text)
            self.assertEqual(b2.status_code, 200, b2.text)
            acct_final = (await r1.get_loyalty("loy-conv-1")).json()
            self.assertEqual(acct_final["balance"], 250)


if __name__ == "__main__":
    unittest.main()
