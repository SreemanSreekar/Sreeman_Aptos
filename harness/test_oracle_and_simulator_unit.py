"""Independent unit tests for canonical helpers, EconomicOracle, and OfflineTerminalSimulator.

Runnable with either `python -m unittest` (stdlib) or `pytest`.
"""

from __future__ import annotations

import hashlib
import os
import random
import unittest
import uuid
from datetime import date, datetime, timedelta, timezone

from harness.canonical import (
    NAMESPACE_DAY_CLOSE,
    business_day_cutoff_utc,
    compute_business_day,
    compute_payload_hash_bytes,
    day_close_event_id,
    fmt_bool,
    fmt_date,
    fmt_int,
    fmt_text,
    fmt_timestamptz,
    topological_batch_sort_key,
)
from harness.client import HarnessConfig
from harness.fixtures import (
    generate_chaos_workload,
    make_giftcard_redeem_event,
    make_loyalty_event,
    make_return_event,
    make_sale_event,
    make_void_event,
)
from harness.oracle import EconomicOracle, OracleEvent, PayloadMismatchError
from harness.simulator import (
    CardSnapshot,
    OfflineRuleError,
    OfflineTerminalClock,
    OfflineTerminalSimulator,
    partition_store_slices,
)


class CanonicalHelpersTests(unittest.TestCase):
    def test_field_encodings_and_no_html_escaping(self) -> None:
        self.assertEqual(fmt_int(42), "42")
        self.assertEqual(fmt_int(None), r"\N")
        self.assertEqual(fmt_bool(True), "true")
        self.assertEqual(fmt_bool(False), "false")
        self.assertEqual(fmt_bool(None), r"\N")
        self.assertEqual(fmt_date(date(2026, 3, 8)), "2026-03-08")
        self.assertEqual(fmt_date(None), r"\N")
        self.assertEqual(
            fmt_timestamptz(datetime(2026, 3, 8, 7, 0, 0, 123, tzinfo=timezone.utc)),
            "2026-03-08T07:00:00.000123Z",
        )
        # RFC 8259 JSON string literal without HTML escaping (<, >, & preserved literally)
        self.assertEqual(fmt_text("a<b>&c"), '"a<b>&c"')
        self.assertEqual(fmt_text(None), r"\N")

    def test_day_close_uuidv5_and_payload_hash_matches_sql_spec(self) -> None:
        eid = day_close_event_id("store-001", date(2026, 6, 14))
        expected = uuid.uuid5(NAMESPACE_DAY_CLOSE, "store-001:2026-06-14")
        self.assertEqual(eid, expected)

        cutoff = datetime(2026, 6, 15, 6, 0, 0, tzinfo=timezone.utc)
        phash = compute_payload_hash_bytes(
            event_type="day_close",
            store_id="store-001",
            terminal_id="SYSTEM",
            occurred_at=cutoff,
            canonical_payload={"store_id": "store-001", "business_day": "2026-06-14"},
        )
        sql_preimage = (
            "day_close|store-001|SYSTEM|2026-06-15T06:00:00.000000Z|"
            '{"business_day":"2026-06-14","store_id":"store-001"}'
        )
        self.assertEqual(phash, hashlib.sha256(sql_preimage.encode("utf-8")).digest())

    def test_canonical_payload_hash_matches_go_prepare_offline_events_all_types(self) -> None:
        """Verify Python payload_hash matches internal/ingest/batch.go PrepareOfflineEvents + canonical.PayloadHash."""
        t0 = datetime(2026, 6, 15, 14, 30, 5, 123456, tzinfo=timezone.utc)
        ts = "2026-06-15T14:30:05.123456Z"
        sid = "11111111-1111-1111-1111-111111111111"
        eid = "22222222-2222-2222-2222-222222222222"

        # 1. sale (sorted keys: amount_minor, sale_id)
        sale_ev = make_sale_event(
            store_id="s&<1>",
            terminal_id="t1",
            occurred_at=t0,
            amount_minor=4200,
            sale_id=sid,
            event_id=eid,
        )
        sale_expected = hashlib.sha256(
            f'sale|s&<1>|t1|{ts}|{{"amount_minor":4200,"sale_id":"{sid}"}}'.encode("utf-8")
        ).digest()
        self.assertEqual(OracleEvent.from_dict(sale_ev).payload_hash, sale_expected)

        # 2. return (Go sets both sale_id = parent_sale_id and parent_sale_id)
        ret_ev = make_return_event(
            store_id="store-001",
            terminal_id="t1",
            occurred_at=t0,
            parent_sale_id=sid,
            amount_minor=1200,
            event_id=eid,
        )
        ret_expected = hashlib.sha256(
            f'return|store-001|t1|{ts}|{{"amount_minor":1200,"parent_sale_id":"{sid}","sale_id":"{sid}"}}'.encode("utf-8")
        ).digest()
        self.assertEqual(OracleEvent.from_dict(ret_ev).payload_hash, ret_expected)

        # 3. void (Go sets both sale_id = parent_sale_id and parent_sale_id)
        void_ev = make_void_event(
            store_id="store-001",
            terminal_id="t1",
            occurred_at=t0,
            parent_sale_id=sid,
            amount_minor=4200,
            event_id=eid,
        )
        void_expected = hashlib.sha256(
            f'void|store-001|t1|{ts}|{{"amount_minor":4200,"parent_sale_id":"{sid}","sale_id":"{sid}"}}'.encode("utf-8")
        ).digest()
        self.assertEqual(OracleEvent.from_dict(void_ev).payload_hash, void_expected)

        # 4. giftcard_redeem offline (includes epoch_gen & terminal_epoch_seq in canonical payload)
        gc_ev = make_giftcard_redeem_event(
            store_id="store-001",
            terminal_id="t1",
            occurred_at=t0,
            card_id="gc<1>&2",
            amount_minor=750,
            epoch_gen=3,
            terminal_epoch_seq=4,
            event_id=eid,
        )
        gc_expected = hashlib.sha256(
            f'giftcard_redeem|store-001|t1|{ts}|{{"amount_minor":750,"card_id":"gc<1>&2","epoch_gen":3,"terminal_epoch_seq":4}}'.encode("utf-8")
        ).digest()
        self.assertEqual(OracleEvent.from_dict(gc_ev).payload_hash, gc_expected)

        # 5. loyalty_earn (no epoch_gen / terminal_epoch_seq)
        earn_ev = make_loyalty_event(
            event_type="loyalty_earn",
            store_id="store-001",
            terminal_id="t1",
            occurred_at=t0,
            account_id="acct-1",
            points=500,
            event_id=eid,
        )
        earn_expected = hashlib.sha256(
            f'loyalty_earn|store-001|t1|{ts}|{{"account_id":"acct-1","points":500}}'.encode("utf-8")
        ).digest()
        self.assertEqual(OracleEvent.from_dict(earn_ev).payload_hash, earn_expected)

        # 6. loyalty_burn offline (includes epoch_gen & terminal_epoch_seq)
        burn_ev = make_loyalty_event(
            event_type="loyalty_burn",
            store_id="store-001",
            terminal_id="t1",
            occurred_at=t0,
            account_id="acct-1",
            points=250,
            epoch_gen=2,
            terminal_epoch_seq=7,
            event_id=eid,
        )
        burn_expected = hashlib.sha256(
            f'loyalty_burn|store-001|t1|{ts}|{{"account_id":"acct-1","epoch_gen":2,"points":250,"terminal_epoch_seq":7}}'.encode("utf-8")
        ).digest()
        self.assertEqual(OracleEvent.from_dict(burn_ev).payload_hash, burn_expected)

    def test_client_docker_compose_defaults(self) -> None:
        cfg = HarnessConfig()
        expected_base_url = os.getenv("LEDGER_BASE_URL", "http://localhost:8080")
        expected_port = os.getenv("POSTGRES_HOST_PORT", "5433")
        expected_app_dsn = os.getenv(
            "LEDGER_APP_DSN",
            f"postgresql://ledger_app:{os.getenv('LEDGER_APP_PASSWORD', 'ledger_app_dev_pw')}@127.0.0.1:{expected_port}/ledger",
        )
        expected_admin_dsn = os.getenv(
            "LEDGER_ADMIN_DSN",
            f"postgresql://ledger_admin:{os.getenv('LEDGER_ADMIN_PASSWORD', 'ledger_admin_dev_pw')}@127.0.0.1:{expected_port}/ledger",
        )
        self.assertEqual(cfg.base_url, expected_base_url)
        self.assertEqual(cfg.app_dsn, expected_app_dsn)
        self.assertEqual(cfg.admin_dsn, expected_admin_dsn)
        u1, u2, distinct = cfg.endpoint_pair()
        self.assertEqual(u1, os.getenv("LEDGER_REPLICA1_URL", "") or expected_base_url)
        self.assertEqual(u2, os.getenv("LEDGER_REPLICA2_URL", "") or expected_base_url)
        expected_distinct = bool(
            os.getenv("LEDGER_REPLICA1_URL", "").strip()
            and os.getenv("LEDGER_REPLICA2_URL", "").strip()
            and os.getenv("LEDGER_REPLICA1_URL", "").strip().rstrip("/")
            != os.getenv("LEDGER_REPLICA2_URL", "").strip().rstrip("/")
        )
        self.assertEqual(distinct, expected_distinct)

    def test_replica_pair_can_require_distinct_direct_urls(self) -> None:
        cfg = HarnessConfig(replica1_url="", replica2_url="")
        with self.assertRaisesRegex(ValueError, "requires distinct direct replica URLs"):
            cfg.endpoint_pair(require_distinct=True)

        cfg = HarnessConfig(
            replica1_url="http://localhost:8082",
            replica2_url="http://localhost:8083",
        )
        self.assertTrue(cfg.endpoint_pair(require_distinct=True)[2])

    def test_business_day_half_open_and_dst_spring_forward_fall_back(self) -> None:
        tz = "America/New_York"
        # Normal EST day (2026-01-15): cutoff is 2026-01-16 02:00 EST = 07:00 UTC
        self.assertEqual(
            business_day_cutoff_utc(date(2026, 1, 15), tz),
            datetime(2026, 1, 16, 7, 0, 0, tzinfo=timezone.utc),
        )
        # 01:59:59.999999 EST -> Jan 15; 02:00:00.000000 EST -> Jan 16
        d_before, _ = compute_business_day(
            datetime(2026, 1, 16, 6, 59, 59, 999999, tzinfo=timezone.utc), tz
        )
        d_at, _ = compute_business_day(
            datetime(2026, 1, 16, 7, 0, 0, 0, tzinfo=timezone.utc), tz
        )
        self.assertEqual(d_before, date(2026, 1, 15))
        self.assertEqual(d_at, date(2026, 1, 16))

        # Spring-forward gap: 2026-03-08 02:00 EST jumps to 03:00 EDT = 07:00 UTC
        spring_cutoff = business_day_cutoff_utc(date(2026, 3, 7), tz)
        self.assertEqual(spring_cutoff, datetime(2026, 3, 8, 7, 0, 0, tzinfo=timezone.utc))
        d_spring_before, _ = compute_business_day(spring_cutoff - timedelta(microseconds=1), tz)
        d_spring_at, _ = compute_business_day(spring_cutoff, tz)
        self.assertEqual(d_spring_before, date(2026, 3, 7))
        self.assertEqual(d_spring_at, date(2026, 3, 8))

        # Fall-back fold: 2026-11-01 01:00-01:59 occurs twice (05:xx UTC EDT and 06:xx UTC EST)
        # Both precede 02:00 EST = 07:00 UTC and must belong to 2026-10-31 business day.
        fall_cutoff = business_day_cutoff_utc(date(2026, 10, 31), tz)
        self.assertEqual(fall_cutoff, datetime(2026, 11, 1, 7, 0, 0, tzinfo=timezone.utc))
        d_fold0, _ = compute_business_day(datetime(2026, 11, 1, 5, 30, 0, tzinfo=timezone.utc), tz)
        d_fold1, _ = compute_business_day(datetime(2026, 11, 1, 6, 30, 0, tzinfo=timezone.utc), tz)
        d_after, _ = compute_business_day(datetime(2026, 11, 1, 7, 0, 0, tzinfo=timezone.utc), tz)
        self.assertEqual(d_fold0, date(2026, 10, 31))
        self.assertEqual(d_fold1, date(2026, 10, 31))
        self.assertEqual(d_after, date(2026, 11, 1))


class EconomicOracleTests(unittest.TestCase):
    def _base_oracle(self) -> EconomicOracle:
        o = EconomicOracle()
        o.seed_store("store-001", "America/New_York", ["store-001-term-01", "store-001-term-02"])
        o.seed_store("store-002", "America/Chicago", ["store-002-term-01"])
        o.seed_gift_card("gc-1", 4000, ["store-001", "store-002"])
        o.seed_loyalty_account("loy-1")
        return o

    def test_order_and_duplicate_invariance(self) -> None:
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        s1 = str(uuid.uuid4())
        s2 = str(uuid.uuid4())
        events = [
            make_sale_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0, amount_minor=5000, sale_id=s1),
            make_return_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0 + timedelta(minutes=3), parent_sale_id=s1, amount_minor=2000),
            make_sale_event(store_id="store-002", terminal_id="store-002-term-01", occurred_at=t0 + timedelta(minutes=1), amount_minor=3000, sale_id=s2),
            make_void_event(store_id="store-002", terminal_id="store-002-term-01", occurred_at=t0 + timedelta(minutes=2), parent_sale_id=s2, amount_minor=3000),
            make_giftcard_redeem_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0 + timedelta(minutes=4), card_id="gc-1", amount_minor=1500, epoch_gen=1, terminal_epoch_seq=1),
            make_loyalty_event(event_type="loyalty_earn", store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0 + timedelta(minutes=5), account_id="loy-1", points=500),
            make_loyalty_event(event_type="loyalty_burn", store_id="store-001", terminal_id="store-001-term-02", occurred_at=t0 + timedelta(minutes=6), account_id="loy-1", points=700, epoch_gen=1, terminal_epoch_seq=1),
        ]

        o_inorder = self._base_oracle()
        o_inorder.add_events(events)
        h_inorder = o_inorder.compute_economic_sha256()

        rng = random.Random(99)
        shuffled = list(events) + [dict(events[0]), dict(events[3]), dict(events[4])]
        rng.shuffle(shuffled)
        o_shuffled = self._base_oracle()
        o_shuffled.add_events(shuffled)
        h_shuffled = o_shuffled.compute_economic_sha256()

        self.assertEqual(h_inorder, h_shuffled)
        self.assertEqual(o_shuffled.sales[s1].status, "PARTIALLY_RETURNED")
        self.assertEqual(o_shuffled.sales[s2].status, "VOIDED")
        self.assertEqual(o_shuffled.gift_cards["gc-1"].balance_minor, 2500)
        self.assertEqual(o_shuffled.loyalty_accounts["loy-1"].balance, -200)
        self.assertEqual(o_shuffled.loyalty_accounts["loy-1"].status, "NEGATIVE_RESTRICTED")
        self.assertEqual(len(o_shuffled.loyalty_negative_flags), 1)

    def test_payload_mismatch_raises(self) -> None:
        o = self._base_oracle()
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        ev_id = str(uuid.uuid4())
        ev1 = make_sale_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0, amount_minor=1000, event_id=ev_id)
        ev2 = make_sale_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0, amount_minor=2000, event_id=ev_id)
        o.add_event(ev1)
        with self.assertRaises(PayloadMismatchError):
            o.add_event(ev2)

    def test_late_void_reclassification_and_duplicate_sale_id(self) -> None:
        o = self._base_oracle()
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        s1 = str(uuid.uuid4())
        sale1 = make_sale_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0, amount_minor=4000, sale_id=s1)
        dup_sale = make_sale_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0 + timedelta(seconds=10), amount_minor=9999, sale_id=s1)
        ret = make_return_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0 + timedelta(minutes=5), parent_sale_id=s1, amount_minor=1500)
        late_void = make_void_event(store_id="store-001", terminal_id="store-001-term-01", occurred_at=t0 + timedelta(minutes=2), parent_sale_id=s1, amount_minor=4000)

        o.add_events([sale1, ret, dup_sale, late_void])
        o.evaluate()
        self.assertEqual(o.sales[s1].status, "VOIDED")
        self.assertEqual(o.sales[s1].returned_amount_minor, 0)
        self.assertEqual(o.event_status[dup_sale["event_id"]], ("REJECTED_INVALID", "DUPLICATE_SALE_ID"))
        self.assertEqual(o.event_status[ret["event_id"]], ("REJECTED_INVALID", "CANNOT_RETURN_VOIDED_SALE"))
        self.assertEqual(o.event_status[late_void["event_id"]], ("APPLIED", None))
        tot = o.economic_day_totals[("store-001", date(2026, 6, 15))]
        self.assertEqual(tot.sales_minor, 4000)
        self.assertEqual(tot.returns_minor, 0)
        self.assertEqual(tot.voids_minor, 4000)
        self.assertEqual(tot.net_sales_minor, 0)

    def test_giftcard_offline_cap_and_6th_store_rejection(self) -> None:
        o = EconomicOracle()
        for i in range(1, 7):
            o.seed_store(f"store-{i:03d}", "America/New_York", [f"store-{i:03d}-term-01"])
        o.seed_gift_card("gc-40", 3000, [f"store-{i:03d}" for i in range(1, 6)])

        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        # 5 eligible stores each redeem $25.00 (2500) -> total $125.00; starting balance $30.00 -> -$95.00
        for i in range(1, 6):
            o.add_event(
                make_giftcard_redeem_event(
                    store_id=f"store-{i:03d}",
                    terminal_id=f"store-{i:03d}-term-01",
                    occurred_at=t0 + timedelta(seconds=i),
                    card_id="gc-40",
                    amount_minor=2500,
                    epoch_gen=1,
                    terminal_epoch_seq=1,
                )
            )
        # Attempt extra $1 at store-001 in same epoch -> OFFLINE_CAP_EXCEEDED
        over_cap_ev = make_giftcard_redeem_event(
            store_id="store-001",
            terminal_id="store-001-term-01",
            occurred_at=t0 + timedelta(seconds=10),
            card_id="gc-40",
            amount_minor=100,
            epoch_gen=1,
            terminal_epoch_seq=2,
        )
        # Attempt at 6th store -> OFFLINE_STORE_NOT_ELIGIBLE
        sixth_ev = make_giftcard_redeem_event(
            store_id="store-006",
            terminal_id="store-006-term-01",
            occurred_at=t0 + timedelta(seconds=11),
            card_id="gc-40",
            amount_minor=500,
            epoch_gen=1,
            terminal_epoch_seq=1,
        )
        o.add_events([over_cap_ev, sixth_ev])
        o.evaluate()
        self.assertEqual(o.gift_cards["gc-40"].balance_minor, 3000 - 12500)
        self.assertEqual(o.gift_cards["gc-40"].status, "OVERSPENT_FROZEN")
        self.assertEqual(o.event_status[over_cap_ev["event_id"]], ("REJECTED_INVALID", "OFFLINE_CAP_EXCEEDED"))
        self.assertEqual(o.event_status[sixth_ev["event_id"]], ("REJECTED_INVALID", "OFFLINE_STORE_NOT_ELIGIBLE"))
        self.assertEqual(len(o.giftcard_overspend_flags), 4)

    def test_loyalty_earn_back_resolves_negative_flag(self) -> None:
        o = self._base_oracle()
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        burn = make_loyalty_event(
            event_type="loyalty_burn",
            store_id="store-001",
            terminal_id="store-001-term-01",
            occurred_at=t0,
            account_id="loy-1",
            points=300,
            epoch_gen=1,
            terminal_epoch_seq=1,
        )
        earn = make_loyalty_event(
            event_type="loyalty_earn",
            store_id="store-001",
            terminal_id="store-001-term-01",
            occurred_at=t0 + timedelta(minutes=10),
            account_id="loy-1",
            points=500,
        )
        # Upload earn before burn and verify convergence
        o.add_events([earn, burn])
        o.evaluate()
        self.assertEqual(o.loyalty_accounts["loy-1"].balance, 200)
        self.assertEqual(o.loyalty_accounts["loy-1"].status, "ACTIVE")
        self.assertEqual(len(o.loyalty_negative_flags), 1)
        self.assertEqual(o.loyalty_negative_flags[0].resolved_by_event_id, earn["event_id"])

    def test_topological_batch_sort_order(self) -> None:
        t0 = datetime(2026, 6, 15, 14, 0, 0, tzinfo=timezone.utc)
        sid = str(uuid.uuid4())
        gc = make_giftcard_redeem_event(store_id="store-001", terminal_id="t1", occurred_at=t0, card_id="gc-1", amount_minor=100)
        ret = make_return_event(store_id="store-001", terminal_id="t1", occurred_at=t0 + timedelta(minutes=5), parent_sale_id=sid, amount_minor=100)
        void = make_void_event(store_id="store-001", terminal_id="t1", occurred_at=t0 + timedelta(minutes=2), parent_sale_id=sid, amount_minor=500)
        sale = make_sale_event(store_id="store-001", terminal_id="t1", occurred_at=t0 + timedelta(minutes=10), amount_minor=500, sale_id=sid)
        ordered = sorted([gc, ret, void, sale], key=topological_batch_sort_key)
        self.assertEqual([e["event_type"] for e in ordered], ["sale", "void", "return", "giftcard_redeem"])

    def test_chaos_workload_generator_and_oracle_smoke(self) -> None:
        workload = generate_chaos_workload(unique_count=2000, store_count=20, seed=42)
        self.assertEqual(len(workload.unique_events), 2000)
        self.assertEqual(len(workload.submitted_events), 2240)  # +12% dupes
        oracle_a = EconomicOracle()
        oracle_b = EconomicOracle()
        for st in workload.stores:
            oracle_a.seed_store(st["store_id"], st["iana_timezone"], st["terminal_ids"])
            oracle_b.seed_store(st["store_id"], st["iana_timezone"], st["terminal_ids"])
        for gc in workload.gift_cards:
            oracle_a.seed_gift_card(gc["card_id"], gc["initial_balance_minor"], gc["eligible_stores"])
            oracle_b.seed_gift_card(gc["card_id"], gc["initial_balance_minor"], gc["eligible_stores"])
        for la in workload.loyalty_accounts:
            oracle_a.seed_loyalty_account(la["account_id"])
            oracle_b.seed_loyalty_account(la["account_id"])

        oracle_a.add_events(workload.unique_events)
        oracle_b.add_events(workload.submitted_events)
        self.assertEqual(oracle_a.compute_economic_sha256(), oracle_b.compute_economic_sha256())

    def test_chaos_outage_events_are_buffered_within_six_hour_window(self) -> None:
        workload = generate_chaos_workload(
            unique_count=1000,
            store_count=10,
            outage_stores=2,
            terminals_per_store=2,
            seed=43,
        )
        outage_stores = set(workload.outage_store_ids)
        outage_events = [event for event in workload.unique_events if event["store_id"] in outage_stores]

        self.assertTrue(outage_events)
        for event in outage_events:
            occurred_at = datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
            self.assertGreaterEqual(occurred_at, workload.simulated_now - timedelta(hours=6))
            self.assertLessEqual(occurred_at, workload.simulated_now)


class OfflineSimulatorTests(unittest.TestCase):
    def _make_terminal(
        self,
        store_id: str = "store-001",
        terminal_id: str = "store-001-term-01",
        slice_minor: int = 1250,
        loyalty_slice_points: int | None = 2500,
    ) -> OfflineTerminalSimulator:
        t0 = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = OfflineTerminalClock(wall_utc=t0, boottime_ms=50_000, boot_id="boot-abc")
        sim = OfflineTerminalSimulator(store_id=store_id, terminal_id=terminal_id, clock=clock)
        pre_boot, pre_ms = sim.begin_sync_request()
        clock.advance(0.25)  # 250ms RTT
        snaps = {
            "gc-1": CardSnapshot("gc-1", "ACTIVE", ("store-001", "store-002"), 5000),
            "gc-2": CardSnapshot("gc-2", "ACTIVE", ("store-001", "store-002"), 5000),
            "gc-frozen": CardSnapshot("gc-frozen", "OVERSPENT_FROZEN", ("store-001",), 0),
        }
        sim.apply_sync_response(
            pre_boot_id=pre_boot,
            pre_boottime_ms=pre_ms,
            epoch_gen=1,
            slice_minor=slice_minor,
            loyalty_slice_points=loyalty_slice_points,
            issued_at_utc=t0,
            slice_expires_at_utc=t0 + timedelta(hours=6),
            card_snapshots=snaps,
        )
        return sim

    def test_per_card_quota_template_and_contiguous_seq(self) -> None:
        sim = self._make_terminal(slice_minor=1250)
        ev1 = sim.redeem_giftcard_offline(card_id="gc-1", amount_minor=1000)
        ev2 = sim.redeem_giftcard_offline(card_id="gc-1", amount_minor=250)
        with self.assertRaises(OfflineRuleError) as ctx:
            sim.redeem_giftcard_offline(card_id="gc-1", amount_minor=1)
        self.assertEqual(ctx.exception.code, "OFFLINE_SLICE_EXCEEDED")

        # Separate card gc-2 still has its full 1250 slice independently
        ev3 = sim.redeem_giftcard_offline(card_id="gc-2", amount_minor=1250)
        events, expected_ids, count = sim.drain_sync_manifest()
        self.assertEqual(count, 3)
        self.assertEqual([e["terminal_epoch_seq"] for e in events], [1, 2, 3])
        self.assertEqual(expected_ids, [ev1["event_id"], ev2["event_id"], ev3["event_id"]])

    def test_6th_store_and_frozen_card_rejected_offline(self) -> None:
        sim = self._make_terminal(store_id="store-006", terminal_id="store-006-term-01")
        with self.assertRaises(OfflineRuleError) as ctx:
            sim.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        self.assertEqual(ctx.exception.code, "OFFLINE_STORE_NOT_ELIGIBLE")

        sim1 = self._make_terminal(store_id="store-001", terminal_id="store-001-term-01")
        with self.assertRaises(OfflineRuleError) as ctx2:
            sim1.redeem_giftcard_offline(card_id="gc-frozen", amount_minor=100)
        self.assertEqual(ctx2.exception.code, "CARD_FROZEN")

    def test_boottime_ttl_expiry_reboot_rollback_and_rtt_guards(self) -> None:
        # 1. Drift-tightened TTL boundary via CLOCK_BOOTTIME (6h minus 1000 ppm = 21,578,400 ms)
        sim_ok = self._make_terminal()
        assert sim_ok.grant is not None
        sim_ok.clock.boottime_ms = sim_ok.grant.grant_boottime_ms + 21_578_399
        sim_ok.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)

        sim = self._make_terminal()
        assert sim.grant is not None
        sim.clock.boottime_ms = sim.grant.grant_boottime_ms + 21_578_400
        with self.assertRaises(OfflineRuleError) as ctx_ttl:
            sim.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        self.assertEqual(ctx_ttl.exception.code, "SLICE_BOOTTIME_TTL_EXPIRED")

        # 2. Offline reboot (boot_id changes) fails closed
        sim_reboot = self._make_terminal()
        sim_reboot.clock.reboot()
        with self.assertRaises(OfflineRuleError) as ctx_reb:
            sim_reboot.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        self.assertEqual(ctx_reb.exception.code, "OFFLINE_REBOOT_DETECTED")

        # 3. Backward wall-clock rollback permanently fences even if clock later recovers
        sim_back = self._make_terminal()
        sim_back.clock.advance(60)
        sim_back.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        recovered_wall = sim_back.clock.wall_utc + timedelta(minutes=1)
        sim_back.clock.tamper_wall_clock(sim_back.clock.wall_utc - timedelta(minutes=5))
        with self.assertRaises(OfflineRuleError) as ctx_hwm:
            sim_back.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        self.assertEqual(ctx_hwm.exception.code, "CLOCK_ROLLBACK_DETECTED")
        sim_back.clock.tamper_wall_clock(recovered_wall)
        with self.assertRaises(OfflineRuleError) as ctx_fenced:
            sim_back.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        self.assertEqual(ctx_fenced.exception.code, "LOCAL_SLICE_FENCED")

        # 4. Sync RTT > 30s discarded
        t0 = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = OfflineTerminalClock(wall_utc=t0)
        sim_rtt = OfflineTerminalSimulator("store-001", "t1", clock)
        pre_boot, pre_ms = sim_rtt.begin_sync_request()
        clock.advance(31.0)
        with self.assertRaises(OfflineRuleError) as ctx_rtt:
            sim_rtt.apply_sync_response(
                pre_boot_id=pre_boot,
                pre_boottime_ms=pre_ms,
                epoch_gen=1,
                slice_minor=1250,
                loyalty_slice_points=2500,
                issued_at_utc=t0,
                slice_expires_at_utc=t0 + timedelta(hours=6),
                card_snapshots={},
            )
        self.assertEqual(ctx_rtt.exception.code, "SYNC_RTT_EXCEEDED")

    def test_loyalty_cap_finite_vs_unlimited_and_multi_terminal_partitioning(self) -> None:
        sim_fin = self._make_terminal(loyalty_slice_points=1000)
        sim_fin.burn_loyalty_offline(account_id="a1", points=600)
        sim_fin.burn_loyalty_offline(account_id="a2", points=400)
        with self.assertRaises(OfflineRuleError) as ctx:
            sim_fin.burn_loyalty_offline(account_id="a3", points=1)
        self.assertEqual(ctx.exception.code, "OFFLINE_LOYALTY_CAP_EXCEEDED")

        sim_unlim = self._make_terminal(loyalty_slice_points=None)
        sim_unlim.burn_loyalty_offline(account_id="a1", points=100_000)
        self.assertEqual(sim_unlim.grant.total_loyalty_burned_points, 100_000)

        # 8 terminals -> floor(2500 / 8) = 312; 1 fenced (312 reserved) -> remaining 7 get floor(2188/7) = 312
        tids = [f"t-{i}" for i in range(1, 9)]
        e1 = partition_store_slices(store_id="store-001", terminal_ids=tids, epoch_gen=1)
        self.assertEqual(sum(g.slice_minor for g in e1.values()), 312 * 8)
        self.assertLessEqual(sum(g.slice_minor for g in e1.values()), 2500)

        e2 = partition_store_slices(
            store_id="store-001",
            terminal_ids=tids,
            epoch_gen=2,
            fenced_reserved_minor=312,
        )
        self.assertLessEqual(sum(g.slice_minor for g in e2.values()) + 312, 2500)

    def test_outage_window_simulation_within_and_beyond_6h(self) -> None:
        sim_ok = self._make_terminal(slice_minor=1250, loyalty_slice_points=2500)
        evs, ids, cnt = sim_ok.simulate_outage_window(
            outage_seconds=5 * 3600,
            giftcard_redemptions=[("gc-1", 600), ("gc-2", 400)],
            loyalty_burns=[("loy-1", 500)],
        )
        self.assertEqual(cnt, 3)
        self.assertEqual(len(evs), 3)
        self.assertEqual(len(ids), 3)
        self.assertEqual([e["terminal_epoch_seq"] for e in evs], [1, 2, 3])

        # Advancing beyond 6h (21,600s) triggers SLICE_BOOTTIME_TTL_EXPIRED on next spend
        sim_ok.clock.advance(3601)
        with self.assertRaises(OfflineRuleError) as ctx:
            sim_ok.redeem_giftcard_offline(card_id="gc-1", amount_minor=100)
        self.assertEqual(ctx.exception.code, "SLICE_BOOTTIME_TTL_EXPIRED")


if __name__ == "__main__":
    unittest.main()
