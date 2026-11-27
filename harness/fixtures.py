"""Deterministic event builders, store/card topology fixtures, and chaos generators."""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from harness.canonical import fmt_timestamptz

CHAIN_TIMEZONES = (
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
)


def deterministic_uuid(seed_str: str) -> str:
    """Create a reproducible UUIDv5 from a seed label."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, seed_str))


def make_sale_event(
    *,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime,
    amount_minor: int,
    sale_id: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    eid = event_id or str(uuid.uuid4())
    sid = sale_id or eid
    return {
        "event_id": eid,
        "event_type": "sale",
        "store_id": store_id,
        "terminal_id": terminal_id,
        "occurred_at": fmt_timestamptz(occurred_at),
        "sale_id": sid,
        "amount_minor": amount_minor,
    }


def make_return_event(
    *,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime,
    parent_sale_id: str,
    amount_minor: int,
    event_id: str | None = None,
) -> dict[str, Any]:
    eid = event_id or str(uuid.uuid4())
    return {
        "event_id": eid,
        "event_type": "return",
        "store_id": store_id,
        "terminal_id": terminal_id,
        "occurred_at": fmt_timestamptz(occurred_at),
        "parent_sale_id": parent_sale_id,
        "amount_minor": amount_minor,
    }


def make_void_event(
    *,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime,
    parent_sale_id: str,
    amount_minor: int,
    event_id: str | None = None,
) -> dict[str, Any]:
    eid = event_id or str(uuid.uuid4())
    return {
        "event_id": eid,
        "event_type": "void",
        "store_id": store_id,
        "terminal_id": terminal_id,
        "occurred_at": fmt_timestamptz(occurred_at),
        "parent_sale_id": parent_sale_id,
        "amount_minor": amount_minor,
    }


def make_giftcard_redeem_event(
    *,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime,
    card_id: str,
    amount_minor: int,
    epoch_gen: int | None = 1,
    terminal_epoch_seq: int | None = 1,
    event_id: str | None = None,
) -> dict[str, Any]:
    eid = event_id or str(uuid.uuid4())
    ev: dict[str, Any] = {
        "event_id": eid,
        "event_type": "giftcard_redeem",
        "store_id": store_id,
        "terminal_id": terminal_id,
        "occurred_at": fmt_timestamptz(occurred_at),
        "card_id": card_id,
        "amount_minor": amount_minor,
    }
    if epoch_gen is not None:
        ev["epoch_gen"] = epoch_gen
    if terminal_epoch_seq is not None:
        ev["terminal_epoch_seq"] = terminal_epoch_seq
    return ev


def make_loyalty_event(
    *,
    event_type: str,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime,
    account_id: str,
    points: int,
    epoch_gen: int | None = None,
    terminal_epoch_seq: int | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    if event_type not in ("loyalty_earn", "loyalty_burn"):
        raise ValueError(f"Invalid loyalty event_type: {event_type}")
    eid = event_id or str(uuid.uuid4())
    ev: dict[str, Any] = {
        "event_id": eid,
        "event_type": event_type,
        "store_id": store_id,
        "terminal_id": terminal_id,
        "occurred_at": fmt_timestamptz(occurred_at),
        "account_id": account_id,
        "points": points,
    }
    if epoch_gen is not None:
        ev["epoch_gen"] = epoch_gen
    if terminal_epoch_seq is not None:
        ev["terminal_epoch_seq"] = terminal_epoch_seq
    return ev


def generate_chain_topology(
    store_count: int = 300, terminals_per_store: int = 8
) -> list[dict[str, Any]]:
    """Generate the 300-store, 2,400-terminal grocery chain topology."""
    stores = []
    for s_idx in range(1, store_count + 1):
        sid = f"store-{s_idx:03d}"
        tz = CHAIN_TIMEZONES[(s_idx - 1) % len(CHAIN_TIMEZONES)]
        tids = [f"{sid}-term-{t_idx:02d}" for t_idx in range(1, terminals_per_store + 1)]
        stores.append({"store_id": sid, "iana_timezone": tz, "terminal_ids": tids})
    return stores


@dataclass
class ChaosWorkload:
    stores: list[dict[str, Any]]
    gift_cards: list[dict[str, Any]]
    loyalty_accounts: list[dict[str, Any]]
    unique_events: list[dict[str, Any]]
    submitted_events: list[dict[str, Any]]  # shuffled + 12% duplicates
    orphan_event_ids: set[str]
    outage_store_ids: list[str]
    simulated_now: datetime


def generate_chaos_workload(
    *,
    unique_count: int = 100_000,
    duplicate_rate: float = 0.12,
    orphan_rate: float = 0.03,
    outage_stores: int = 20,
    store_count: int = 300,
    terminals_per_store: int = 8,
    giftcard_count: int = 200,
    loyalty_count: int = 500,
    seed: int = 20270101,
    anchor_now: datetime | None = None,
) -> ChaosWorkload:
    """Build deterministic chaos replay workload per Phase 1 §5 Invariant 1.

    - `unique_count` unique events (default 100,000) across 300 stores / 2,400 terminals
    - `duplicate_rate` exact duplicates (12% -> 112,000 total submitted)
    - `orphan_rate` returns/voids placed BEFORE their parent sale in delivery order
    - Shuffled within a 48h lateness window (`[anchor_now - 47h, anchor_now - 1m]`)
    - `outage_stores` designated stores emitting bursts after simulated 6h outages
    """
    rng = random.Random(seed)
    now = anchor_now or datetime(2026, 6, 15, 16, 0, 0, tzinfo=timezone.utc)
    stores = generate_chain_topology(store_count=store_count, terminals_per_store=terminals_per_store)
    outage_store_ids = [s["store_id"] for s in stores[:outage_stores]]

    gift_cards = []
    for c_idx in range(1, giftcard_count + 1):
        elig = sorted(rng.sample([s["store_id"] for s in stores], k=5))
        gift_cards.append(
            {
                "card_id": f"gc-{c_idx:05d}",
                "initial_balance_minor": rng.choice([1000, 2500, 5000, 10000, 25000]),
                "eligible_stores": elig,
            }
        )

    loyalty_accounts = [{"account_id": f"loy-{i:05d}"} for i in range(1, loyalty_count + 1)]

    # Track per-(store, terminal, epoch=1) contiguous terminal_epoch_seq for offline GC/loyalty burns
    term_epoch_seq: dict[tuple[str, str], int] = {}
    gc_store_spend: dict[tuple[str, str], int] = {}

    unique_events: list[dict[str, Any]] = []
    sale_records: list[tuple[str, str, str, datetime, int]] = []  # (sale_id, sid, tid, occ, amt)
    orphan_candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []  # (child, parent_sale)

    # First reserve ~60% sales, ~20% returns/voids, ~10% giftcards, ~10% loyalty
    target_sales = int(unique_count * 0.60)
    target_rv = int(unique_count * 0.20)
    target_gc = int(unique_count * 0.10)
    target_loy = unique_count - target_sales - target_rv - target_gc

    for idx in range(target_sales):
        st = stores[idx % store_count]
        tid = st["terminal_ids"][idx % terminals_per_store]
        # Outage stores concentrate timestamps within a 6h outage window inside last 46h
        if st["store_id"] in outage_store_ids:
            offset_sec = rng.randint(60, 6 * 3600)
        else:
            offset_sec = rng.randint(60, 46 * 3600)
        occ = now - timedelta(seconds=offset_sec)
        amt = rng.randint(200, 15000)
        sale_id = deterministic_uuid(f"chaos-{seed}-sale-{idx}")
        ev_id = deterministic_uuid(f"chaos-{seed}-ev-sale-{idx}")
        ev = make_sale_event(
            store_id=st["store_id"],
            terminal_id=tid,
            occurred_at=occ,
            amount_minor=amt,
            sale_id=sale_id,
            event_id=ev_id,
        )
        unique_events.append(ev)
        sale_records.append((sale_id, st["store_id"], tid, occ, amt))

    target_orphans = int(unique_count * orphan_rate)
    orphan_event_ids: set[str] = set()

    for idx in range(target_rv):
        sale_id, sid, tid, sale_occ, sale_amt = sale_records[idx % len(sale_records)]
        child_occ = min(now - timedelta(seconds=10), sale_occ + timedelta(seconds=rng.randint(5, 1800)))
        ev_id = deterministic_uuid(f"chaos-{seed}-ev-rv-{idx}")
        if idx % 5 == 0:
            child_ev = make_void_event(
                store_id=sid,
                terminal_id=tid,
                occurred_at=child_occ,
                parent_sale_id=sale_id,
                amount_minor=sale_amt,
                event_id=ev_id,
            )
        else:
            ret_amt = max(100, sale_amt // 2)
            child_ev = make_return_event(
                store_id=sid,
                terminal_id=tid,
                occurred_at=child_occ,
                parent_sale_id=sale_id,
                amount_minor=ret_amt,
                event_id=ev_id,
            )
        unique_events.append(child_ev)
        if len(orphan_event_ids) < target_orphans:
            orphan_event_ids.add(ev_id)
            parent_ev = unique_events[idx % len(sale_records)]
            orphan_candidates.append((child_ev, parent_ev))

    for idx in range(target_gc):
        card = gift_cards[idx % giftcard_count]
        sid = card["eligible_stores"][idx % len(card["eligible_stores"])]
        tid = f"{sid}-term-{(idx % terminals_per_store) + 1:02d}"
        used = gc_store_spend.get((card["card_id"], sid), 0)
        amt = rng.randint(50, 500)
        if used + amt > 2500:
            # Reset to another card/store or allow some cap-rejection coverage
            amt = rng.randint(50, 250)
        gc_store_spend[(card["card_id"], sid)] = used + amt
        seq = term_epoch_seq.get((sid, tid), 0) + 1
        term_epoch_seq[(sid, tid)] = seq
        occ = now - timedelta(seconds=rng.randint(60, 46 * 3600))
        ev_id = deterministic_uuid(f"chaos-{seed}-ev-gc-{idx}")
        unique_events.append(
            make_giftcard_redeem_event(
                store_id=sid,
                terminal_id=tid,
                occurred_at=occ,
                card_id=card["card_id"],
                amount_minor=amt,
                epoch_gen=1,
                terminal_epoch_seq=seq,
                event_id=ev_id,
            )
        )

    for idx in range(target_loy):
        st = stores[idx % store_count]
        sid = st["store_id"]
        tid = st["terminal_ids"][idx % terminals_per_store]
        acct = loyalty_accounts[idx % loyalty_count]["account_id"]
        occ = now - timedelta(seconds=rng.randint(60, 46 * 3600))
        ev_id = deterministic_uuid(f"chaos-{seed}-ev-loy-{idx}")
        if idx % 3 == 0:
            seq = term_epoch_seq.get((sid, tid), 0) + 1
            term_epoch_seq[(sid, tid)] = seq
            unique_events.append(
                make_loyalty_event(
                    event_type="loyalty_burn",
                    store_id=sid,
                    terminal_id=tid,
                    occurred_at=occ,
                    account_id=acct,
                    points=rng.randint(10, 150),
                    epoch_gen=1,
                    terminal_epoch_seq=seq,
                    event_id=ev_id,
                )
            )
        else:
            unique_events.append(
                make_loyalty_event(
                    event_type="loyalty_earn",
                    store_id=sid,
                    terminal_id=tid,
                    occurred_at=occ,
                    account_id=acct,
                    points=rng.randint(50, 500),
                    event_id=ev_id,
                )
            )

    # Shuffle delivery stream, force designated orphans ahead of their parents, and inject 12% dupes.
    dup_count = int(round(len(unique_events) * duplicate_rate))
    duplicates = [dict(rng.choice(unique_events)) for _ in range(dup_count)]
    combined = list(unique_events) + duplicates
    rng.shuffle(combined)

    if orphan_event_ids:
        orphan_front = [e for e in combined if e["event_id"] in orphan_event_ids]
        parent_ids = {p["event_id"] for _, p in orphan_candidates}
        middle = [
            e
            for e in combined
            if e["event_id"] not in orphan_event_ids and e["event_id"] not in parent_ids
        ]
        parents_tail = [e for e in combined if e["event_id"] in parent_ids]
        submitted_events = orphan_front + middle + parents_tail
    else:
        submitted_events = combined

    return ChaosWorkload(
        stores=stores,
        gift_cards=gift_cards,
        loyalty_accounts=loyalty_accounts,
        unique_events=unique_events,
        submitted_events=submitted_events,
        orphan_event_ids=orphan_event_ids,
        outage_store_ids=outage_store_ids,
        simulated_now=now,
    )
