"""Independent Python economic_sha256 Reference Oracle.

Evaluates accepted/reconciled ledger events in strict canonical order
(Phase 1 v10 §3.1-§3.7, Phase 2 v10 §3.1-§3.3) with zero shared application imports.

Produces the exact 4-section economic_sha256 digest:
  1. #TABLE:economic_store_day_totals (ORDER BY store_id ASC, origin_business_day ASC)
  2. #TABLE:sales                     (ORDER BY sale_id ASC)
  3. #TABLE:gift_cards                (ORDER BY card_id ASC)
  4. #TABLE:loyalty_accounts          (ORDER BY account_id ASC)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable

from harness.canonical import (
    EVENT_PHASE_ORDER,
    build_canonical_table_section,
    compute_business_day,
    compute_payload_hash_bytes,
    fmt_date,
    fmt_int,
    fmt_text,
    parse_utc,
    sha256_hex,
)


@dataclass(frozen=True)
class OracleStore:
    store_id: str
    iana_timezone: str
    terminal_ids: tuple[str, ...] = ()


@dataclass
class OracleGiftCard:
    card_id: str
    initial_balance_minor: int
    balance_minor: int
    status: str  # 'ACTIVE' | 'OVERSPENT_FROZEN' | 'ADMIN_FROZEN'
    eligible_stores: tuple[str, ...]


@dataclass
class OracleGiftCardOverspendFlag:
    card_id: str
    event_id: str
    store_id: str
    balance_after_minor: int
    overspend_delta_minor: int


@dataclass
class OracleLoyaltyAccount:
    account_id: str
    earned_total: int = 0
    burned_total: int = 0
    balance: int = 0
    status: str = "ACTIVE"


@dataclass
class OracleLoyaltyNegativeFlag:
    account_id: str
    trigger_event_id: str
    deficit_points: int
    resolved_by_event_id: str | None = None


@dataclass
class OracleSale:
    sale_id: str
    store_id: str | None = None
    origin_business_day: date | None = None
    status: str = "PLACEHOLDER"
    original_amount_minor: int = 0
    returned_amount_minor: int = 0
    primary_event_id: str | None = None


@dataclass
class OracleEconomicDayTotal:
    store_id: str
    origin_business_day: date
    sales_minor: int = 0
    returns_minor: int = 0
    voids_minor: int = 0
    giftcard_redeem_minor: int = 0

    @property
    def net_sales_minor(self) -> int:
        # Phase 1 §3.7 & Phase 2 §3.2: giftcard_redeem_minor does not alter net_sales_minor.
        return self.sales_minor - self.returns_minor - self.voids_minor


@dataclass(frozen=True)
class OracleEvent:
    event_id: str
    event_type: str
    channel: str  # 'ONLINE' | 'OFFLINE' | 'SYSTEM'
    store_id: str
    terminal_id: str
    occurred_at: datetime
    payload_hash: bytes
    sale_id: str | None = None
    parent_sale_id: str | None = None
    card_id: str | None = None
    account_id: str | None = None
    amount_minor: int | None = None
    points: int | None = None
    epoch_gen: int | None = None
    terminal_epoch_seq: int | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "OracleEvent":
        etype = raw["event_type"]
        eid = str(uuid.UUID(str(raw["event_id"])))
        store_id = str(raw["store_id"])
        terminal_id = str(raw["terminal_id"])
        occ = parse_utc(raw["occurred_at"])
        channel = str(raw.get("channel", "OFFLINE"))

        sale_id = str(uuid.UUID(str(raw["sale_id"]))) if raw.get("sale_id") else None
        parent_sale_id = (
            str(uuid.UUID(str(raw["parent_sale_id"]))) if raw.get("parent_sale_id") else None
        )
        if etype in ("return", "void") and parent_sale_id is not None:
            sale_id = parent_sale_id

        card_id = str(raw["card_id"]) if raw.get("card_id") is not None else None
        account_id = str(raw["account_id"]) if raw.get("account_id") is not None else None
        amount_minor = int(raw["amount_minor"]) if raw.get("amount_minor") is not None else None
        points = int(raw["points"]) if raw.get("points") is not None else None
        epoch_gen = int(raw["epoch_gen"]) if raw.get("epoch_gen") is not None else None
        terminal_epoch_seq = (
            int(raw["terminal_epoch_seq"]) if raw.get("terminal_epoch_seq") is not None else None
        )

        if "payload_hash" in raw and isinstance(raw["payload_hash"], (bytes, bytearray)):
            phash = bytes(raw["payload_hash"])
        elif "payload_hash" in raw and isinstance(raw["payload_hash"], str):
            phash = bytes.fromhex(raw["payload_hash"])
        else:
            canonical_payload = raw.get("payload")
            if canonical_payload is None:
                canonical_payload = raw.get("payload_json")
            if canonical_payload is None:
                canonical_payload = _default_canonical_payload(
                    etype=etype,
                    store_id=store_id,
                    sale_id=sale_id,
                    parent_sale_id=parent_sale_id,
                    card_id=card_id,
                    account_id=account_id,
                    amount_minor=amount_minor,
                    points=points,
                    epoch_gen=epoch_gen,
                    terminal_epoch_seq=terminal_epoch_seq,
                    raw=raw,
                )
            else:
                # Overlay normalized keys identically to PrepareOfflineEvents in internal/ingest/batch.go
                canonical_payload = dict(canonical_payload)
                if sale_id is not None:
                    canonical_payload["sale_id"] = sale_id
                if parent_sale_id is not None:
                    canonical_payload["parent_sale_id"] = parent_sale_id
                if card_id is not None:
                    canonical_payload["card_id"] = card_id
                if account_id is not None:
                    canonical_payload["account_id"] = account_id
                if amount_minor is not None:
                    canonical_payload["amount_minor"] = amount_minor
                if points is not None:
                    canonical_payload["points"] = points
                if epoch_gen is not None:
                    canonical_payload["epoch_gen"] = epoch_gen
                if terminal_epoch_seq is not None:
                    canonical_payload["terminal_epoch_seq"] = terminal_epoch_seq
            phash = compute_payload_hash_bytes(
                event_type=etype,
                store_id=store_id,
                terminal_id=terminal_id,
                occurred_at=occ,
                canonical_payload=canonical_payload,
            )

        return cls(
            event_id=eid,
            event_type=etype,
            channel=channel,
            store_id=store_id,
            terminal_id=terminal_id,
            occurred_at=occ,
            payload_hash=phash,
            sale_id=sale_id,
            parent_sale_id=parent_sale_id,
            card_id=card_id,
            account_id=account_id,
            amount_minor=amount_minor,
            points=points,
            epoch_gen=epoch_gen,
            terminal_epoch_seq=terminal_epoch_seq,
        )


def _default_canonical_payload(
    *,
    etype: str,
    store_id: str,
    sale_id: str | None,
    parent_sale_id: str | None,
    card_id: str | None,
    account_id: str | None,
    amount_minor: int | None,
    points: int | None,
    epoch_gen: int | None,
    terminal_epoch_seq: int | None,
    raw: dict[str, Any],
) -> dict[str, Any]:
    """Build canonical payload map matching PrepareOfflineEvents in internal/ingest/batch.go."""
    if etype == "sale":
        obj: dict[str, Any] = {"sale_id": sale_id, "amount_minor": amount_minor}
    elif etype in ("return", "void"):
        # Go sets pe.SaleID = &parentUUID and pe.ParentSaleID = &parentUUID and writes both keys.
        effective_sale_id = sale_id or parent_sale_id
        obj = {
            "sale_id": effective_sale_id,
            "parent_sale_id": parent_sale_id,
            "amount_minor": amount_minor,
        }
    elif etype == "giftcard_redeem":
        obj = {"card_id": card_id, "amount_minor": amount_minor}
    elif etype in ("loyalty_earn", "loyalty_burn"):
        obj = {"account_id": account_id, "points": points}
    elif etype == "day_close":
        return {"store_id": store_id, "business_day": str(raw.get("business_day", ""))}
    else:
        raise ValueError(f"Unsupported event_type: {etype}")

    if epoch_gen is not None:
        obj["epoch_gen"] = epoch_gen
    if terminal_epoch_seq is not None:
        obj["terminal_epoch_seq"] = terminal_epoch_seq
    return obj


class PayloadMismatchError(ValueError):
    """Raised when an existing event_id is re-submitted with a different payload_hash."""


class EconomicOracle:
    """In-order deterministic reference oracle producing economic_sha256."""

    def __init__(
        self,
        giftcard_offline_store_cap_minor: int = 2500,
        loyalty_offline_store_burn_cap: int = 5000,
    ) -> None:
        self.giftcard_offline_store_cap_minor = giftcard_offline_store_cap_minor
        self.loyalty_offline_store_burn_cap = loyalty_offline_store_burn_cap
        self.stores: dict[str, OracleStore] = {}
        self.gift_cards_seed: dict[str, tuple[int, tuple[str, ...], bool]] = {}
        self.loyalty_accounts_seed: set[str] = set()
        self.events_by_id: dict[str, OracleEvent] = {}

        # Computed read-model snapshots (updated on evaluate())
        self.sales: dict[str, OracleSale] = {}
        self.gift_cards: dict[str, OracleGiftCard] = {}
        self.giftcard_overspend_flags: list[OracleGiftCardOverspendFlag] = []
        self.loyalty_accounts: dict[str, OracleLoyaltyAccount] = {}
        self.loyalty_negative_flags: list[OracleLoyaltyNegativeFlag] = []
        self.economic_day_totals: dict[tuple[str, date], OracleEconomicDayTotal] = {}
        self.event_status: dict[str, tuple[str, str | None]] = {}

    def seed_store(
        self, store_id: str, iana_timezone: str, terminal_ids: Iterable[str] = ()
    ) -> None:
        if store_id in self.stores and self.stores[store_id].iana_timezone != iana_timezone:
            raise ValueError(f"STORE_TIMEZONE_CONFLICT for {store_id}")
        self.stores[store_id] = OracleStore(
            store_id=store_id,
            iana_timezone=iana_timezone,
            terminal_ids=tuple(terminal_ids),
        )

    def seed_gift_card(
        self,
        card_id: str,
        initial_balance_minor: int,
        eligible_stores: Iterable[str],
        admin_frozen: bool = False,
    ) -> None:
        stores_tuple = tuple(eligible_stores)
        if not (1 <= len(stores_tuple) <= 5):
            raise ValueError("eligible_stores must have cardinality 1..5")
        if initial_balance_minor < 0:
            raise ValueError("initial_balance_minor must be >= 0")
        if card_id in self.gift_cards_seed:
            prev_bal, prev_stores, _ = self.gift_cards_seed[card_id]
            if prev_bal != initial_balance_minor or prev_stores != stores_tuple:
                raise ValueError(f"GIFT_CARD_SEED_CONFLICT for {card_id}")
            return
        self.gift_cards_seed[card_id] = (initial_balance_minor, stores_tuple, admin_frozen)

    def seed_loyalty_account(self, account_id: str) -> None:
        self.loyalty_accounts_seed.add(account_id)

    def add_event(self, raw_or_event: dict[str, Any] | OracleEvent) -> bool:
        """Idempotently record an accepted/submitted event. Returns True if newly added."""
        ev = (
            raw_or_event
            if isinstance(raw_or_event, OracleEvent)
            else OracleEvent.from_dict(raw_or_event)
        )
        if ev.store_id not in self.stores:
            raise ValueError(f"Unknown store_id: {ev.store_id}")
        existing = self.events_by_id.get(ev.event_id)
        if existing is not None:
            if existing.payload_hash != ev.payload_hash:
                raise PayloadMismatchError(
                    f"EVENT_ID_PAYLOAD_MISMATCH for {ev.event_id}"
                )
            return False
        self.events_by_id[ev.event_id] = ev
        return True

    def add_events(self, events: Iterable[dict[str, Any] | OracleEvent]) -> int:
        added = 0
        for ev in events:
            if self.add_event(ev):
                added += 1
        return added

    def _origin_day(self, ev: OracleEvent) -> date:
        store = self.stores[ev.store_id]
        day, _ = compute_business_day(ev.occurred_at, store.iana_timezone)
        return day

    def _ensure_day_total(self, store_id: str, origin_day: date) -> OracleEconomicDayTotal:
        key = (store_id, origin_day)
        tot = self.economic_day_totals.get(key)
        if tot is None:
            tot = OracleEconomicDayTotal(store_id=store_id, origin_business_day=origin_day)
            self.economic_day_totals[key] = tot
        return tot

    def evaluate(self) -> None:
        """Recompute all economic read models deterministically in canonical order."""
        self.sales = {}
        self.giftcard_overspend_flags = []
        self.loyalty_negative_flags = []
        self.economic_day_totals = {}
        self.event_status = {}

        self.gift_cards = {
            cid: OracleGiftCard(
                card_id=cid,
                initial_balance_minor=init_bal,
                balance_minor=init_bal,
                status="ADMIN_FROZEN" if admin_frozen else "ACTIVE",
                eligible_stores=elig,
            )
            for cid, (init_bal, elig, admin_frozen) in self.gift_cards_seed.items()
        }
        self.loyalty_accounts = {
            aid: OracleLoyaltyAccount(account_id=aid)
            for aid in self.loyalty_accounts_seed
        }

        all_events = list(self.events_by_id.values())

        # 1. Sales / Returns / Voids grouped by sale_id, ordered (occurred_at ASC, event_id ASC)
        sale_groups: dict[str, list[OracleEvent]] = {}
        gc_events: list[OracleEvent] = []
        loyalty_events: list[OracleEvent] = []

        for ev in all_events:
            if ev.event_type in ("sale", "return", "void"):
                assert ev.sale_id is not None
                sale_groups.setdefault(ev.sale_id, []).append(ev)
            elif ev.event_type == "giftcard_redeem":
                gc_events.append(ev)
            elif ev.event_type in ("loyalty_earn", "loyalty_burn"):
                loyalty_events.append(ev)
            elif ev.event_type == "day_close":
                self.event_status[ev.event_id] = ("APPLIED", None)

        self._evaluate_sales(sale_groups)
        self._evaluate_giftcards(gc_events)
        self._evaluate_loyalty(loyalty_events)

    def _evaluate_sales(self, sale_groups: dict[str, list[OracleEvent]]) -> None:
        for sale_id, events in sorted(sale_groups.items()):
            # Phase 1 §3.1: within a sale group, earliest sale is primary;
            # overall canonical order is (occurred_at ASC, event_id ASC) with phase tie-break.
            events.sort(
                key=lambda e: (
                    e.occurred_at,
                    EVENT_PHASE_ORDER[e.event_type],
                    e.event_id,
                )
            )
            has_sale = any(e.event_type == "sale" for e in events)
            if not has_sale:
                # Orphan returns/voids parked, placeholder row excluded from economic_sha256?
                # Wait: Phase 1 §3.7 #TABLE:sales lists all rows in sales ordered by sale_id ASC.
                # Parked orphans create a PLACEHOLDER row in sales!
                self.sales[sale_id] = OracleSale(sale_id=sale_id, status="PLACEHOLDER")
                for e in events:
                    self.event_status[e.event_id] = ("PARKED", None)
                continue

            # Find earliest sale event in canonical (occurred_at, event_id) order
            sale_obj = OracleSale(sale_id=sale_id, status="PLACEHOLDER")
            # Wait: what if an orphan return/void has an earlier occurred_at than the primary sale?
            # Look at Phase 2 §3.1:
            # First row in prefix with event_type='sale' establishes ACTIVE if none existed before it;
            # any return/void whose occurred_at < primary sale's occurred_at has no sale in prefix before it!
            # Wait: let's match Phase 2 §3.1 exactly: in canonical (occurred_at ASC, event_id ASC) order,
            # if a return/void precedes the first sale in occurred_at order, it has no parent sale yet
            # unless the parent sale arrived with occurred_at <= return.occurred_at.
            # Wait: in Phase 1 §3.2, orphan return/void is only parked BEFORE parent sale arrives;
            # once parent sale arrives, can a valid return have occurred_at < sale.occurred_at? No, physically
            # a return/void of a sale occurs AFTER the sale, except clock skew. If return/void occurs before
            # sale in canonical order, there is no sale yet -> PARKED (or unresolved if no sale precedes it).
            # Still, if all returns/voids have occurred_at >= sale.occurred_at, canonical order processes sale first.
            for ev in events:
                amt = ev.amount_minor or 0
                if amt <= 0:
                    self.event_status[ev.event_id] = ("REJECTED_INVALID", "INVALID_AMOUNT")
                    continue

                if ev.event_type == "sale":
                    if sale_obj.status == "PLACEHOLDER":
                        oday = self._origin_day(ev)
                        sale_obj.status = "ACTIVE"
                        sale_obj.store_id = ev.store_id
                        sale_obj.origin_business_day = oday
                        sale_obj.original_amount_minor = amt
                        sale_obj.returned_amount_minor = 0
                        sale_obj.primary_event_id = ev.event_id
                        self._ensure_day_total(ev.store_id, oday).sales_minor += amt
                        self.event_status[ev.event_id] = ("APPLIED", None)
                    else:
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "DUPLICATE_SALE_ID")

                elif ev.event_type == "return":
                    if sale_obj.status == "PLACEHOLDER":
                        self.event_status[ev.event_id] = ("PARKED", None)
                    elif sale_obj.status == "VOIDED":
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "CANNOT_RETURN_VOIDED_SALE")
                    elif sale_obj.status == "FULLY_RETURNED":
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "ALREADY_FULLY_RETURNED")
                    elif sale_obj.returned_amount_minor + amt > sale_obj.original_amount_minor:
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "RETURN_EXCEEDS_REMAINING")
                    else:
                        sale_obj.returned_amount_minor += amt
                        if sale_obj.returned_amount_minor == sale_obj.original_amount_minor:
                            sale_obj.status = "FULLY_RETURNED"
                        else:
                            sale_obj.status = "PARTIALLY_RETURNED"
                        oday = self._origin_day(ev)
                        self._ensure_day_total(ev.store_id, oday).returns_minor += amt
                        self.event_status[ev.event_id] = ("APPLIED", None)

                elif ev.event_type == "void":
                    if sale_obj.status == "PLACEHOLDER":
                        self.event_status[ev.event_id] = ("PARKED", None)
                    elif sale_obj.status in ("PARTIALLY_RETURNED", "FULLY_RETURNED"):
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "CANNOT_VOID_RETURNED_SALE")
                    elif sale_obj.status == "VOIDED":
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "ALREADY_VOIDED")
                    elif ev.store_id != sale_obj.store_id:
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "VOID_STORE_MISMATCH")
                    elif amt != sale_obj.original_amount_minor:
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "INVALID_AMOUNT")
                    else:
                        sale_obj.status = "VOIDED"
                        oday = self._origin_day(ev)
                        self._ensure_day_total(ev.store_id, oday).voids_minor += amt
                        self.event_status[ev.event_id] = ("APPLIED", None)

            self.sales[sale_id] = sale_obj

    def _evaluate_giftcards(self, gc_events: list[OracleEvent]) -> None:
        # Per-card canonical order: (occurred_at ASC, event_id ASC)
        gc_events.sort(key=lambda e: (e.occurred_at, e.event_id))
        offline_epoch_spend: dict[tuple[str, str, int], int] = {}

        for ev in gc_events:
            assert ev.card_id is not None
            amt = ev.amount_minor or 0
            card = self.gift_cards.get(ev.card_id)
            if card is None:
                self.event_status[ev.event_id] = ("REJECTED_INVALID", "CARD_NOT_FOUND")
                continue
            if amt <= 0:
                self.event_status[ev.event_id] = ("REJECTED_INVALID", "INVALID_AMOUNT")
                continue

            if ev.channel == "OFFLINE":
                if ev.store_id not in card.eligible_stores:
                    self.event_status[ev.event_id] = ("REJECTED_INVALID", "OFFLINE_STORE_NOT_ELIGIBLE")
                    continue
                if ev.epoch_gen is None or ev.epoch_gen <= 0:
                    self.event_status[ev.event_id] = ("REJECTED_INVALID", "INVALID_EPOCH_GEN")
                    continue
                cap_key = (card.card_id, ev.store_id, ev.epoch_gen)
                prior = offline_epoch_spend.get(cap_key, 0)
                if prior + amt > self.giftcard_offline_store_cap_minor:
                    self.event_status[ev.event_id] = ("REJECTED_INVALID", "OFFLINE_CAP_EXCEEDED")
                    continue
                offline_epoch_spend[cap_key] = prior + amt
            else:
                # ONLINE channel: if accepted into ledger_events, it passed online balance/status checks
                # at admission time; still guard against non-positive or unknown card.
                pass

            card.balance_minor -= amt
            if card.balance_minor < 0:
                overspend_delta = min(amt, -card.balance_minor)
                self.giftcard_overspend_flags.append(
                    OracleGiftCardOverspendFlag(
                        card_id=card.card_id,
                        event_id=ev.event_id,
                        store_id=ev.store_id,
                        balance_after_minor=card.balance_minor,
                        overspend_delta_minor=overspend_delta,
                    )
                )
                if card.status != "ADMIN_FROZEN":
                    card.status = "OVERSPENT_FROZEN"

            oday = self._origin_day(ev)
            self._ensure_day_total(ev.store_id, oday).giftcard_redeem_minor += amt
            self.event_status[ev.event_id] = ("APPLIED", None)

    def _evaluate_loyalty(self, loyalty_events: list[OracleEvent]) -> None:
        # Canonical order: (occurred_at ASC, event_id ASC)
        loyalty_events.sort(key=lambda e: (e.occurred_at, e.event_id))
        store_epoch_burn: dict[tuple[str, int], int] = {}
        open_flags_by_account: dict[str, list[OracleLoyaltyNegativeFlag]] = {}

        for ev in loyalty_events:
            assert ev.account_id is not None
            pts = ev.points or 0
            acct = self.loyalty_accounts.get(ev.account_id)
            if acct is None:
                acct = OracleLoyaltyAccount(account_id=ev.account_id)
                self.loyalty_accounts[ev.account_id] = acct

            if pts <= 0:
                self.event_status[ev.event_id] = ("REJECTED_INVALID", "INVALID_AMOUNT")
                continue

            if ev.event_type == "loyalty_burn":
                if ev.channel == "OFFLINE" and self.loyalty_offline_store_burn_cap > 0:
                    if ev.epoch_gen is None or ev.epoch_gen <= 0:
                        self.event_status[ev.event_id] = ("REJECTED_INVALID", "INVALID_EPOCH_GEN")
                        continue
                    key = (ev.store_id, ev.epoch_gen)
                    prior = store_epoch_burn.get(key, 0)
                    if prior + pts > self.loyalty_offline_store_burn_cap:
                        self.event_status[ev.event_id] = (
                            "REJECTED_INVALID",
                            "OFFLINE_LOYALTY_CAP_EXCEEDED",
                        )
                        continue
                    store_epoch_burn[key] = prior + pts

                acct.burned_total += pts
                acct.balance = acct.earned_total - acct.burned_total
                if acct.balance < 0:
                    acct.status = "NEGATIVE_RESTRICTED"
                    flag = OracleLoyaltyNegativeFlag(
                        account_id=acct.account_id,
                        trigger_event_id=ev.event_id,
                        deficit_points=-acct.balance,
                        resolved_by_event_id=None,
                    )
                    self.loyalty_negative_flags.append(flag)
                    open_flags_by_account.setdefault(acct.account_id, []).append(flag)
                else:
                    acct.status = "ACTIVE"
                self.event_status[ev.event_id] = ("APPLIED", None)

            elif ev.event_type == "loyalty_earn":
                acct.earned_total += pts
                acct.balance = acct.earned_total - acct.burned_total
                if acct.balance >= 0:
                    acct.status = "ACTIVE"
                    for flag in open_flags_by_account.get(acct.account_id, []):
                        if flag.resolved_by_event_id is None:
                            flag.resolved_by_event_id = ev.event_id
                else:
                    acct.status = "NEGATIVE_RESTRICTED"
                self.event_status[ev.event_id] = ("APPLIED", None)

    def serialize_economic_canonical(self) -> str:
        """Serialize the exact 4-section economic payload per Phase 1 §3.7."""
        self.evaluate()

        # 1. #TABLE:economic_store_day_totals (ORDER BY store_id ASC, origin_business_day ASC)
        day_rows = []
        for (sid, oday) in sorted(self.economic_day_totals.keys()):
            tot = self.economic_day_totals[(sid, oday)]
            day_rows.append(
                (
                    fmt_text(tot.store_id),
                    fmt_date(tot.origin_business_day),
                    fmt_int(tot.sales_minor),
                    fmt_int(tot.returns_minor),
                    fmt_int(tot.voids_minor),
                    fmt_int(tot.giftcard_redeem_minor),
                    fmt_int(tot.net_sales_minor),
                )
            )
        sec1 = build_canonical_table_section("economic_store_day_totals", day_rows)

        # 2. #TABLE:sales (ORDER BY sale_id ASC)
        sale_rows = []
        for sale_id in sorted(self.sales.keys()):
            s = self.sales[sale_id]
            sale_rows.append(
                (
                    fmt_text(s.sale_id),
                    fmt_text(s.store_id),
                    fmt_date(s.origin_business_day),
                    fmt_text(s.status),
                    fmt_int(s.original_amount_minor),
                    fmt_int(s.returned_amount_minor),
                )
            )
        sec2 = build_canonical_table_section("sales", sale_rows)

        # 3. #TABLE:gift_cards (ORDER BY card_id ASC)
        gc_rows = []
        for card_id in sorted(self.gift_cards.keys()):
            gc = self.gift_cards[card_id]
            gc_rows.append(
                (
                    fmt_text(gc.card_id),
                    fmt_int(gc.balance_minor),
                    fmt_text(gc.status),
                )
            )
        sec3 = build_canonical_table_section("gift_cards", gc_rows)

        # 4. #TABLE:loyalty_accounts (ORDER BY account_id ASC)
        loy_rows = []
        for account_id in sorted(self.loyalty_accounts.keys()):
            acct = self.loyalty_accounts[account_id]
            loy_rows.append(
                (
                    fmt_text(acct.account_id),
                    fmt_int(acct.earned_total),
                    fmt_int(acct.burned_total),
                    fmt_int(acct.balance),
                    fmt_text(acct.status),
                )
            )
        sec4 = build_canonical_table_section("loyalty_accounts", loy_rows)

        return sec1 + sec2 + sec3 + sec4

    def compute_economic_sha256(self) -> str:
        """Compute exact Phase 1 §3.7 economic_sha256 hex digest."""
        return sha256_hex(self.serialize_economic_canonical())
