"""Independent Offline POS Terminal Simulator (mirrors pos_ruleset).

Implements Phase 1 (v10) §3.4 & Phase 2 (v10) §4 deterministic offline rules:
  - LocalSliceGrant with CLOCK_BOOTTIME TTL expiry (6h = 21,600,000 ms)
  - boot_id offline-reboot fail-closed guard
  - hwm_utc backward clock-rollback fail-closed guard
  - Sync RTT > 30s grant discard
  - 5-store eligibility cluster enforcement (OFFLINE_STORE_NOT_ELIGIBLE)
  - Per-card quota template (spent_by_card_minor[card_id] <= slice_minor)
  - Store/terminal loyalty burn cap (loyalty_slice_points: int or None for unlimited)
  - Contiguous 1..N terminal_epoch_seq event buffering and local fencing
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from harness.canonical import parse_utc

DEFAULT_TTL_MS = 6 * 3600 * 1000  # 21,600,000 ms
DEFAULT_MAX_DRIFT_PPM = 1000  # 0.1% bounded hardware oscillator drift (matches pos_ruleset)
DEFAULT_MAX_RTT_MS = 30 * 1000  # 30,000 ms


def effective_boottime_ttl_ms(
    ttl_ms: int = DEFAULT_TTL_MS, max_drift_ppm: int = DEFAULT_MAX_DRIFT_PPM
) -> int:
    """Return boottime TTL tightened by bounded oscillator drift (matches Go pos_ruleset)."""
    return ttl_ms - (ttl_ms * max_drift_ppm) // 1_000_000


class OfflineRuleError(Exception):
    """Raised when the deterministic offline ruleset rejects an action."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True)
class CardSnapshot:
    card_id: str
    status: str  # 'ACTIVE' | 'OVERSPENT_FROZEN' | 'ADMIN_FROZEN'
    eligible_stores: tuple[str, ...]
    snapshot_balance_minor: int | None = None


@dataclass
class LocalSliceGrant:
    store_id: str
    terminal_id: str
    epoch_gen: int
    slice_minor: int
    loyalty_slice_points: int | None  # None = unlimited
    issued_at_utc: datetime
    slice_expires_at_utc: datetime
    boot_id: str
    grant_boottime_ms: int
    hwm_utc: datetime
    ttl_ms: int = DEFAULT_TTL_MS
    local_fenced: bool = False
    spent_by_card_minor: dict[str, int] = field(default_factory=dict)
    total_loyalty_burned_points: int = 0
    terminal_epoch_seq: int = 0
    card_snapshots: dict[str, CardSnapshot] = field(default_factory=dict)


class OfflineTerminalClock:
    """Simulated terminal wall + CLOCK_BOOTTIME + boot_id state."""

    def __init__(
        self,
        wall_utc: datetime,
        boottime_ms: int = 10_000,
        boot_id: str = "boot-1",
    ) -> None:
        self.wall_utc = parse_utc(wall_utc)
        self.boottime_ms = boottime_ms
        self.boot_id = boot_id

    def advance(self, seconds: float) -> None:
        """Advance both wall clock and CLOCK_BOOTTIME (including across OS suspend)."""
        delta_ms = int(round(seconds * 1000))
        self.wall_utc = self.wall_utc + timedelta(milliseconds=delta_ms)
        self.boottime_ms += delta_ms

    def advance_with_drift(self, hq_seconds: float, terminal_ppm_bias: float) -> None:
        """Advance terminal boottime with oscillator drift (e.g. -1000 ppm = -0.001)."""
        factor = 1.0 + (terminal_ppm_bias / 1_000_000.0)
        delta_ms = int(round(hq_seconds * 1000 * factor))
        self.wall_utc = self.wall_utc + timedelta(seconds=hq_seconds)
        self.boottime_ms += delta_ms

    def reboot(self, new_boot_id: str | None = None) -> None:
        """Simulate terminal OS reboot while offline (resets boottime and rotates boot_id)."""
        self.boot_id = new_boot_id or f"boot-{uuid.uuid4().hex[:8]}"
        self.boottime_ms = 500

    def tamper_wall_clock(self, new_wall_utc: datetime) -> None:
        """Jump wall clock (e.g. backward rollback attack)."""
        self.wall_utc = parse_utc(new_wall_utc)


class OfflineTerminalSimulator:
    """Offline POS terminal state machine enforcing pos_ruleset invariants."""

    def __init__(
        self,
        store_id: str,
        terminal_id: str,
        clock: OfflineTerminalClock,
        grant: LocalSliceGrant | None = None,
    ) -> None:
        self.store_id = store_id
        self.terminal_id = terminal_id
        self.clock = clock
        self.grant = grant
        self.buffer: list[dict[str, Any]] = []

    def begin_sync_request(self) -> tuple[str, int]:
        """Pre-request monotonic timestamping (Phase 1 §3.4)."""
        return self.clock.boot_id, self.clock.boottime_ms

    def apply_sync_response(
        self,
        *,
        pre_boot_id: str,
        pre_boottime_ms: int,
        epoch_gen: int,
        slice_minor: int,
        loyalty_slice_points: int | None,
        issued_at_utc: datetime,
        slice_expires_at_utc: datetime,
        card_snapshots: dict[str, CardSnapshot],
        ttl_ms: int = DEFAULT_TTL_MS,
        max_rtt_ms: int = DEFAULT_MAX_RTT_MS,
    ) -> LocalSliceGrant:
        """Install a LocalSliceGrant unless sync RTT > 30s or terminal rebooted mid-RTT."""
        if self.clock.boot_id != pre_boot_id:
            raise OfflineRuleError("REBOOT_DURING_SYNC", "boot_id changed during sync")
        rtt_ms = self.clock.boottime_ms - pre_boottime_ms
        if rtt_ms < 0 or rtt_ms > max_rtt_ms:
            raise OfflineRuleError(
                "SYNC_RTT_EXCEEDED",
                f"sync RTT {rtt_ms}ms exceeds limit {max_rtt_ms}ms; grant discarded",
            )
        issued = parse_utc(issued_at_utc)
        expires = parse_utc(slice_expires_at_utc)
        grant = LocalSliceGrant(
            store_id=self.store_id,
            terminal_id=self.terminal_id,
            epoch_gen=epoch_gen,
            slice_minor=slice_minor,
            loyalty_slice_points=loyalty_slice_points,
            issued_at_utc=issued,
            slice_expires_at_utc=expires,
            boot_id=pre_boot_id,
            grant_boottime_ms=pre_boottime_ms,
            hwm_utc=max(self.clock.wall_utc, issued),
            ttl_ms=ttl_ms,
            local_fenced=False,
            card_snapshots=dict(card_snapshots),
        )
        self.grant = grant
        return grant

    def _validate_grant_alive(self) -> LocalSliceGrant:
        grant = self.grant
        if grant is None:
            raise OfflineRuleError("NO_ACTIVE_GRANT", "terminal holds no offline slice grant")
        if grant.local_fenced:
            raise OfflineRuleError("LOCAL_SLICE_FENCED", "terminal slice is locally fenced")
        if self.clock.boot_id != grant.boot_id:
            grant.local_fenced = True
            raise OfflineRuleError("OFFLINE_REBOOT_DETECTED", "boot_id changed while offline; fenced")
        if self.clock.wall_utc < grant.hwm_utc:
            grant.local_fenced = True
            raise OfflineRuleError(
                "CLOCK_ROLLBACK_DETECTED",
                f"wall_clock {self.clock.wall_utc.isoformat()} < hwm {grant.hwm_utc.isoformat()}",
            )
        elapsed_ms = self.clock.boottime_ms - grant.grant_boottime_ms
        eff_ttl_ms = effective_boottime_ttl_ms(grant.ttl_ms)
        if elapsed_ms < 0 or elapsed_ms >= eff_ttl_ms:
            grant.local_fenced = True
            raise OfflineRuleError(
                "SLICE_BOOTTIME_TTL_EXPIRED",
                f"elapsed {elapsed_ms}ms >= effective TTL {eff_ttl_ms}ms",
            )
        if self.clock.wall_utc >= grant.slice_expires_at_utc:
            grant.local_fenced = True
            raise OfflineRuleError(
                "SLICE_WALL_EXPIRED",
                f"wall_clock {self.clock.wall_utc.isoformat()} >= {grant.slice_expires_at_utc.isoformat()}",
            )
        grant.hwm_utc = self.clock.wall_utc
        return grant

    def redeem_giftcard_offline(
        self,
        *,
        card_id: str,
        amount_minor: int,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute deterministic offline gift-card redemption check and buffer event."""
        if amount_minor <= 0:
            raise OfflineRuleError("INVALID_AMOUNT", "amount_minor must be > 0")
        grant = self._validate_grant_alive()

        snap = grant.card_snapshots.get(card_id)
        if snap is None:
            raise OfflineRuleError("CARD_NOT_IN_SNAPSHOT", f"card {card_id} not in offline snapshot")
        if snap.status != "ACTIVE":
            raise OfflineRuleError("CARD_FROZEN", f"card {card_id} status is {snap.status}")
        if self.store_id not in snap.eligible_stores:
            raise OfflineRuleError(
                "OFFLINE_STORE_NOT_ELIGIBLE",
                f"store {self.store_id} not in eligible_stores {snap.eligible_stores}",
            )

        spent = grant.spent_by_card_minor.get(card_id, 0)
        card_cap = grant.slice_minor
        if snap.snapshot_balance_minor is not None:
            card_cap = min(snap.snapshot_balance_minor, grant.slice_minor)
        if spent + amount_minor > card_cap:
            raise OfflineRuleError(
                "OFFLINE_SLICE_EXCEEDED",
                f"spent {spent} + {amount_minor} > cap {card_cap} for card {card_id}",
            )

        grant.spent_by_card_minor[card_id] = spent + amount_minor
        grant.terminal_epoch_seq += 1
        eid = event_id or str(uuid.uuid4())
        occ = self.clock.wall_utc
        ev = {
            "event_id": eid,
            "event_type": "giftcard_redeem",
            "store_id": self.store_id,
            "terminal_id": self.terminal_id,
            "occurred_at": occ.strftime("%Y-%m-%dT%H:%M:%S.") + f"{occ.microsecond:06d}Z",
            "epoch_gen": grant.epoch_gen,
            "terminal_epoch_seq": grant.terminal_epoch_seq,
            "card_id": card_id,
            "amount_minor": amount_minor,
        }
        self.buffer.append(ev)
        return ev

    def burn_loyalty_offline(
        self,
        *,
        account_id: str,
        points: int,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Execute deterministic offline loyalty burn check and buffer event."""
        if points <= 0:
            raise OfflineRuleError("INVALID_AMOUNT", "points must be > 0")
        grant = self._validate_grant_alive()

        if grant.loyalty_slice_points is not None:
            if grant.total_loyalty_burned_points + points > grant.loyalty_slice_points:
                raise OfflineRuleError(
                    "OFFLINE_LOYALTY_CAP_EXCEEDED",
                    f"burned {grant.total_loyalty_burned_points} + {points} > {grant.loyalty_slice_points}",
                )

        grant.total_loyalty_burned_points += points
        grant.terminal_epoch_seq += 1
        eid = event_id or str(uuid.uuid4())
        occ = self.clock.wall_utc
        ev = {
            "event_id": eid,
            "event_type": "loyalty_burn",
            "store_id": self.store_id,
            "terminal_id": self.terminal_id,
            "occurred_at": occ.strftime("%Y-%m-%dT%H:%M:%S.") + f"{occ.microsecond:06d}Z",
            "epoch_gen": grant.epoch_gen,
            "terminal_epoch_seq": grant.terminal_epoch_seq,
            "account_id": account_id,
            "points": points,
        }
        self.buffer.append(ev)
        return ev

    def drain_sync_manifest(self) -> tuple[list[dict[str, Any]], list[str], int]:
        """Return (buffered_events, expected_event_ids, submitted_count) for /sync."""
        events = list(self.buffer)
        expected_ids = [str(e["event_id"]) for e in events]
        return events, expected_ids, len(events)

    def simulate_outage_window(
        self,
        *,
        outage_seconds: int,
        giftcard_redemptions: list[tuple[str, int]] = (),
        loyalty_burns: list[tuple[str, int]] = (),
        step_seconds: int = 30,
    ) -> tuple[list[dict[str, Any]], list[str], int]:
        """Simulate an offline outage window advancing CLOCK_BOOTTIME across buffered spends."""
        if outage_seconds < 0:
            raise ValueError("outage_seconds must be >= 0")
        elapsed = 0
        for card_id, amount_minor in giftcard_redemptions:
            if elapsed + step_seconds <= outage_seconds:
                self.clock.advance(step_seconds)
                elapsed += step_seconds
            self.redeem_giftcard_offline(card_id=card_id, amount_minor=amount_minor)
        for account_id, points in loyalty_burns:
            if elapsed + step_seconds <= outage_seconds:
                self.clock.advance(step_seconds)
                elapsed += step_seconds
            self.burn_loyalty_offline(account_id=account_id, points=points)
        if outage_seconds > elapsed:
            self.clock.advance(outage_seconds - elapsed)
        return self.drain_sync_manifest()


def partition_store_slices(
    *,
    store_id: str,
    terminal_ids: list[str],
    epoch_gen: int,
    store_cap_minor: int = 2500,
    fenced_reserved_minor: int = 0,
    loyalty_store_cap: int | None = 5000,
    fenced_reserved_loyalty: int = 0,
    issued_at_utc: datetime | None = None,
    ttl_seconds: int = 21600,
) -> dict[str, LocalSliceGrant]:
    """Compute non-overlapping multi-terminal slices matching Phase 1 §3.4 & Phase 2 §2.5."""
    if not terminal_ids:
        raise ValueError("terminal_ids must be non-empty")
    n = len(terminal_ids)
    avail_gc = max(0, store_cap_minor - fenced_reserved_minor)
    slice_gc = avail_gc // n
    if slice_gc * n + fenced_reserved_minor > store_cap_minor:
        raise AssertionError("Aggregate gift-card cap invariant violated")

    if loyalty_store_cap is None or loyalty_store_cap == 0:
        slice_loyalty: int | None = None
    else:
        avail_loy = max(0, loyalty_store_cap - fenced_reserved_loyalty)
        slice_loyalty = avail_loy // n

    issued = parse_utc(issued_at_utc or datetime.now(timezone.utc))
    expires = issued + timedelta(seconds=ttl_seconds)

    grants: dict[str, LocalSliceGrant] = {}
    for idx, tid in enumerate(sorted(terminal_ids)):
        grants[tid] = LocalSliceGrant(
            store_id=store_id,
            terminal_id=tid,
            epoch_gen=epoch_gen,
            slice_minor=slice_gc,
            loyalty_slice_points=slice_loyalty,
            issued_at_utc=issued,
            slice_expires_at_utc=expires,
            boot_id=f"boot-{store_id}-{tid}-{epoch_gen}",
            grant_boottime_ms=10_000 + idx,
            hwm_utc=issued,
            ttl_ms=ttl_seconds * 1000,
        )
    return grants
