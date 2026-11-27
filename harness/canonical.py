"""Canonical serialization, business-day cutoffs, DST rules, and SHA-256 helpers.

Implements the exact locked Phase 1 (v10) §3.1, §3.3, §3.7 and Phase 2 (v10) §2.2
specifications in pure Python 3.12 with zero application-code imports.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

# RFC 4122 NameSpaceURL used by Phase 2 §2.1 day_close_event_id()
NAMESPACE_DAY_CLOSE = uuid.UUID("6ba7b811-9dad-11d1-80b4-00c04fd430c8")

# Topological phases defined in Phase 1 §3.1
EVENT_PHASE_ORDER: dict[str, int] = {
    "sale": 1,
    "return": 2,
    "void": 2,
    "giftcard_redeem": 3,
    "loyalty_earn": 3,
    "loyalty_burn": 3,
    "day_close": 4,
}


def parse_utc(value: str | datetime) -> datetime:
    """Parse an RFC3339 string or aware datetime into UTC datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_int(value: int | None) -> str:
    """Encode BIGINT/INT as base-10 ASCII or \\N if NULL."""
    if value is None:
        return r"\N"
    return str(int(value))


def fmt_bool(value: bool | None) -> str:
    """Encode BOOLEAN as 'true'/'false' or \\N if NULL."""
    if value is None:
        return r"\N"
    return "true" if value else "false"


def fmt_date(value: date | str | None) -> str:
    """Encode DATE as YYYY-MM-DD or \\N if NULL."""
    if value is None:
        return r"\N"
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(value).isoformat()


def fmt_timestamptz(value: datetime | str | None) -> str:
    """Encode TIMESTAMPTZ as UTC YYYY-MM-DDTHH:MM:SS.ffffffZ (6-digit microseconds)."""
    if value is None:
        return r"\N"
    dt = parse_utc(value)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond:06d}Z"


def fmt_text(value: str | None) -> str:
    """Encode TEXT as compact RFC 8259 JSON string literal without HTML escaping."""
    if value is None:
        return r"\N"
    return json.dumps(str(value), ensure_ascii=False, separators=(",", ":"))


def fmt_json_compact(obj: Any) -> str:
    """Serialize a JSON object canonically with sorted keys and no whitespace/HTML escaping."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def build_canonical_table_section(table_name: str, rows: Iterable[Sequence[str]]) -> str:
    """Format a canonical section '#TABLE:<table_name>\\n' + tab-delimited rows."""
    lines = [f"#TABLE:{table_name}"]
    for row in rows:
        lines.append("\t".join(row))
    return "\n".join(lines) + "\n"


def sha256_hex(data: str | bytes) -> str:
    """Compute lowercase SHA-256 hex digest."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(raw).hexdigest()


def day_close_event_id(store_id: str, business_day: date | str) -> uuid.UUID:
    """Compute deterministic UUIDv5 matching Phase 2 day_close_event_id()."""
    day_str = fmt_date(business_day)
    return uuid.uuid5(NAMESPACE_DAY_CLOSE, f"{store_id}:{day_str}")


def compute_payload_hash_bytes(
    event_type: str,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime | str,
    canonical_payload: dict[str, Any],
) -> bytes:
    """Compute 32-byte SHA-256 payload_hash per Phase 1 §3.1 and Phase 2 chk_day_close_fields.

    Preimage:
        event_type | store_id | terminal_id | YYYY-MM-DDTHH:MM:SS.ffffffZ | compact_sorted_json
    """
    ts_str = fmt_timestamptz(occurred_at)
    payload_str = fmt_json_compact(canonical_payload)
    preimage = f"{event_type}|{store_id}|{terminal_id}|{ts_str}|{payload_str}"
    return hashlib.sha256(preimage.encode("utf-8")).digest()


def compute_payload_hash_hex(
    event_type: str,
    store_id: str,
    terminal_id: str,
    occurred_at: datetime | str,
    canonical_payload: dict[str, Any],
) -> str:
    """Return lowercase hex digest of compute_payload_hash_bytes()."""
    return compute_payload_hash_bytes(
        event_type=event_type,
        store_id=store_id,
        terminal_id=terminal_id,
        occurred_at=occurred_at,
        canonical_payload=canonical_payload,
    ).hex()


def business_day_cutoff_utc(business_day: date, iana_timezone: str) -> datetime:
    """Return cutoff_utc(D) = 02:00:00.000000 store-local on calendar date D+1.

    DST handling (Phase 1 §3.3):
    - Spring-forward gap (02:00 local skipped -> e.g. 03:00 EDT): first valid UTC instant
      at or after 02:00 local on D+1.
    - Fall-back fold (01:00-01:59 occurs twice): 02:00 standard time is unambiguous.
    """
    tz = ZoneInfo(iana_timezone)
    next_cal_date = business_day + timedelta(days=1)
    naive_target = datetime.combine(next_cal_date, time(2, 0, 0))

    # Probe local midnight -> next day in UTC to resolve DST gaps unambiguously.
    # For a spring-forward gap where 02:00 does not exist (round-trip fails), scan forward
    # from the first UTC instant whose local time >= 02:00 on next_cal_date.
    cand_fold0 = naive_target.replace(tzinfo=tz, fold=0)
    cand_fold1 = naive_target.replace(tzinfo=tz, fold=1)

    # Check if naive_target exists in wall time via UTC -> local round-trip.
    for cand in (cand_fold0, cand_fold1):
        roundtrip = cand.astimezone(timezone.utc).astimezone(tz)
        if roundtrip.replace(tzinfo=None) == naive_target:
            # Prefer fold=1 on fold edge if ever ambiguous, though 02:00 itself is post-fold.
            return cand.astimezone(timezone.utc)

    # Spring-forward gap: find the exact transition instant at or after 02:00 local.
    # Start just before local 01:00 on next_cal_date and step at 1-minute then 1-second precision.
    probe_start = datetime.combine(next_cal_date, time(0, 0, 0)).replace(tzinfo=tz, fold=0).astimezone(timezone.utc)
    probe_end = probe_start + timedelta(hours=6)
    cur = probe_start
    while cur <= probe_end:
        loc = cur.astimezone(tz)
        if loc.date() > next_cal_date or (loc.date() == next_cal_date and loc.time() >= time(2, 0, 0)):
            return cur
        cur += timedelta(seconds=60)
    raise ValueError(f"Unable to resolve cutoff for {business_day} in {iana_timezone}")


def compute_business_day(instant_utc: datetime | str, iana_timezone: str) -> tuple[date, datetime]:
    """Return (business_day, cutoff_utc) for an instant using half-open [cutoff(D-1), cutoff(D))."""
    utc_dt = parse_utc(instant_utc)
    tz = ZoneInfo(iana_timezone)
    local_dt = utc_dt.astimezone(tz)
    cal_date = local_dt.date()

    # Local time before 02:00 belongs to previous calendar date's business day (subject to gap check).
    # Evaluate candidate business days around cal_date to respect exact half-open intervals.
    for delta in (-1, 0, 1):
        day_d = cal_date + timedelta(days=delta)
        start_utc = business_day_cutoff_utc(day_d - timedelta(days=1), iana_timezone)
        end_utc = business_day_cutoff_utc(day_d, iana_timezone)
        if start_utc <= utc_dt < end_utc:
            return day_d, end_utc

    raise ValueError(f"Could not map {utc_dt.isoformat()} into business day for {iana_timezone}")


def topological_batch_sort_key(event: dict[str, Any]) -> tuple[int, datetime, str]:
    """Return Phase 1 §3.1 pre-insert topological sort key: (phase, occurred_at, event_id)."""
    etype = event["event_type"]
    phase = EVENT_PHASE_ORDER.get(etype, 99)
    occ = parse_utc(event["occurred_at"])
    eid = str(uuid.UUID(str(event["event_id"])))
    return (phase, occ, eid)
