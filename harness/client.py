"""Reusable async HTTP and PostgreSQL client helpers for the verification harness.

Aligned with Person 1's merged Go backend & Docker Compose configuration:
  - Nginx load balancer base URL: http://localhost:8080 (env: LEDGER_BASE_URL)
  - Optional direct replica overrides: LEDGER_REPLICA1_URL / LEDGER_REPLICA2_URL
    (default to LEDGER_BASE_URL when direct replica ports are not exposed)
  - PostgreSQL host port: 5433 (env: POSTGRES_HOST_PORT or LEDGER_APP_DSN / LEDGER_ADMIN_DSN)
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import httpx

from harness.canonical import fmt_date, fmt_timestamptz

try:
    import psycopg  # type: ignore
except ImportError:  # pragma: no cover - optional until requirements.txt installed
    psycopg = None


def _default_dsn(role: str, default_pw_env: str, default_pw: str) -> str:
    pg_port = os.getenv("POSTGRES_HOST_PORT", "5433")
    pw = os.getenv(default_pw_env, default_pw)
    return f"postgresql://{role}:{pw}@127.0.0.1:{pg_port}/ledger"


@dataclass(frozen=True)
class HarnessConfig:
    base_url: str = os.getenv("LEDGER_BASE_URL", "http://localhost:8080")
    replica1_url: str = os.getenv("LEDGER_REPLICA1_URL", "")
    replica2_url: str = os.getenv("LEDGER_REPLICA2_URL", "")
    app_dsn: str = os.getenv(
        "LEDGER_APP_DSN",
        _default_dsn("ledger_app", "LEDGER_APP_PASSWORD", "ledger_app_dev_pw"),
    )
    admin_dsn: str = os.getenv(
        "LEDGER_ADMIN_DSN",
        _default_dsn("ledger_admin", "LEDGER_ADMIN_PASSWORD", "ledger_admin_dev_pw"),
    )
    integration_enabled: bool = os.getenv("LEDGER_INTEGRATION", "0") == "1"
    p1_merged: bool = os.getenv("LEDGER_P1_MERGED", "1") == "1"
    p2_merged: bool = os.getenv("LEDGER_P2_MERGED", "0") == "1"

    def endpoint_pair(self, *, require_distinct: bool = False) -> tuple[str, str, bool]:
        """Return (url_a, url_b, has_distinct_direct_replicas).

        When LEDGER_REPLICA1_URL / LEDGER_REPLICA2_URL are unset, both return base_url
        (Nginx least_conn) and has_distinct_direct_replicas is False.
        """
        u1 = self.replica1_url.strip() or self.base_url
        u2 = self.replica2_url.strip() or self.base_url
        distinct = bool(
            self.replica1_url.strip()
            and self.replica2_url.strip()
            and self.replica1_url.strip().rstrip("/") != self.replica2_url.strip().rstrip("/")
        )
        if require_distinct and not distinct:
            raise ValueError(
                "This integration test requires distinct direct replica URLs; set "
                "LEDGER_REPLICA1_URL and LEDGER_REPLICA2_URL to the published API replica ports."
            )
        return u1, u2, distinct


def integration_skip_reason(requires_p2: bool = False) -> str | None:
    """Return skip reason when live stack or required teammate features are unavailable."""
    cfg = HarnessConfig()
    if requires_p2 and not cfg.p2_merged:
        return (
            "Integration execution pending Person 2 features (/v1/admin/seed, /v1/ledger/checksum, "
            "/v1/admin/snapshots, /v1/admin/rebuild, /v1/giftcards/*, /v1/loyalty/*, /sync, /fence); "
            "set LEDGER_P2_MERGED=1 and LEDGER_INTEGRATION=1 once merged."
        )
    if not cfg.p1_merged:
        return "Integration execution pending Person 1 core backend (LEDGER_P1_MERGED=1)."
    if not cfg.integration_enabled:
        return (
            "Blocked on live stack: Docker/PostgreSQL not running in this environment "
            "(start `ALLOW_SIMULATED_CLOCK=true docker compose up -d --build` and set LEDGER_INTEGRATION=1)."
        )
    if psycopg is None:
        return "Blocked on missing Python dependency: install psycopg[binary,pool] from requirements.txt."
    return None


def _sim_headers(simulated_now: datetime | str | None) -> dict[str, str]:
    if simulated_now is None:
        return {}
    return {"X-Simulated-Now": fmt_timestamptz(simulated_now)}


class LedgerClient:
    """Async HTTP client targeting Nginx (:8080) or explicit replica URLs."""

    def __init__(self, base_url: str | None = None, timeout: float = 30.0) -> None:
        cfg = HarnessConfig()
        self.base_url = (base_url or cfg.base_url).rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=64),
        )

    async def __aenter__(self) -> "LedgerClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def healthz(self) -> httpx.Response:
        return await self._client.get("/healthz")

    async def post_batch(
        self,
        *,
        store_id: str,
        terminal_id: str,
        events: list[dict[str, Any]],
        batch_id: str | None = None,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        body = {
            "batch_id": batch_id or str(uuid.uuid4()),
            "store_id": store_id,
            "terminal_id": terminal_id,
            "events": events,
        }
        return await self._client.post(
            "/v1/batches", json=body, headers=_sim_headers(simulated_now)
        )

    async def online_redeem_giftcard(
        self,
        *,
        event_id: str,
        store_id: str,
        terminal_id: str,
        card_id: str,
        amount_minor: int,
        occurred_at: datetime | str,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        body = {
            "event_id": event_id,
            "store_id": store_id,
            "terminal_id": terminal_id,
            "card_id": card_id,
            "amount_minor": amount_minor,
            "occurred_at": fmt_timestamptz(occurred_at),
        }
        return await self._client.post(
            "/v1/giftcards/redeem", json=body, headers=_sim_headers(simulated_now)
        )

    async def online_burn_loyalty(
        self,
        *,
        event_id: str,
        store_id: str,
        terminal_id: str,
        account_id: str,
        points: int,
        occurred_at: datetime | str,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        body = {
            "event_id": event_id,
            "store_id": store_id,
            "terminal_id": terminal_id,
            "account_id": account_id,
            "points": points,
            "occurred_at": fmt_timestamptz(occurred_at),
        }
        return await self._client.post(
            "/v1/loyalty/burn", json=body, headers=_sim_headers(simulated_now)
        )

    async def close_business_day(
        self,
        *,
        store_id: str,
        business_day: date | str,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        day_str = fmt_date(business_day)
        return await self._client.post(
            f"/v1/stores/{store_id}/days/{day_str}/close",
            headers=_sim_headers(simulated_now),
        )

    async def sync_terminal(
        self,
        *,
        store_id: str,
        terminal_id: str,
        epoch_gen: int,
        events: list[dict[str, Any]],
        expected_event_ids: list[str],
        submitted_count: int,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        body = {
            "epoch_gen": epoch_gen,
            "events": events,
            "expected_event_ids": expected_event_ids,
            "submitted_count": submitted_count,
        }
        return await self._client.post(
            f"/v1/stores/{store_id}/terminals/{terminal_id}/sync",
            json=body,
            headers=_sim_headers(simulated_now),
        )

    async def fence_terminal(
        self,
        *,
        store_id: str,
        terminal_id: str,
        epoch_gen: int,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        body = {"epoch_gen": epoch_gen}
        return await self._client.post(
            f"/v1/stores/{store_id}/terminals/{terminal_id}/fence",
            json=body,
            headers=_sim_headers(simulated_now),
        )

    async def get_store_totals(self, *, store_id: str, day: date | str) -> httpx.Response:
        return await self._client.get(
            f"/v1/stores/{store_id}/totals", params={"day": fmt_date(day)}
        )

    async def get_giftcard(self, card_id: str) -> httpx.Response:
        return await self._client.get(f"/v1/giftcards/{card_id}")

    async def get_loyalty(self, account_id: str) -> httpx.Response:
        return await self._client.get(f"/v1/loyalty/{account_id}")

    async def get_checksum(self) -> httpx.Response:
        return await self._client.get("/v1/ledger/checksum")

    async def admin_seed(
        self,
        *,
        stores: list[dict[str, Any]] | None = None,
        gift_cards: list[dict[str, Any]] | None = None,
        loyalty_accounts: list[dict[str, Any]] | None = None,
        simulated_now: datetime | str | None = None,
    ) -> httpx.Response:
        body = {
            "stores": stores or [],
            "gift_cards": gift_cards or [],
            "loyalty_accounts": loyalty_accounts or [],
        }
        return await self._client.post(
            "/v1/admin/seed", json=body, headers=_sim_headers(simulated_now)
        )

    async def admin_capture_snapshot(self) -> httpx.Response:
        return await self._client.post("/v1/admin/snapshots")

    async def admin_rebuild(self, mode: str = "full") -> httpx.Response:
        if mode not in ("full", "snapshot"):
            raise ValueError(f"Unsupported rebuild mode: {mode}")
        return await self._client.post(
            "/v1/admin/rebuild",
            params={"mode": mode},
            timeout=110.0,
        )


class DbInspector:
    """Direct PostgreSQL invariant inspector (uses psycopg3 when available)."""

    def __init__(self, dsn: str) -> None:
        if psycopg is None:
            raise RuntimeError("psycopg is not installed; install requirements.txt first")
        self.dsn = dsn

    async def fetch_scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        async with await psycopg.AsyncConnection.connect(self.dsn) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                row = await cur.fetchone()
                return row[0] if row else None

    async def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        async with await psycopg.AsyncConnection.connect(self.dsn) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
                return await cur.fetchall()

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        async with await psycopg.AsyncConnection.connect(self.dsn) as conn:
            async with conn.cursor() as cur:
                await cur.execute(sql, params)
            await conn.commit()

    async def seed_store_sql(
        self,
        *,
        store_id: str,
        iana_timezone: str,
        terminal_ids: list[str],
        simulated_now: datetime | str | None = None,
    ) -> None:
        """Seed a store directly via Person 1's admin_seed_store() SQL procedure (ledger_admin)."""
        async with await psycopg.AsyncConnection.connect(self.dsn, autocommit=True) as conn:
            await conn.execute("UPDATE ledger_config SET allow_simulated_clock = true WHERE id = 1")
            if simulated_now is not None:
                await conn.execute(
                    "SELECT admin_seed_store(%s, %s, %s::text[], %s::timestamptz)",
                    (store_id, iana_timezone, terminal_ids, fmt_timestamptz(simulated_now)),
                )
            else:
                await conn.execute(
                    "SELECT admin_seed_store(%s, %s, %s::text[], NULL)",
                    (store_id, iana_timezone, terminal_ids),
                )
