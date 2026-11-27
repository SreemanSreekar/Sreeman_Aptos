import os
import uuid
from datetime import datetime, timezone

import httpx
import pytest

BASE_URL = os.getenv("LEDGER_BASE_URL", "http://localhost:8080")


def unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


@pytest.fixture(scope="module")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=15.0) as c:
        r = c.get("/healthz")
        assert r.status_code == 200
        yield c


def test_seed_giftcard_online_and_idempotency(client: httpx.Client):
    s1 = unique_id("S1")
    s2 = unique_id("S2")
    card_id = unique_id("GC")

    r = client.post(
        "/v1/admin/seed",
        json={
            "stores": [
                {"store_id": s1, "iana_timezone": "America/New_York", "terminal_ids": ["T1", "T2"]},
                {"store_id": s2, "iana_timezone": "America/New_York", "terminal_ids": ["T1"]},
            ],
            "gift_cards": [
                {"card_id": card_id, "initial_balance_minor": 3000, "eligible_stores": [s1, s2]}
            ],
        },
    )
    assert r.status_code == 200, r.text

    ev1 = str(uuid.uuid4())
    now = iso_now()
    payload = {
        "event_id": ev1,
        "store_id": s1,
        "terminal_id": "T1",
        "occurred_at": now,
        "card_id": card_id,
        "amount_minor": 2000,
    }
    r1 = client.post("/v1/giftcards/redeem", json=payload)
    assert r1.status_code == 200, r1.text
    assert r1.json()["balance_minor"] == 1000

    # Idempotent replay
    r2 = client.post("/v1/giftcards/redeem", json=payload)
    assert r2.status_code == 200, r2.text
    assert r2.json()["balance_minor"] == 1000

    # Conflicting payload on same event_id -> 409 EVENT_ID_PAYLOAD_MISMATCH
    bad = dict(payload)
    bad["amount_minor"] = 1500
    r3 = client.post("/v1/giftcards/redeem", json=bad)
    assert r3.status_code == 409, r3.text
    assert r3.json()["error"] == "EVENT_ID_PAYLOAD_MISMATCH"

    # Insufficient funds -> 409
    ev2 = str(uuid.uuid4())
    r4 = client.post(
        "/v1/giftcards/redeem",
        json={
            "event_id": ev2,
            "store_id": s1,
            "terminal_id": "T1",
            "occurred_at": iso_now(),
            "card_id": card_id,
            "amount_minor": 2000,
        },
    )
    assert r4.status_code == 409, r4.text
    assert r4.json()["error"] == "INSUFFICIENT_FUNDS"

    g = client.get(f"/v1/giftcards/{card_id}")
    assert g.status_code == 200
    assert g.json()["balance_minor"] == 1000
    assert g.json()["status"] == "ACTIVE"


def test_offline_giftcard_overspend_and_ineligible_store(client: httpx.Client):
    stores = [unique_id("GS") for _ in range(6)]
    card_id = unique_id("GCO")

    r = client.post(
        "/v1/admin/seed",
        json={
            "stores": [{"store_id": s, "iana_timezone": "UTC", "terminal_ids": ["T1"]} for s in stores],
            "gift_cards": [
                {"card_id": card_id, "initial_balance_minor": 3000, "eligible_stores": stores[:5]}
            ],
        },
    )
    assert r.status_code == 200, r.text

    # Spend $25 (2500) at 2 eligible offline stores -> balance = -2000, OVERSPENT_FROZEN
    for s in stores[:2]:
        rb = client.post(
            "/v1/batches",
            json={
                "store_id": s,
                "terminal_id": "T1",
                "events": [
                    {
                        "event_id": str(uuid.uuid4()),
                        "event_type": "giftcard_redeem",
                        "store_id": s,
                        "terminal_id": "T1",
                        "occurred_at": iso_now(),
                        "epoch_gen": 1,
                        "terminal_epoch_seq": 1,
                        "card_id": card_id,
                        "amount_minor": 2500,
                    }
                ],
            },
        )
        assert rb.status_code == 200, rb.text
        assert rb.json()["accepted"] == 1

    # 6th ineligible store -> rejected_invalid
    rb6 = client.post(
        "/v1/batches",
        json={
            "store_id": stores[5],
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "giftcard_redeem",
                    "store_id": stores[5],
                    "terminal_id": "T1",
                    "occurred_at": iso_now(),
                    "epoch_gen": 1,
                    "terminal_epoch_seq": 1,
                    "card_id": card_id,
                    "amount_minor": 1000,
                }
            ],
        },
    )
    assert rb6.status_code == 200, rb6.text
    assert rb6.json()["rejected_invalid"] == 1

    g = client.get(f"/v1/giftcards/{card_id}")
    assert g.status_code == 200
    data = g.json()
    assert data["balance_minor"] == -2000
    assert data["status"] == "OVERSPENT_FROZEN"
    assert len(data["overspend_flags"]) == 1
    assert data["overspend_flags"][0]["overspend_delta_minor"] == 2000


def test_loyalty_convergence_negative_and_recovery(client: httpx.Client):
    s1 = unique_id("LS1")
    s2 = unique_id("LS2")
    acc = unique_id("ACC")

    r = client.post(
        "/v1/admin/seed",
        json={
            "stores": [
                {"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1"]},
                {"store_id": s2, "iana_timezone": "UTC", "terminal_ids": ["T1"]},
            ],
            "loyalty_accounts": [{"account_id": acc}],
        },
    )
    assert r.status_code == 200, r.text

    # Earn 500, then offline burn 300 at s1 and 300 at s2 -> balance -100, NEGATIVE_RESTRICTED
    rb = client.post(
        "/v1/batches",
        json={
            "store_id": s1,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_earn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": iso_now(),
                    "account_id": acc,
                    "points": 500,
                },
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_burn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": iso_now(),
                    "epoch_gen": 1,
                    "terminal_epoch_seq": 1,
                    "account_id": acc,
                    "points": 300,
                },
            ],
        },
    )
    assert rb.status_code == 200 and rb.json()["accepted"] == 2

    rb2 = client.post(
        "/v1/batches",
        json={
            "store_id": s2,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_burn",
                    "store_id": s2,
                    "terminal_id": "T1",
                    "occurred_at": iso_now(),
                    "epoch_gen": 1,
                    "terminal_epoch_seq": 1,
                    "account_id": acc,
                    "points": 300,
                }
            ],
        },
    )
    assert rb2.status_code == 200 and rb2.json()["accepted"] == 1

    # Online burn must be blocked with 409 ACCOUNT_NEGATIVE_RESTRICTED
    ro = client.post(
        "/v1/loyalty/burn",
        json={
            "event_id": str(uuid.uuid4()),
            "store_id": s1,
            "terminal_id": "T1",
            "occurred_at": iso_now(),
            "account_id": acc,
            "points": 50,
        },
    )
    assert ro.status_code == 409
    assert ro.json()["error"] == "ACCOUNT_NEGATIVE_RESTRICTED"

    # Earn 200 -> recovers to ACTIVE (+100)
    rb3 = client.post(
        "/v1/batches",
        json={
            "store_id": s1,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_earn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": iso_now(),
                    "account_id": acc,
                    "points": 200,
                }
            ],
        },
    )
    assert rb3.status_code == 200 and rb3.json()["accepted"] == 1

    gl = client.get(f"/v1/loyalty/{acc}")
    assert gl.status_code == 200
    assert gl.json()["balance"] == 100
    assert gl.json()["status"] == "ACTIVE"


def test_epoch_sync_and_snapshot_rebuild(client: httpx.Client):
    s1 = unique_id("ES")
    r = client.post(
        "/v1/admin/seed",
        json={
            "stores": [{"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1", "T2"]}]
        },
    )
    assert r.status_code == 200, r.text

    # T1 syncs first -> BLOCKED_PENDING_SIBLINGS
    rs1 = client.post(f"/v1/stores/{s1}/terminals/T1/sync", json={"epoch_gen": 1, "events": []})
    assert rs1.status_code == 200, rs1.text
    assert rs1.json()["renewal_status"] == "BLOCKED_PENDING_SIBLINGS"

    # T2 syncs -> advances epoch to 2 and returns GRANTED
    rs2 = client.post(f"/v1/stores/{s1}/terminals/T2/sync", json={"epoch_gen": 1, "events": []})
    assert rs2.status_code == 200, rs2.text
    assert rs2.json()["renewal_status"] == "GRANTED"
    assert rs2.json()["active_epoch_gen"] == 2

    # Fence before TTL expires -> 409 SLICE_NOT_EXPIRED_YET
    rf = client.post(f"/v1/stores/{s1}/terminals/T1/fence", json={"epoch_gen": 2})
    assert rf.status_code == 409
    assert rf.json()["error"] == "SLICE_NOT_EXPIRED_YET"

    # Checksum -> Snapshot -> Full Rebuild -> Snapshot Rebuild must match bit-for-bit
    c1 = client.get("/v1/ledger/checksum").json()
    snap = client.post("/v1/admin/snapshots")
    assert snap.status_code == 200, snap.text
    snap_id = snap.json()["snapshot_id"]

    rf_full = client.post("/v1/admin/rebuild?mode=full")
    assert rf_full.status_code == 200, rf_full.text
    c2 = client.get("/v1/ledger/checksum").json()
    assert c2 == c1

    rf_snap = client.post(f"/v1/admin/rebuild?mode=snapshot&snapshot_id={snap_id}")
    assert rf_snap.status_code == 200, rf_snap.text
    c3 = client.get("/v1/ledger/checksum").json()
    assert c3 == c1
