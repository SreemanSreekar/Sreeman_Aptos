import concurrent.futures
import os
import uuid
from datetime import datetime, timedelta, timezone

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


def test_online_declines_and_loyalty_burn_rebuild_parity(client: httpx.Client):
    s1 = unique_id("OD")
    card_id = unique_id("GCOD")
    acc_id = unique_id("ACCOD")
    t_seed = "2026-06-10T10:00:00.000000Z"

    r = client.post(
        "/v1/admin/seed",
        headers={"X-Simulated-Now": t_seed},
        json={
            "stores": [{"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1"]}],
            "gift_cards": [{"card_id": card_id, "initial_balance_minor": 1000, "eligible_stores": [s1]}],
            "loyalty_accounts": [{"account_id": acc_id}],
        },
    )
    assert r.status_code == 200, r.text

    # Declined online gift-card redeem on a fresh day (no prior sales/day row)
    r_dec = client.post(
        "/v1/giftcards/redeem",
        headers={"X-Simulated-Now": "2026-06-11T12:00:00.000000Z"},
        json={
            "event_id": str(uuid.uuid4()),
            "store_id": s1,
            "terminal_id": "T1",
            "occurred_at": "2026-06-11T12:00:00.000000Z",
            "card_id": card_id,
            "amount_minor": 5000,
        },
    )
    assert r_dec.status_code == 409 and r_dec.json()["error"] == "INSUFFICIENT_FUNDS"

    # Earn 500 on Day 1, then approved online loyalty burn on Day 2 (fresh day with no financial events)
    rb = client.post(
        "/v1/batches",
        headers={"X-Simulated-Now": "2026-06-11T12:05:00.000000Z"},
        json={
            "store_id": s1,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_earn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": "2026-06-11T12:05:00.000000Z",
                    "account_id": acc_id,
                    "points": 500,
                }
            ],
        },
    )
    assert rb.status_code == 200 and rb.json()["accepted"] == 1

    r_burn = client.post(
        "/v1/loyalty/burn",
        headers={"X-Simulated-Now": "2026-06-12T12:00:00.000000Z"},
        json={
            "event_id": str(uuid.uuid4()),
            "store_id": s1,
            "terminal_id": "T1",
            "occurred_at": "2026-06-12T12:00:00.000000Z",
            "account_id": acc_id,
            "points": 200,
        },
    )
    assert r_burn.status_code == 200, r_burn.text
    assert r_burn.json()["balance"] == 300

    # Declined online loyalty burn on Day 3
    r_burn_dec = client.post(
        "/v1/loyalty/burn",
        headers={"X-Simulated-Now": "2026-06-13T12:00:00.000000Z"},
        json={
            "event_id": str(uuid.uuid4()),
            "store_id": s1,
            "terminal_id": "T1",
            "occurred_at": "2026-06-13T12:00:00.000000Z",
            "account_id": acc_id,
            "points": 900,
        },
    )
    assert r_burn_dec.status_code == 409 and r_burn_dec.json()["error"] == "INSUFFICIENT_POINTS"

    c_live = client.get("/v1/ledger/checksum").json()
    rf = client.post("/v1/admin/rebuild?mode=full")
    assert rf.status_code == 200, rf.text
    c_rebuilt = client.get("/v1/ledger/checksum").json()
    assert c_rebuilt == c_live


def test_concurrent_online_and_batch_race(client: httpx.Client):
    s1 = unique_id("RACE")
    card_id = unique_id("GCR")
    acc_id = unique_id("ACCR")

    r = client.post(
        "/v1/admin/seed",
        json={
            "stores": [{"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1"]}],
            "gift_cards": [{"card_id": card_id, "initial_balance_minor": 3000, "eligible_stores": [s1]}],
            "loyalty_accounts": [{"account_id": acc_id}],
        },
    )
    assert r.status_code == 200, r.text

    # Seed loyalty balance = 500
    rearn = client.post(
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
                    "account_id": acc_id,
                    "points": 500,
                }
            ],
        },
    )
    assert rearn.status_code == 200

    # Race 1: Concurrent online giftcard redeem + batch giftcard redeem with identical event_id
    gc_ev = str(uuid.uuid4())
    gc_ts = iso_now()

    def post_online_gc():
        with httpx.Client(base_url=BASE_URL, timeout=15.0) as c:
            return c.post(
                "/v1/giftcards/redeem",
                json={
                    "event_id": gc_ev,
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": gc_ts,
                    "card_id": card_id,
                    "amount_minor": 1000,
                },
            )

    def post_batch_gc():
        with httpx.Client(base_url=BASE_URL, timeout=15.0) as c:
            return c.post(
                "/v1/batches",
                json={
                    "store_id": s1,
                    "terminal_id": "T1",
                    "events": [
                        {
                            "event_id": gc_ev,
                            "event_type": "giftcard_redeem",
                            "store_id": s1,
                            "terminal_id": "T1",
                            "occurred_at": gc_ts,
                            "epoch_gen": 1,
                            "terminal_epoch_seq": 1,
                            "card_id": card_id,
                            "amount_minor": 1000,
                        }
                    ],
                },
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(post_online_gc)
        f2 = pool.submit(post_batch_gc)
        r_on, r_ba = f1.result(), f2.result()

    assert r_on.status_code == 200, r_on.text
    assert r_ba.status_code == 200, r_ba.text
    assert client.get(f"/v1/giftcards/{card_id}").json()["balance_minor"] == 2000

    # Race 2: Concurrent online loyalty burn + batch loyalty burn with identical event_id
    lb_ev = str(uuid.uuid4())
    lb_ts = iso_now()

    def post_online_lb():
        with httpx.Client(base_url=BASE_URL, timeout=15.0) as c:
            return c.post(
                "/v1/loyalty/burn",
                json={
                    "event_id": lb_ev,
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": lb_ts,
                    "account_id": acc_id,
                    "points": 200,
                },
            )

    def post_batch_lb():
        with httpx.Client(base_url=BASE_URL, timeout=15.0) as c:
            return c.post(
                "/v1/batches",
                json={
                    "store_id": s1,
                    "terminal_id": "T1",
                    "events": [
                        {
                            "event_id": lb_ev,
                            "event_type": "loyalty_burn",
                            "store_id": s1,
                            "terminal_id": "T1",
                            "occurred_at": lb_ts,
                            "epoch_gen": 1,
                            "terminal_epoch_seq": 2,
                            "account_id": acc_id,
                            "points": 200,
                        }
                    ],
                },
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(post_online_lb)
        f2 = pool.submit(post_batch_lb)
        r_on, r_ba = f1.result(), f2.result()

    assert r_on.status_code == 200, r_on.text
    assert r_ba.status_code == 200, r_ba.text
    assert client.get(f"/v1/loyalty/{acc_id}").json()["balance"] == 300


def test_backdated_online_loyalty_burn_rebuild_parity(client: httpx.Client):
    s1 = unique_id("BLB")
    acc_id = unique_id("ACCBLB")
    base = datetime(2026, 7, 1, 10, 0, 0, tzinfo=timezone.utc)
    sim_now = (base + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    r = client.post(
        "/v1/admin/seed",
        headers={"X-Simulated-Now": base.strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
        json={
            "stores": [{"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1"]}],
            "loyalty_accounts": [{"account_id": acc_id}],
        },
    )
    assert r.status_code == 200, r.text

    # 10:00 +500, 10:02 -600 (deficit 100), 10:04 +300 (balance +200, ACTIVE)
    rb = client.post(
        "/v1/batches",
        headers={"X-Simulated-Now": sim_now},
        json={
            "store_id": s1,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_earn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": (base + timedelta(minutes=0)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "account_id": acc_id,
                    "points": 500,
                },
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_burn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": (base + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "epoch_gen": 1,
                    "terminal_epoch_seq": 1,
                    "account_id": acc_id,
                    "points": 600,
                },
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_earn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": (base + timedelta(minutes=4)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "account_id": acc_id,
                    "points": 300,
                },
            ],
        },
    )
    assert rb.status_code == 200 and rb.json()["accepted"] == 3

    # Backdated online burn of 150 at 10:01 -> canonical deficit at 10:02 becomes 250!
    ro = client.post(
        "/v1/loyalty/burn",
        headers={"X-Simulated-Now": sim_now},
        json={
            "event_id": str(uuid.uuid4()),
            "store_id": s1,
            "terminal_id": "T1",
            "occurred_at": (base + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "account_id": acc_id,
            "points": 150,
        },
    )
    assert ro.status_code == 200, ro.text
    assert ro.json()["balance"] == 50

    gl = client.get(f"/v1/loyalty/{acc_id}").json()
    assert gl["balance"] == 50
    assert gl["status"] == "ACTIVE"
    assert len(gl["negative_flags"]) == 1
    assert gl["negative_flags"][0]["deficit_points"] == 250

    c_live = client.get("/v1/ledger/checksum").json()
    rf = client.post("/v1/admin/rebuild?mode=full")
    assert rf.status_code == 200, rf.text
    c_rebuilt = client.get("/v1/ledger/checksum").json()
    assert c_rebuilt == c_live


def test_mixed_event_terminal_sync_and_fenced_reconcile(client: httpx.Client):
    s1 = unique_id("MXS")
    card_id = unique_id("GCMXS")
    acc_id = unique_id("ACCMXS")
    t0 = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
    t0_s = t0.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    r = client.post(
        "/v1/admin/seed",
        headers={"X-Simulated-Now": t0_s},
        json={
            "stores": [{"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1", "T2"]}],
            "gift_cards": [{"card_id": card_id, "initial_balance_minor": 4000, "eligible_stores": [s1]}],
            "loyalty_accounts": [{"account_id": acc_id}],
        },
    )
    assert r.status_code == 200, r.text

    # T1 syncs a mixed batch (sale + giftcard_redeem + loyalty_burn) without explicit expected_event_ids
    rs1 = client.post(
        f"/v1/stores/{s1}/terminals/T1/sync",
        headers={"X-Simulated-Now": (t0 + timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")},
        json={
            "epoch_gen": 1,
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "sale",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": (t0 + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "amount_minor": 1500,
                },
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "giftcard_redeem",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": (t0 + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "epoch_gen": 1,
                    "terminal_epoch_seq": 1,
                    "card_id": card_id,
                    "amount_minor": 500,
                },
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "loyalty_burn",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": (t0 + timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "epoch_gen": 1,
                    "terminal_epoch_seq": 2,
                    "account_id": acc_id,
                    "points": 100,
                },
            ],
        },
    )
    assert rs1.status_code == 200, rs1.text
    assert rs1.json()["renewal_status"] == "BLOCKED_PENDING_SIBLINGS"
    assert rs1.json()["batch_summary"]["accepted"] == 3

    # Advance simulated clock past 6h TTL + 600s fence margin and fence offline T2
    t_fence = (t0 + timedelta(hours=6, minutes=15)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    rf = client.post(
        f"/v1/stores/{s1}/terminals/T2/fence",
        headers={"X-Simulated-Now": t_fence},
        json={"epoch_gen": 1},
    )
    assert rf.status_code == 200, rf.text
    assert rf.json()["state"] == "FENCED"

    # Now T1 re-syncs epoch 1 -> epoch advances to 2, T1 receives GRANTED with reduced allowance (T2 liability reserved)
    rs1_retry = client.post(
        f"/v1/stores/{s1}/terminals/T1/sync",
        headers={"X-Simulated-Now": t_fence},
        json={"epoch_gen": 1, "events": []},
    )
    assert rs1_retry.status_code == 200, rs1_retry.text
    assert rs1_retry.json()["renewal_status"] == "GRANTED"
    assert rs1_retry.json()["active_epoch_gen"] == 2
    assert rs1_retry.json()["offline_allowance_minor"] == 625  # (2500 - 1250 fenced) / 2
    assert card_id in rs1_retry.json()["grant"]["cards"]

    # T2 later comes back and reconciles its FENCED epoch 1 slice with 0 events
    rs2_late = client.post(
        f"/v1/stores/{s1}/terminals/T2/sync",
        headers={"X-Simulated-Now": t_fence},
        json={"epoch_gen": 1, "events": []},
    )
    assert rs2_late.status_code == 200, rs2_late.text
    assert rs2_late.json()["renewal_status"] == "GRANTED"
    assert rs2_late.json()["active_epoch_gen"] == 2


def test_economic_checksum_excludes_zero_adjustment_only_days(client: httpx.Client):
    s1 = unique_id("ECZ")
    day1_sale_ts = "2026-09-01T10:00:00.000000Z"
    day1_close_ts = "2026-09-02T03:00:00.000000Z"
    day2_late_ret_ts = "2026-09-02T09:00:00.000000Z"

    r = client.post(
        "/v1/admin/seed",
        headers={"X-Simulated-Now": day1_sale_ts},
        json={"stores": [{"store_id": s1, "iana_timezone": "UTC", "terminal_ids": ["T1"]}]},
    )
    assert r.status_code == 200, r.text

    sale_id = str(uuid.uuid4())
    rb1 = client.post(
        "/v1/batches",
        headers={"X-Simulated-Now": day1_sale_ts},
        json={
            "store_id": s1,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": sale_id,
                    "event_type": "sale",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": day1_sale_ts,
                    "amount_minor": 2000,
                }
            ],
        },
    )
    assert rb1.status_code == 200 and rb1.json()["accepted"] == 1

    # Close Day 1
    rc = client.post(
        f"/v1/stores/{s1}/days/2026-09-01/close",
        headers={"X-Simulated-Now": day1_close_ts},
    )
    assert rc.status_code == 200, rc.text

    # Late return for Day 1 arrives on Day 2 (which has 0 direct events) -> creates Day 2 adjustment-only row
    rb2 = client.post(
        "/v1/batches",
        headers={"X-Simulated-Now": day2_late_ret_ts},
        json={
            "store_id": s1,
            "terminal_id": "T1",
            "events": [
                {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "return",
                    "store_id": s1,
                    "terminal_id": "T1",
                    "occurred_at": "2026-09-01T11:00:00.000000Z",
                    "parent_sale_id": sale_id,
                    "amount_minor": 500,
                }
            ],
        },
    )
    assert rb2.status_code == 200 and rb2.json()["accepted"] == 1

    c_live = client.get("/v1/ledger/checksum").json()
    snap = client.post("/v1/admin/snapshots")
    assert snap.status_code == 200, snap.text
    snap_id = snap.json()["snapshot_id"]

    assert client.post("/v1/admin/rebuild?mode=full").status_code == 200
    assert client.get("/v1/ledger/checksum").json() == c_live

    assert client.post(f"/v1/admin/rebuild?mode=snapshot&snapshot_id={snap_id}").status_code == 200
    assert client.get("/v1/ledger/checksum").json() == c_live
