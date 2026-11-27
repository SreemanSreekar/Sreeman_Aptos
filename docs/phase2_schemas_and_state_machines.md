# Phase 2: Data Schemas & State Machines (v10)

This document translates the approved **Phase 1 (v10)** architecture into complete, executable PostgreSQL 16 DDL, role/trigger isolation, formal state transition tables, the serialized epoch-advance & trusted local time/expiry mechanism, and the HTTP API contract.

---

## 1. PostgreSQL Roles, Config Table & Privilege Isolation

Two distinct database roles enforce append-only event log integrity, closed-day immutability, and admin-owned configuration without any app-settable bypass GUCs:

```sql
CREATE ROLE ledger_admin LOGIN PASSWORD 'ledger_admin_dev_pw';
CREATE ROLE ledger_app   LOGIN PASSWORD 'ledger_app_dev_pw';

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO ledger_app;
GRANT ALL   ON SCHEMA public TO ledger_admin;
```

- **`ledger_admin`** (schema owner; `ADMIN_DATABASE_URL`): owns all tables, sequences, triggers, `ledger_config`, `snapshot_*` tables, and `SECURITY DEFINER` admin functions.
- **`ledger_app`** (runtime API replicas `api-1`/`api-2`; `DATABASE_URL`):
  - `SELECT, INSERT` on `ledger_events` (**no `UPDATE`, `DELETE`, `TRUNCATE`**).
  - `USAGE, SELECT` on `ledger_applied_order_seq` and `ledger_events_seq_id_seq`.
  - `SELECT` only on `ledger_config` and `stores`.
  - `SELECT, INSERT, UPDATE, DELETE` on mutable read-model and operational tables, strictly constrained by immutability triggers.
  - **Zero `EXECUTE`** on `admin_*` functions; zero access to `ledger_snapshots` / `snapshot_*`.

---

## 2. Complete Executable PostgreSQL 16 Schema (DDL)

### 2.1 Admin-Owned Configuration & Reference Tables

```sql
-- Admin-owned single-row typed configuration (replaces any app-settable GUC)
CREATE TABLE ledger_config (
    id                               INT         PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    giftcard_offline_store_cap_minor BIGINT      NOT NULL DEFAULT 2500  CHECK (giftcard_offline_store_cap_minor >= 0),
    giftcard_max_offline_stores      INT         NOT NULL DEFAULT 5     CHECK (giftcard_max_offline_stores BETWEEN 1 AND 5),
    loyalty_offline_store_burn_cap   BIGINT      NOT NULL DEFAULT 5000  CHECK (loyalty_offline_store_burn_cap >= 0),
    offline_slice_ttl_seconds        BIGINT      NOT NULL DEFAULT 21600 CHECK (offline_slice_ttl_seconds > 0),
    hq_fence_margin_seconds          BIGINT      NOT NULL DEFAULT 600   CHECK (hq_fence_margin_seconds >= 0),
    allow_simulated_clock            BOOLEAN     NOT NULL DEFAULT false,
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO ledger_config (id) VALUES (1);

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Exact Phase 1 §3.3 deterministic UUIDv5(NAMESPACE_DAY_CLOSE, store_id || ':' || business_day)
-- NAMESPACE_DAY_CLOSE = 6ba7b811-9dad-11d1-80b4-00c04fd430c8 (RFC 4122 NameSpaceURL)
CREATE OR REPLACE FUNCTION day_close_event_id(p_store_id TEXT, p_business_day DATE)
RETURNS UUID LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::UUID, p_store_id || ':' || p_business_day::TEXT);
$$;

CREATE TABLE stores (
    store_id       TEXT        PRIMARY KEY,
    iana_timezone  TEXT        NOT NULL,
    terminal_count INT         NOT NULL CHECK (terminal_count > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Explicit terminal roster per store (replaces synthetic terminal ID generation)
CREATE TABLE store_terminals (
    store_id    TEXT    NOT NULL REFERENCES stores(store_id),
    terminal_id TEXT    NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT true,
    PRIMARY KEY (store_id, terminal_id)
);
```

### 2.2 Append-Only Source of Truth (`ledger_events`)

- `PRIMARY KEY (event_id)` aligns directly with Phase 1 (v10); `seq_id` and `applied_order` are `UNIQUE NOT NULL`.
- Every `CHECK` constraint includes explicit `IS NOT NULL` guards so PostgreSQL three-valued `NULL` logic can never bypass validation.
- `sale_id` is normalized for `sale`, `return`, and `void` (`return`/`void` enforce `sale_id = parent_sale_id`) with partial composite prefix indexes `(sale_id, applied_order)`, `(card_id, applied_order)`, and `(account_id, applied_order)`.

```sql
CREATE SEQUENCE ledger_applied_order_seq AS BIGINT START WITH 1 INCREMENT BY 1;

CREATE TABLE ledger_events (
    event_id            UUID        PRIMARY KEY,
    seq_id              BIGSERIAL   NOT NULL UNIQUE,
    applied_order       BIGINT      NOT NULL UNIQUE,
    commit_xid          xid8        NOT NULL DEFAULT pg_current_xact_id(),
    event_type          TEXT        NOT NULL CHECK (
        event_type IN (
            'sale', 'return', 'void',
            'giftcard_redeem', 'loyalty_earn', 'loyalty_burn',
            'day_close'
        )
    ),
    -- Strictly server-derived (never client-supplied):
    -- POST /v1/batches & sync buffers -> 'OFFLINE'; POST /v1/giftcards/redeem & /v1/loyalty/burn -> 'ONLINE'; day_close -> 'SYSTEM'
    channel             TEXT        NOT NULL CHECK (
        channel IN ('ONLINE', 'OFFLINE', 'SYSTEM')
    ),
    epoch_gen           BIGINT,
    terminal_epoch_seq  INT,
    store_id            TEXT        NOT NULL REFERENCES stores(store_id),
    terminal_id         TEXT        NOT NULL,
    occurred_at         TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL,

    -- Pinned business-day & timezone metadata at initial ingest (immune to future tzdata shifts)
    origin_business_day DATE        NOT NULL,
    origin_cutoff_utc   TIMESTAMPTZ NOT NULL,
    tz_version          TEXT        NOT NULL,

    -- Normalized entity keys for prefix-scoped canonical recomputation
    sale_id             UUID,
    parent_sale_id      UUID,
    card_id             TEXT,
    account_id          TEXT,

    -- Strictly integer minor units / points (zero floats)
    amount_minor        BIGINT,
    points              BIGINT,

    -- Canonical payload & collision-detection hash
    payload_json        JSONB       NOT NULL,
    payload_hash        BYTEA       NOT NULL CHECK (octet_length(payload_hash) = 32),

    -- NULL-safe structural invariants per event_type & channel
    CONSTRAINT chk_epoch_gen_provenance CHECK (
        (channel = 'OFFLINE' AND event_type IN ('giftcard_redeem', 'loyalty_burn')
         AND epoch_gen IS NOT NULL AND epoch_gen > 0
         AND terminal_epoch_seq IS NOT NULL AND terminal_epoch_seq > 0)
        OR
        (NOT (channel = 'OFFLINE' AND event_type IN ('giftcard_redeem', 'loyalty_burn'))
         AND (epoch_gen IS NULL OR epoch_gen > 0)
         AND (terminal_epoch_seq IS NULL OR terminal_epoch_seq > 0))
    ),
    CONSTRAINT chk_sale_fields CHECK (
        event_type <> 'sale' OR (
            sale_id IS NOT NULL
            AND parent_sale_id IS NULL
            AND card_id IS NULL
            AND account_id IS NULL
            AND amount_minor IS NOT NULL AND amount_minor > 0
            AND points IS NULL
        )
    ),
    CONSTRAINT chk_return_fields CHECK (
        event_type <> 'return' OR (
            parent_sale_id IS NOT NULL
            AND sale_id IS NOT NULL AND sale_id = parent_sale_id
            AND card_id IS NULL
            AND account_id IS NULL
            AND amount_minor IS NOT NULL AND amount_minor > 0
            AND points IS NULL
        )
    ),
    CONSTRAINT chk_void_fields CHECK (
        event_type <> 'void' OR (
            parent_sale_id IS NOT NULL
            AND sale_id IS NOT NULL AND sale_id = parent_sale_id
            AND card_id IS NULL
            AND account_id IS NULL
            AND amount_minor IS NOT NULL AND amount_minor > 0
            AND points IS NULL
        )
    ),
    CONSTRAINT chk_giftcard_fields CHECK (
        event_type <> 'giftcard_redeem' OR (
            card_id IS NOT NULL
            AND sale_id IS NULL AND parent_sale_id IS NULL AND account_id IS NULL
            AND amount_minor IS NOT NULL AND amount_minor > 0
            AND points IS NULL
        )
    ),
    CONSTRAINT chk_loyalty_fields CHECK (
        event_type NOT IN ('loyalty_earn', 'loyalty_burn') OR (
            account_id IS NOT NULL
            AND sale_id IS NULL AND parent_sale_id IS NULL AND card_id IS NULL
            AND points IS NOT NULL AND points > 0
            AND amount_minor IS NULL
        )
    ),
    CONSTRAINT chk_day_close_fields CHECK (
        event_type <> 'day_close' OR (
            channel = 'SYSTEM'
            AND occurred_at = origin_cutoff_utc
            AND event_id = day_close_event_id(store_id, origin_business_day)
            AND payload_json = jsonb_build_object('store_id', store_id, 'business_day', origin_business_day::TEXT)
            AND payload_hash = sha256(convert_to(
                event_type || '|' || store_id || '|' || terminal_id || '|' ||
                to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') || '|' ||
                '{"business_day":' || to_json(origin_business_day::TEXT)::TEXT ||
                ',"store_id":' || to_json(store_id)::TEXT || '}',
                'UTF8'
            ))
            AND sale_id IS NULL AND parent_sale_id IS NULL
            AND card_id IS NULL AND account_id IS NULL
            AND amount_minor IS NULL AND points IS NULL
        )
    )
);

CREATE UNIQUE INDEX uq_ledger_events_terminal_epoch_seq
    ON ledger_events (store_id, terminal_id, epoch_gen, terminal_epoch_seq)
    WHERE channel = 'OFFLINE' AND terminal_epoch_seq IS NOT NULL;

CREATE INDEX idx_ledger_events_sale_prefix
    ON ledger_events (sale_id, applied_order) WHERE sale_id IS NOT NULL;
CREATE INDEX idx_ledger_events_card_prefix
    ON ledger_events (card_id, applied_order) WHERE card_id IS NOT NULL;
CREATE INDEX idx_ledger_events_card_store_epoch
    ON ledger_events (card_id, store_id, epoch_gen, applied_order)
    WHERE event_type = 'giftcard_redeem' AND channel = 'OFFLINE';
CREATE INDEX idx_ledger_events_account_prefix
    ON ledger_events (account_id, applied_order) WHERE account_id IS NOT NULL;
CREATE INDEX idx_ledger_events_commit_xid
    ON ledger_events (commit_xid);

-- Hard append-only trigger on ledger_events
CREATE OR REPLACE FUNCTION trg_ledger_events_append_only() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'LEDGER_EVENTS_APPEND_ONLY: % is forbidden on ledger_events', TG_OP
        USING ERRCODE = '22023';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_events_no_update_delete
    BEFORE UPDATE OR DELETE ON ledger_events
    FOR EACH ROW EXECUTE FUNCTION trg_ledger_events_append_only();
```

---

### 2.3 Deterministic Read-Model / Projection Tables (Hashed in Phase 1 v10 `read_model_sha256`)

All 10 tables contain **zero wall-clock `NOW()` columns**; all timestamps are derived strictly from `ledger_events`.

```sql
-- 1. store_business_days
CREATE TABLE store_business_days (
    store_id        TEXT        NOT NULL REFERENCES stores(store_id),
    business_day    DATE        NOT NULL,
    cutoff_utc      TIMESTAMPTZ NOT NULL,
    tz_version      TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    closed_at_utc   TIMESTAMPTZ,
    close_event_id  UUID,
    PRIMARY KEY (store_id, business_day)
);

-- 2. store_day_totals
CREATE TABLE store_day_totals (
    store_id              TEXT   NOT NULL,
    business_day          DATE   NOT NULL,
    sales_minor           BIGINT NOT NULL DEFAULT 0,
    returns_minor         BIGINT NOT NULL DEFAULT 0,
    voids_minor           BIGINT NOT NULL DEFAULT 0,
    giftcard_redeem_minor BIGINT NOT NULL DEFAULT 0,
    adjustments_net_minor BIGINT NOT NULL DEFAULT 0,
    net_total_minor       BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (store_id, business_day),
    FOREIGN KEY (store_id, business_day) REFERENCES store_business_days(store_id, business_day)
);

-- 3. store_day_adjustments
CREATE TABLE store_day_adjustments (
    store_id             TEXT   NOT NULL,
    posting_business_day DATE   NOT NULL,
    origin_business_day  DATE   NOT NULL,
    event_id             UUID   NOT NULL,
    entry_kind           TEXT   NOT NULL CHECK (entry_kind IN ('LATE_POSTING', 'RECLASSIFICATION')),
    revision             INT    NOT NULL DEFAULT 0,
    caused_by_event_id   UUID   NOT NULL,
    event_type           TEXT   NOT NULL CHECK (event_type IN ('sale', 'return', 'void', 'giftcard_redeem')),
    amount_delta_minor   BIGINT NOT NULL,
    PRIMARY KEY (store_id, posting_business_day, origin_business_day, event_id, entry_kind, revision),
    FOREIGN KEY (store_id, posting_business_day) REFERENCES store_business_days(store_id, business_day)
);

-- 4. sales (unified parent-key lock stub + status-guarded per-sale state machine)
CREATE TABLE sales (
    sale_id               UUID   PRIMARY KEY,
    store_id              TEXT   REFERENCES stores(store_id),
    origin_business_day   DATE,
    status                TEXT   NOT NULL CHECK (
        status IN ('PLACEHOLDER', 'ACTIVE', 'PARTIALLY_RETURNED', 'FULLY_RETURNED', 'VOIDED')
    ),
    original_amount_minor BIGINT NOT NULL DEFAULT 0,
    returned_amount_minor BIGINT NOT NULL DEFAULT 0,
    revision              INT    NOT NULL DEFAULT 0,
    primary_event_id      UUID,

    CONSTRAINT chk_sales_per_status_invariants CHECK (
        (status = 'PLACEHOLDER'
         AND store_id IS NULL
         AND origin_business_day IS NULL
         AND original_amount_minor = 0
         AND returned_amount_minor = 0
         AND primary_event_id IS NULL)
        OR
        (status = 'ACTIVE'
         AND store_id IS NOT NULL
         AND origin_business_day IS NOT NULL
         AND original_amount_minor > 0
         AND returned_amount_minor = 0
         AND primary_event_id IS NOT NULL)
        OR
        (status = 'PARTIALLY_RETURNED'
         AND store_id IS NOT NULL
         AND origin_business_day IS NOT NULL
         AND original_amount_minor > 0
         AND returned_amount_minor > 0
         AND returned_amount_minor < original_amount_minor
         AND primary_event_id IS NOT NULL)
        OR
        (status = 'FULLY_RETURNED'
         AND store_id IS NOT NULL
         AND origin_business_day IS NOT NULL
         AND original_amount_minor > 0
         AND returned_amount_minor = original_amount_minor
         AND primary_event_id IS NOT NULL)
        OR
        (status = 'VOIDED'
         AND store_id IS NOT NULL
         AND origin_business_day IS NOT NULL
         AND original_amount_minor > 0
         AND returned_amount_minor = 0
         AND primary_event_id IS NOT NULL)
    )
);

-- 5. parked_events
CREATE TABLE parked_events (
    event_id                  UUID        PRIMARY KEY,
    parent_sale_id            UUID        NOT NULL REFERENCES sales(sale_id),
    status                    TEXT        NOT NULL CHECK (status IN ('PENDING_PARENT', 'RESOLVED')),
    resolved_at_utc           TIMESTAMPTZ,
    resolved_by_sale_event_id UUID
);
CREATE INDEX idx_parked_events_parent_pending
    ON parked_events (parent_sale_id) WHERE status = 'PENDING_PARENT';

CREATE VIEW v_stale_unresolved_orphans AS
SELECT pe.event_id, pe.parent_sale_id, le.store_id, le.occurred_at, le.ingested_at
FROM parked_events pe
JOIN ledger_events le ON le.event_id = pe.event_id
WHERE pe.status = 'PENDING_PARENT'
  AND le.ingested_at < now() - INTERVAL '72 hours';

-- 6. event_application_status
CREATE TABLE event_application_status (
    event_id              UUID   PRIMARY KEY,
    status                TEXT   NOT NULL CHECK (status IN ('APPLIED', 'PARKED', 'REJECTED_INVALID')),
    rejection_reason      TEXT,
    applied_amount_minor  BIGINT NOT NULL DEFAULT 0,
    applied_store_id      TEXT,
    applied_posting_day   DATE,
    applied_is_adjustment BOOLEAN NOT NULL DEFAULT FALSE
);

-- 7. gift_cards (initial_balance_minor & eligible_stores are immutable master attributes)
CREATE TABLE gift_cards (
    card_id               TEXT   PRIMARY KEY,
    initial_balance_minor BIGINT NOT NULL CHECK (initial_balance_minor >= 0),
    balance_minor         BIGINT NOT NULL,
    status                TEXT   NOT NULL CHECK (status IN ('ACTIVE', 'OVERSPENT_FROZEN', 'ADMIN_FROZEN')),
    eligible_stores       TEXT[] NOT NULL CHECK (cardinality(eligible_stores) BETWEEN 1 AND 5)
);

CREATE OR REPLACE FUNCTION trg_gift_cards_immutable_master() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'GIFT_CARD_DELETE_FORBIDDEN: cannot delete gift card %',
            OLD.card_id USING ERRCODE = '22023';
    END IF;
    IF NEW.card_id <> OLD.card_id
       OR NEW.initial_balance_minor <> OLD.initial_balance_minor
       OR NEW.eligible_stores <> OLD.eligible_stores THEN
        RAISE EXCEPTION 'GIFT_CARD_MASTER_IMMUTABLE: cannot alter card_id, initial_balance_minor, or eligible_stores for %',
            OLD.card_id USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER gift_cards_immutable_master_fields
    BEFORE UPDATE OR DELETE ON gift_cards
    FOR EACH ROW EXECUTE FUNCTION trg_gift_cards_immutable_master();

-- 8. giftcard_overspend_flags
CREATE TABLE giftcard_overspend_flags (
    card_id               TEXT   NOT NULL REFERENCES gift_cards(card_id),
    event_id              UUID   NOT NULL,
    store_id              TEXT   NOT NULL,
    balance_after_minor   BIGINT NOT NULL CHECK (balance_after_minor < 0),
    overspend_delta_minor BIGINT NOT NULL CHECK (overspend_delta_minor > 0),
    PRIMARY KEY (card_id, event_id)
);

-- 9. loyalty_accounts
CREATE TABLE loyalty_accounts (
    account_id   TEXT   PRIMARY KEY,
    earned_total BIGINT NOT NULL DEFAULT 0 CHECK (earned_total >= 0),
    burned_total BIGINT NOT NULL DEFAULT 0 CHECK (burned_total >= 0),
    balance      BIGINT NOT NULL DEFAULT 0,
    status       TEXT   NOT NULL CHECK (status IN ('ACTIVE', 'NEGATIVE_RESTRICTED'))
);

-- 10. loyalty_negative_flags
CREATE TABLE loyalty_negative_flags (
    account_id           TEXT   NOT NULL REFERENCES loyalty_accounts(account_id),
    trigger_event_id     UUID   NOT NULL,
    deficit_points       BIGINT NOT NULL CHECK (deficit_points > 0),
    resolved_by_event_id UUID,
    PRIMARY KEY (account_id, trigger_event_id)
);
```

### 2.4 Hardened Closed-Day Immutability Triggers (OLD + NEW Checks & `FOR SHARE` Close Serialization)

```sql
CREATE OR REPLACE FUNCTION trg_store_business_days_immutable() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'OPEN' OR NEW.closed_at_utc IS NOT NULL OR NEW.close_event_id IS NOT NULL THEN
            RAISE EXCEPTION 'BUSINESS_DAY_MUST_INSERT_OPEN: store_business_days (%, %) must be inserted OPEN without close metadata',
                NEW.store_id, NEW.business_day USING ERRCODE = '22023';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'BUSINESS_DAY_DELETE_FORBIDDEN: cannot delete store_business_days row (%, %)',
            OLD.store_id, OLD.business_day USING ERRCODE = '22023';
    END IF;
    IF NEW.store_id <> OLD.store_id
       OR NEW.business_day <> OLD.business_day
       OR NEW.cutoff_utc <> OLD.cutoff_utc
       OR NEW.tz_version <> OLD.tz_version THEN
        RAISE EXCEPTION 'BUSINESS_DAY_METADATA_IMMUTABLE: cannot alter key/cutoff/tz for (%, %)',
            OLD.store_id, OLD.business_day USING ERRCODE = '22023';
    END IF;
    IF OLD.status = 'OPEN' AND NEW.status = 'CLOSED' THEN
        IF NEW.closed_at_utc IS NULL OR NEW.closed_at_utc < OLD.cutoff_utc THEN
            RAISE EXCEPTION 'BUSINESS_DAY_CUTOFF_NOT_REACHED: cannot close (%, %) at % before cutoff %',
                OLD.store_id, OLD.business_day, NEW.closed_at_utc, OLD.cutoff_utc USING ERRCODE = '22023';
        END IF;
        -- Both explicit and lazy closes MUST append the deterministic UUIDv5 day_close event to ledger_events first
        IF NEW.close_event_id IS NULL
           OR NEW.close_event_id <> day_close_event_id(NEW.store_id, NEW.business_day)
           OR NOT EXISTS (
            SELECT 1 FROM ledger_events le
            WHERE le.event_id = NEW.close_event_id
              AND le.event_type = 'day_close'
              AND le.store_id = NEW.store_id
              AND le.origin_business_day = NEW.business_day
              AND le.ingested_at = NEW.closed_at_utc
        ) THEN
            RAISE EXCEPTION 'INVALID_DAY_CLOSE_EVENT: close_event_id % must equal deterministic UUIDv5 and match day_close event (with ingested_at = closed_at_utc) for (%, %)',
                NEW.close_event_id, NEW.store_id, NEW.business_day USING ERRCODE = '22023';
        END IF;
    END IF;
    IF OLD.status = 'CLOSED' AND (
        NEW.status <> 'CLOSED'
        OR NEW.closed_at_utc IS DISTINCT FROM OLD.closed_at_utc
        OR NEW.close_event_id IS DISTINCT FROM OLD.close_event_id
    ) THEN
        RAISE EXCEPTION 'BUSINESS_DAY_CLOSED_IMMUTABLE: cannot modify status, closed_at_utc, or close_event_id for closed day (%, %)',
            OLD.store_id, OLD.business_day USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER store_business_days_immutable_guard
    BEFORE INSERT OR UPDATE OR DELETE ON store_business_days
    FOR EACH ROW EXECUTE FUNCTION trg_store_business_days_immutable();

-- Checks BOTH OLD (on UPDATE/DELETE) and NEW (on INSERT/UPDATE) with FOR SHARE serialization against day_close FOR UPDATE
CREATE OR REPLACE FUNCTION trg_enforce_closed_day_immutable() RETURNS TRIGGER AS $$
DECLARE
    v_state TEXT;
    v_old_store TEXT;
    v_old_day   DATE;
    v_new_store TEXT;
    v_new_day   DATE;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        v_old_store := OLD.store_id;
        v_old_day   := CASE WHEN TG_TABLE_NAME = 'store_day_totals' THEN OLD.business_day ELSE OLD.posting_business_day END;

        SELECT status INTO v_state
        FROM store_business_days
        WHERE store_id = v_old_store AND business_day = v_old_day
        FOR SHARE;

        IF v_state = 'CLOSED' THEN
            RAISE EXCEPTION 'BUSINESS_DAY_CLOSED_IMMUTABLE: % forbidden on closed day (%, %) in %',
                TG_OP, v_old_store, v_old_day, TG_TABLE_NAME USING ERRCODE = '22023';
        END IF;
    END IF;

    IF TG_OP IN ('INSERT', 'UPDATE') THEN
        v_new_store := NEW.store_id;
        v_new_day   := CASE WHEN TG_TABLE_NAME = 'store_day_totals' THEN NEW.business_day ELSE NEW.posting_business_day END;

        SELECT status INTO v_state
        FROM store_business_days
        WHERE store_id = v_new_store AND business_day = v_new_day
        FOR SHARE;

        IF v_state = 'CLOSED' THEN
            RAISE EXCEPTION 'BUSINESS_DAY_CLOSED_IMMUTABLE: % forbidden on closed day (%, %) in %',
                TG_OP, v_new_store, v_new_day, TG_TABLE_NAME USING ERRCODE = '22023';
        END IF;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER store_day_totals_immutable_when_closed
    BEFORE INSERT OR UPDATE OR DELETE ON store_day_totals
    FOR EACH ROW EXECUTE FUNCTION trg_enforce_closed_day_immutable();

CREATE TRIGGER store_day_adjustments_immutable_when_closed
    BEFORE INSERT OR UPDATE OR DELETE ON store_day_adjustments
    FOR EACH ROW EXECUTE FUNCTION trg_enforce_closed_day_immutable();
```

---

### 2.5 Operational Tables (Excluded from `read_model_sha256` & `economic_sha256`)

```sql
CREATE TABLE online_action_receipts (
    event_id      UUID        PRIMARY KEY,
    action_type   TEXT        NOT NULL CHECK (action_type IN ('giftcard_redeem', 'loyalty_burn')),
    payload_hash  BYTEA       NOT NULL CHECK (octet_length(payload_hash) = 32),
    status        TEXT        NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'DECLINED')),
    http_status   INT,
    response_body JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ingest_batches (
    batch_id        UUID        PRIMARY KEY,
    store_id        TEXT        NOT NULL REFERENCES stores(store_id),
    terminal_id     TEXT        NOT NULL,
    event_count     INT         NOT NULL,
    accepted_count  INT         NOT NULL,
    duplicate_count INT         NOT NULL,
    parked_count    INT         NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE store_offline_epochs (
    store_id              TEXT        PRIMARY KEY REFERENCES stores(store_id),
    active_epoch_gen      BIGINT      NOT NULL DEFAULT 1 CHECK (active_epoch_gen > 0),
    roster_terminal_count INT         NOT NULL CHECK (roster_terminal_count > 0),
    state                 TEXT        NOT NULL DEFAULT 'OPEN' CHECK (state IN ('OPEN', 'RECONCILING')),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE store_epoch_history (
    store_id              TEXT        NOT NULL REFERENCES store_offline_epochs(store_id),
    epoch_gen             BIGINT      NOT NULL CHECK (epoch_gen > 0),
    roster_terminal_count INT         NOT NULL CHECK (roster_terminal_count > 0),
    pg_start_ts           TIMESTAMPTZ NOT NULL DEFAULT pg_postmaster_start_time(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (store_id, epoch_gen)
);

-- Immutable per-epoch terminal roster snapshot (enforces exact set-coverage barrier on epoch advance)
CREATE TABLE store_epoch_roster (
    store_id    TEXT   NOT NULL,
    epoch_gen   BIGINT NOT NULL,
    terminal_id TEXT   NOT NULL,
    PRIMARY KEY (store_id, epoch_gen, terminal_id),
    FOREIGN KEY (store_id, epoch_gen) REFERENCES store_epoch_history(store_id, epoch_gen)
);

CREATE OR REPLACE FUNCTION trg_store_epoch_roster_immutable() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'EPOCH_ROSTER_IMMUTABLE: % forbidden on store_epoch_roster', TG_OP USING ERRCODE = '22023';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER store_epoch_roster_no_mutate
    BEFORE UPDATE OR DELETE ON store_epoch_roster
    FOR EACH ROW EXECUTE FUNCTION trg_store_epoch_roster_immutable();

-- Deferred commit-time constraint trigger enforcing roster_terminal_count == COUNT(store_epoch_roster)
CREATE OR REPLACE FUNCTION trg_check_epoch_roster_count() RETURNS TRIGGER AS $$
DECLARE
    v_store    TEXT   := COALESCE(NEW.store_id, OLD.store_id);
    v_epoch    BIGINT := COALESCE(NEW.epoch_gen, OLD.epoch_gen);
    v_expected INT;
    v_actual   INT;
BEGIN
    SELECT roster_terminal_count INTO v_expected
    FROM store_epoch_history
    WHERE store_id = v_store AND epoch_gen = v_epoch;

    IF FOUND THEN
        SELECT COUNT(*) INTO v_actual
        FROM store_epoch_roster
        WHERE store_id = v_store AND epoch_gen = v_epoch;

        IF v_actual <> v_expected THEN
            RAISE EXCEPTION 'EPOCH_ROSTER_COUNT_MISMATCH: (%, epoch %) expected % roster rows, found %',
                v_store, v_epoch, v_expected, v_actual USING ERRCODE = '22023';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER store_epoch_history_roster_count_chk
    AFTER INSERT OR UPDATE ON store_epoch_history
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION trg_check_epoch_roster_count();

CREATE CONSTRAINT TRIGGER store_epoch_roster_count_chk
    AFTER INSERT OR UPDATE OR DELETE ON store_epoch_roster
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION trg_check_epoch_roster_count();

CREATE TABLE terminal_offline_slices (
    store_id             TEXT        NOT NULL REFERENCES store_offline_epochs(store_id),
    terminal_id          TEXT        NOT NULL,
    epoch_gen            BIGINT      NOT NULL,
    slice_minor          BIGINT      NOT NULL CHECK (slice_minor >= 0),
    -- NULL = unlimited (when loyalty_offline_store_burn_cap = 0); >= 0 = finite store/epoch cap share
    loyalty_slice_points BIGINT      CHECK (loyalty_slice_points IS NULL OR loyalty_slice_points >= 0),
    issued_at_utc        TIMESTAMPTZ NOT NULL,
    slice_expires_at_utc TIMESTAMPTZ NOT NULL,
    pg_start_ts          TIMESTAMPTZ NOT NULL DEFAULT pg_postmaster_start_time(),
    state                TEXT        NOT NULL CHECK (state IN ('ACTIVE', 'RECONCILED', 'FENCED')),
    reconciled_at_utc    TIMESTAMPTZ,
    fenced_at_utc        TIMESTAMPTZ,
    PRIMARY KEY (store_id, terminal_id, epoch_gen),
    FOREIGN KEY (store_id, epoch_gen, terminal_id) REFERENCES store_epoch_roster(store_id, epoch_gen, terminal_id)
);
CREATE INDEX idx_terminal_slices_store_epoch
    ON terminal_offline_slices (store_id, epoch_gen, state);

-- Composite FK binding offline giftcard_redeem / loyalty_burn events to real issued slices (MATCH SIMPLE: enforced whenever epoch_gen IS NOT NULL)
ALTER TABLE ledger_events
    ADD CONSTRAINT fk_ledger_events_offline_slice
    FOREIGN KEY (store_id, terminal_id, epoch_gen)
    REFERENCES terminal_offline_slices (store_id, terminal_id, epoch_gen)
    MATCH SIMPLE;

-- Protect slice identity, ACTIVE/FENCED unreconciled slices from deletion & enforce valid state transitions
CREATE OR REPLACE FUNCTION trg_terminal_slices_immutable_roster() RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.state IN ('ACTIVE', 'FENCED') THEN
            RAISE EXCEPTION 'EPOCH_ROSTER_IMMUTABLE: cannot delete % slice (%, %, epoch %)',
                OLD.state, OLD.store_id, OLD.terminal_id, OLD.epoch_gen USING ERRCODE = '22023';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.store_id <> OLD.store_id
       OR NEW.terminal_id <> OLD.terminal_id
       OR NEW.epoch_gen <> OLD.epoch_gen
       OR NEW.slice_minor <> OLD.slice_minor
       OR NEW.loyalty_slice_points IS DISTINCT FROM OLD.loyalty_slice_points
       OR NEW.issued_at_utc <> OLD.issued_at_utc
       OR NEW.slice_expires_at_utc <> OLD.slice_expires_at_utc
       OR NEW.pg_start_ts <> OLD.pg_start_ts THEN
        RAISE EXCEPTION 'EPOCH_ROSTER_IMMUTABLE: cannot mutate identity or reallocate slice for (%, %, epoch %)',
            OLD.store_id, OLD.terminal_id, OLD.epoch_gen USING ERRCODE = '22023';
    END IF;
    IF OLD.state = 'RECONCILED' AND NEW.state <> 'RECONCILED' THEN
        RAISE EXCEPTION 'EPOCH_SLICE_TERMINAL_STATE: cannot transition RECONCILED -> %',
            NEW.state USING ERRCODE = '22023';
    END IF;
    IF OLD.state = 'FENCED' AND NEW.state = 'ACTIVE' THEN
        RAISE EXCEPTION 'EPOCH_SLICE_INVALID_TRANSITION: cannot transition FENCED -> ACTIVE'
            USING ERRCODE = '22023';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER terminal_offline_slices_immutable_roster
    BEFORE UPDATE OR DELETE ON terminal_offline_slices
    FOR EACH ROW EXECUTE FUNCTION trg_terminal_slices_immutable_roster();

-- Serialized aggregate cap enforcement (giftcard cap + epoch-scoped loyalty cap) using admin-owned ledger_config
CREATE OR REPLACE FUNCTION trg_terminal_slices_aggregate_cap() RETURNS TRIGGER AS $$
DECLARE
    v_cfg           ledger_config%ROWTYPE;
    v_total_minor   BIGINT;
    v_total_loyalty BIGINT;
BEGIN
    PERFORM 1 FROM store_offline_epochs WHERE store_id = NEW.store_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'EPOCH_NOT_FOUND: store_offline_epochs row missing for store %',
            NEW.store_id USING ERRCODE = '22023';
    END IF;

    SELECT * INTO v_cfg FROM ledger_config WHERE id = 1;

    SELECT COALESCE(SUM(slice_minor), 0) INTO v_total_minor
    FROM terminal_offline_slices
    WHERE store_id = NEW.store_id
      AND state IN ('ACTIVE', 'FENCED');

    IF v_total_minor > v_cfg.giftcard_offline_store_cap_minor THEN
        RAISE EXCEPTION 'STORE_OFFLINE_CAP_EXCEEDED: store % active+fenced giftcard slices (%) exceed cap (%)',
            NEW.store_id, v_total_minor, v_cfg.giftcard_offline_store_cap_minor USING ERRCODE = '22023';
    END IF;

    -- Epoch-scoped loyalty cap check across (epoch_gen = NEW.epoch_gen AND ACTIVE) OR FENCED
    IF v_cfg.loyalty_offline_store_burn_cap > 0 AND NEW.loyalty_slice_points IS NOT NULL THEN
        SELECT COALESCE(SUM(loyalty_slice_points), 0) INTO v_total_loyalty
        FROM terminal_offline_slices
        WHERE store_id = NEW.store_id
          AND ((epoch_gen = NEW.epoch_gen AND state = 'ACTIVE') OR state = 'FENCED')
          AND loyalty_slice_points IS NOT NULL;

        IF v_total_loyalty > v_cfg.loyalty_offline_store_burn_cap THEN
            RAISE EXCEPTION 'STORE_LOYALTY_CAP_EXCEEDED: store % epoch % active+fenced loyalty slices (%) exceed cap (%)',
                NEW.store_id, NEW.epoch_gen, v_total_loyalty, v_cfg.loyalty_offline_store_burn_cap USING ERRCODE = '22023';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER terminal_offline_slices_check_cap
    AFTER INSERT OR UPDATE ON terminal_offline_slices
    FOR EACH ROW EXECUTE FUNCTION trg_terminal_slices_aggregate_cap();

-- Narrowly scoped atomic reconciliation routine with complete contiguous 1..N manifest proof
CREATE OR REPLACE FUNCTION reconcile_terminal_slice(
    p_store_id           TEXT,
    p_terminal_id        TEXT,
    p_epoch_gen          BIGINT,
    p_expected_event_ids UUID[],
    p_submitted_count    INT,
    p_simulated_now      TIMESTAMPTZ DEFAULT NULL
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cfg           ledger_config%ROWTYPE;
    v_now           TIMESTAMPTZ;
    v_active_epoch  BIGINT;
    v_slice_state   TEXT;
    v_missing_count INT;
    v_omitted_count INT;
    v_actual_count  INT;
    v_min_seq       INT;
    v_max_seq       INT;
    v_distinct_seq  INT;
BEGIN
    SELECT * INTO v_cfg FROM ledger_config WHERE id = 1;
    IF p_simulated_now IS NOT NULL AND NOT v_cfg.allow_simulated_clock THEN
        RAISE EXCEPTION 'SIMULATED_CLOCK_DISABLED' USING ERRCODE = '22023';
    END IF;
    v_now := COALESCE(p_simulated_now, now());

    IF p_expected_event_ids IS NULL OR p_submitted_count < 0 OR cardinality(p_expected_event_ids) <> p_submitted_count THEN
        RAISE EXCEPTION 'RECONCILE_INVALID_MANIFEST: expected_event_ids must be non-null with cardinality = submitted_count (%)',
            p_submitted_count USING ERRCODE = '22023';
    END IF;

    SELECT active_epoch_gen INTO v_active_epoch
    FROM store_offline_epochs WHERE store_id = p_store_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'EPOCH_NOT_FOUND: store %', p_store_id USING ERRCODE = '22023';
    END IF;

    SELECT state INTO v_slice_state
    FROM terminal_offline_slices
    WHERE store_id = p_store_id AND terminal_id = p_terminal_id AND epoch_gen = p_epoch_gen
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'SLICE_NOT_FOUND: (%, %, epoch %)', p_store_id, p_terminal_id, p_epoch_gen USING ERRCODE = '22023';
    END IF;
    IF NOT ((p_epoch_gen = v_active_epoch AND v_slice_state = 'ACTIVE') OR (p_epoch_gen <= v_active_epoch AND v_slice_state = 'FENCED')) THEN
        IF v_slice_state = 'RECONCILED' THEN
            RETURN;
        END IF;
        RAISE EXCEPTION 'INVALID_RECONCILE_EPOCH: epoch % state % not reconcilable against active epoch %',
            p_epoch_gen, v_slice_state, v_active_epoch USING ERRCODE = '22023';
    END IF;

    -- (a) Every ID in p_expected_event_ids exists in ledger_events for (store_id, terminal_id, epoch_gen, OFFLINE)
    IF p_submitted_count > 0 THEN
        SELECT COUNT(*) INTO v_missing_count
        FROM unnest(p_expected_event_ids) AS req(eid)
        WHERE NOT EXISTS (
            SELECT 1 FROM ledger_events le
            WHERE le.event_id = req.eid
              AND le.channel = 'OFFLINE'
              AND le.store_id = p_store_id
              AND le.terminal_id = p_terminal_id
              AND le.epoch_gen = p_epoch_gen
        );
        IF v_missing_count > 0 THEN
            RAISE EXCEPTION 'RECONCILE_EVENT_PROVENANCE_MISMATCH: % events missing/mismatched', v_missing_count USING ERRCODE = '22023';
        END IF;
    END IF;

    -- (b) No offline event in ledger_events for (store_id, terminal_id, epoch_gen) is omitted from p_expected_event_ids
    SELECT COUNT(*) INTO v_omitted_count
    FROM ledger_events le
    WHERE le.channel = 'OFFLINE'
      AND le.store_id = p_store_id
      AND le.terminal_id = p_terminal_id
      AND le.epoch_gen = p_epoch_gen
      AND NOT (le.event_id = ANY(p_expected_event_ids));
    IF v_omitted_count > 0 THEN
        RAISE EXCEPTION 'RECONCILE_OMITTED_EVENTS: % persisted offline events omitted from manifest', v_omitted_count USING ERRCODE = '22023';
    END IF;

    -- (c) Exact count = p_submitted_count with contiguous terminal_epoch_seq 1..p_submitted_count (0 if empty)
    SELECT COUNT(*), MIN(terminal_epoch_seq), MAX(terminal_epoch_seq), COUNT(DISTINCT terminal_epoch_seq)
      INTO v_actual_count, v_min_seq, v_max_seq, v_distinct_seq
    FROM ledger_events le
    WHERE le.channel = 'OFFLINE'
      AND le.store_id = p_store_id
      AND le.terminal_id = p_terminal_id
      AND le.epoch_gen = p_epoch_gen;

    IF v_actual_count <> p_submitted_count
       OR (p_submitted_count > 0 AND (v_min_seq <> 1 OR v_max_seq <> p_submitted_count OR v_distinct_seq <> p_submitted_count)) THEN
        RAISE EXCEPTION 'RECONCILE_SEQUENCE_GAP: expected contiguous 1..%, got count=% min=% max=% distinct=%',
            p_submitted_count, v_actual_count, v_min_seq, v_max_seq, v_distinct_seq USING ERRCODE = '22023';
    END IF;

    UPDATE terminal_offline_slices
    SET state = 'RECONCILED',
        reconciled_at_utc = COALESCE(reconciled_at_utc, v_now)
    WHERE store_id = p_store_id
      AND terminal_id = p_terminal_id
      AND epoch_gen = p_epoch_gen
      AND state IN ('ACTIVE', 'FENCED');
END;
$$;
ALTER FUNCTION reconcile_terminal_slice(TEXT, TEXT, BIGINT, UUID[], INT, TIMESTAMPTZ) OWNER TO ledger_admin;
REVOKE ALL ON FUNCTION reconcile_terminal_slice(TEXT, TEXT, BIGINT, UUID[], INT, TIMESTAMPTZ) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION reconcile_terminal_slice(TEXT, TEXT, BIGINT, UUID[], INT, TIMESTAMPTZ) TO ledger_app, ledger_admin;

-- DB-anchored atomic fencing routine (uses pg_postmaster_start_time() + clock_timestamp(); zero untrusted caller clock params)
CREATE OR REPLACE FUNCTION fence_terminal_slice(
    p_store_id      TEXT,
    p_terminal_id   TEXT,
    p_epoch_gen     BIGINT,
    p_simulated_now TIMESTAMPTZ DEFAULT NULL
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cfg          ledger_config%ROWTYPE;
    v_ingest_now   TIMESTAMPTZ;
    v_elapsed_sec  DOUBLE PRECISION;
    v_active_epoch BIGINT;
    v_slice        terminal_offline_slices%ROWTYPE;
BEGIN
    SELECT * INTO v_cfg FROM ledger_config WHERE id = 1;
    IF p_simulated_now IS NOT NULL AND NOT v_cfg.allow_simulated_clock THEN
        RAISE EXCEPTION 'SIMULATED_CLOCK_DISABLED' USING ERRCODE = '22023';
    END IF;

    SELECT active_epoch_gen INTO v_active_epoch
    FROM store_offline_epochs WHERE store_id = p_store_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'EPOCH_NOT_FOUND: store %', p_store_id USING ERRCODE = '22023';
    END IF;

    IF p_epoch_gen <> v_active_epoch THEN
        RAISE EXCEPTION 'FENCE_NON_ACTIVE_EPOCH: epoch % does not match active_epoch_gen %',
            p_epoch_gen, v_active_epoch USING ERRCODE = '22023';
    END IF;

    SELECT * INTO v_slice
    FROM terminal_offline_slices
    WHERE store_id = p_store_id AND terminal_id = p_terminal_id AND epoch_gen = p_epoch_gen
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'SLICE_NOT_FOUND: (%, %, epoch %)', p_store_id, p_terminal_id, p_epoch_gen USING ERRCODE = '22023';
    END IF;
    IF v_slice.state <> 'ACTIVE' THEN
        RAISE EXCEPTION 'FENCE_REQUIRES_ACTIVE_SLICE: slice state is %', v_slice.state USING ERRCODE = '22023';
    END IF;

    -- Fail closed if PostgreSQL/HQ restarted since slice issuance
    IF pg_postmaster_start_time() <> v_slice.pg_start_ts THEN
        RAISE EXCEPTION 'HQ_CLOCK_STEP_EXPLICIT_ACK_REQUIRED: DB/HQ restarted since slice issuance' USING ERRCODE = '22023';
    END IF;

    v_ingest_now  := COALESCE(p_simulated_now, now());
    v_elapsed_sec := CASE
        WHEN p_simulated_now IS NOT NULL THEN EXTRACT(EPOCH FROM (p_simulated_now - v_slice.issued_at_utc))
        ELSE EXTRACT(EPOCH FROM (clock_timestamp() - v_slice.issued_at_utc))
    END;

    IF v_elapsed_sec < (v_cfg.offline_slice_ttl_seconds + v_cfg.hq_fence_margin_seconds)
       OR v_ingest_now < (v_slice.slice_expires_at_utc + make_interval(secs => v_cfg.hq_fence_margin_seconds::INT)) THEN
        RAISE EXCEPTION 'SLICE_NOT_EXPIRED_YET: fence threshold not yet reached' USING ERRCODE = '22023';
    END IF;

    UPDATE terminal_offline_slices
    SET state = 'FENCED',
        fenced_at_utc = v_ingest_now
    WHERE store_id = p_store_id AND terminal_id = p_terminal_id AND epoch_gen = p_epoch_gen;
END;
$$;
ALTER FUNCTION fence_terminal_slice(TEXT, TEXT, BIGINT, TIMESTAMPTZ) OWNER TO ledger_admin;
REVOKE ALL ON FUNCTION fence_terminal_slice(TEXT, TEXT, BIGINT, TIMESTAMPTZ) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION fence_terminal_slice(TEXT, TEXT, BIGINT, TIMESTAMPTZ) TO ledger_app, ledger_admin;

-- Encapsulated atomic epoch advancement (exact set-coverage barrier over store_epoch_roster + DB-anchored clocks)
CREATE OR REPLACE FUNCTION advance_store_epoch(
    p_store_id      TEXT,
    p_simulated_now TIMESTAMPTZ DEFAULT NULL
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cfg               ledger_config%ROWTYPE;
    v_now               TIMESTAMPTZ;
    v_active_epoch      BIGINT;
    v_epoch_pg_start_ts TIMESTAMPTZ;
    v_new_term_count    INT;
    v_fenced_gc         BIGINT;
    v_reserved_loyalty  BIGINT;
    v_avail_gc          BIGINT;
    v_slice_gc          BIGINT;
    v_slice_loyalty     BIGINT;
    v_new_epoch         BIGINT;
BEGIN
    SELECT * INTO v_cfg FROM ledger_config WHERE id = 1;
    IF p_simulated_now IS NOT NULL AND NOT v_cfg.allow_simulated_clock THEN
        RAISE EXCEPTION 'SIMULATED_CLOCK_DISABLED' USING ERRCODE = '22023';
    END IF;
    v_now := COALESCE(p_simulated_now, now());

    SELECT active_epoch_gen INTO v_active_epoch
    FROM store_offline_epochs WHERE store_id = p_store_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'EPOCH_NOT_FOUND: store %', p_store_id USING ERRCODE = '22023';
    END IF;

    SELECT pg_start_ts INTO v_epoch_pg_start_ts
    FROM store_epoch_history
    WHERE store_id = p_store_id AND epoch_gen = v_active_epoch;

    -- If DB/HQ restarted since active_epoch_gen was issued, block advancement unless every rostered terminal
    -- is already RECONCILED or was FENCED strictly prior to the restart.
    IF pg_postmaster_start_time() <> v_epoch_pg_start_ts THEN
        IF EXISTS (
            SELECT 1
            FROM store_epoch_roster r
            LEFT JOIN terminal_offline_slices s
              ON s.store_id = r.store_id AND s.epoch_gen = r.epoch_gen AND s.terminal_id = r.terminal_id
            WHERE r.store_id = p_store_id
              AND r.epoch_gen = v_active_epoch
              AND NOT (
                  s.state = 'RECONCILED'
                  OR (s.state = 'FENCED' AND s.fenced_at_utc < pg_postmaster_start_time())
              )
        ) THEN
            RAISE EXCEPTION 'HQ_CLOCK_STEP_EXPLICIT_ACK_REQUIRED: DB/HQ restarted; active_epoch_gen % requires RECONCILED or pre-restart FENCED slices',
                v_active_epoch USING ERRCODE = '22023';
        END IF;
    END IF;

    -- Exact set-coverage barrier: NO rostered terminal in active_epoch_gen may lack a RECONCILED or FENCED slice
    IF EXISTS (
        SELECT 1
        FROM store_epoch_roster r
        WHERE r.store_id = p_store_id
          AND r.epoch_gen = v_active_epoch
          AND NOT EXISTS (
              SELECT 1 FROM terminal_offline_slices s
              WHERE s.store_id = r.store_id
                AND s.epoch_gen = r.epoch_gen
                AND s.terminal_id = r.terminal_id
                AND s.state IN ('RECONCILED', 'FENCED')
          )
    ) THEN
        RETURN 0;
    END IF;

    SELECT COUNT(*) INTO v_new_term_count
    FROM store_terminals
    WHERE store_id = p_store_id AND active = true;
    IF v_new_term_count <= 0 THEN
        RAISE EXCEPTION 'EMPTY_STORE_ROSTER: store % has no active terminals', p_store_id USING ERRCODE = '22023';
    END IF;

    SELECT COALESCE(SUM(slice_minor), 0) INTO v_fenced_gc
    FROM terminal_offline_slices
    WHERE store_id = p_store_id AND state = 'FENCED';

    SELECT COALESCE(SUM(loyalty_slice_points), 0) INTO v_reserved_loyalty
    FROM terminal_offline_slices
    WHERE store_id = p_store_id AND state = 'FENCED' AND loyalty_slice_points IS NOT NULL;

    v_avail_gc      := GREATEST(0, v_cfg.giftcard_offline_store_cap_minor - v_fenced_gc);
    v_slice_gc      := v_avail_gc / v_new_term_count;
    v_slice_loyalty := CASE
        WHEN v_cfg.loyalty_offline_store_burn_cap = 0 THEN NULL
        ELSE GREATEST(0, v_cfg.loyalty_offline_store_burn_cap - v_reserved_loyalty) / v_new_term_count
    END;
    v_new_epoch     := v_active_epoch + 1;

    UPDATE store_offline_epochs
    SET active_epoch_gen = v_new_epoch,
        roster_terminal_count = v_new_term_count,
        updated_at = v_now
    WHERE store_id = p_store_id;

    INSERT INTO store_epoch_history (store_id, epoch_gen, roster_terminal_count, pg_start_ts, created_at)
    VALUES (p_store_id, v_new_epoch, v_new_term_count, pg_postmaster_start_time(), v_now);

    -- Persist immutable roster snapshot for v_new_epoch
    INSERT INTO store_epoch_roster (store_id, epoch_gen, terminal_id)
    SELECT p_store_id, v_new_epoch, st.terminal_id
    FROM store_terminals st
    WHERE st.store_id = p_store_id AND st.active = true
    ORDER BY st.terminal_id ASC;

    INSERT INTO terminal_offline_slices (
        store_id, terminal_id, epoch_gen,
        slice_minor, loyalty_slice_points,
        issued_at_utc, slice_expires_at_utc,
        pg_start_ts, state
    )
    SELECT
        r.store_id, r.terminal_id, r.epoch_gen,
        v_slice_gc, v_slice_loyalty,
        v_now, v_now + make_interval(secs => v_cfg.offline_slice_ttl_seconds::INT),
        pg_postmaster_start_time(), 'ACTIVE'
    FROM store_epoch_roster r
    WHERE r.store_id = p_store_id AND r.epoch_gen = v_new_epoch
    ORDER BY r.terminal_id ASC;

    RETURN v_new_epoch;
END;
$$;
ALTER FUNCTION advance_store_epoch(TEXT, TIMESTAMPTZ) OWNER TO ledger_admin;
REVOKE ALL ON FUNCTION advance_store_epoch(TEXT, TIMESTAMPTZ) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION advance_store_epoch(TEXT, TIMESTAMPTZ) TO ledger_app, ledger_admin;
```

---

### 2.6 Full Executable Snapshot Mirror Tables & `SECURITY DEFINER` `ledger_admin` Procedures

```sql
CREATE TABLE ledger_snapshots (
    snapshot_id       BIGSERIAL   PRIMARY KEY,
    pg_snapshot_text  TEXT        NOT NULL,
    xmin              xid8        NOT NULL,
    xmax              xid8        NOT NULL,
    read_model_sha256 TEXT        NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE snapshot_store_business_days (
    snapshot_id    BIGINT      NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    store_id       TEXT        NOT NULL,
    business_day   DATE        NOT NULL,
    cutoff_utc     TIMESTAMPTZ NOT NULL,
    tz_version     TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    closed_at_utc  TIMESTAMPTZ,
    close_event_id UUID,
    PRIMARY KEY (snapshot_id, store_id, business_day)
);

CREATE TABLE snapshot_store_day_totals (
    snapshot_id           BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    store_id              TEXT   NOT NULL,
    business_day          DATE   NOT NULL,
    sales_minor           BIGINT NOT NULL,
    returns_minor         BIGINT NOT NULL,
    voids_minor           BIGINT NOT NULL,
    giftcard_redeem_minor BIGINT NOT NULL,
    adjustments_net_minor BIGINT NOT NULL,
    net_total_minor       BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, store_id, business_day)
);

CREATE TABLE snapshot_store_day_adjustments (
    snapshot_id          BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    store_id             TEXT   NOT NULL,
    posting_business_day DATE   NOT NULL,
    origin_business_day  DATE   NOT NULL,
    event_id             UUID   NOT NULL,
    entry_kind           TEXT   NOT NULL,
    revision             INT    NOT NULL,
    caused_by_event_id   UUID   NOT NULL,
    event_type           TEXT   NOT NULL,
    amount_delta_minor   BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, store_id, posting_business_day, origin_business_day, event_id, entry_kind, revision)
);

CREATE TABLE snapshot_sales (
    snapshot_id           BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    sale_id               UUID   NOT NULL,
    store_id              TEXT,
    origin_business_day   DATE,
    status                TEXT   NOT NULL,
    original_amount_minor BIGINT NOT NULL,
    returned_amount_minor BIGINT NOT NULL,
    revision              INT    NOT NULL,
    primary_event_id      UUID,
    PRIMARY KEY (snapshot_id, sale_id)
);

CREATE TABLE snapshot_parked_events (
    snapshot_id               BIGINT      NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    event_id                  UUID        NOT NULL,
    parent_sale_id            UUID        NOT NULL,
    status                    TEXT        NOT NULL,
    resolved_at_utc           TIMESTAMPTZ,
    resolved_by_sale_event_id UUID,
    PRIMARY KEY (snapshot_id, event_id)
);

CREATE TABLE snapshot_event_application_status (
    snapshot_id           BIGINT  NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    event_id              UUID    NOT NULL,
    status                TEXT    NOT NULL,
    rejection_reason      TEXT,
    applied_amount_minor  BIGINT  NOT NULL,
    applied_store_id      TEXT,
    applied_posting_day   DATE,
    applied_is_adjustment BOOLEAN NOT NULL,
    PRIMARY KEY (snapshot_id, event_id)
);

CREATE TABLE snapshot_gift_cards (
    snapshot_id           BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    card_id               TEXT   NOT NULL,
    initial_balance_minor BIGINT NOT NULL,
    balance_minor         BIGINT NOT NULL,
    status                TEXT   NOT NULL,
    eligible_stores       TEXT[] NOT NULL,
    PRIMARY KEY (snapshot_id, card_id)
);

CREATE TABLE snapshot_giftcard_overspend_flags (
    snapshot_id           BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    card_id               TEXT   NOT NULL,
    event_id              UUID   NOT NULL,
    store_id              TEXT   NOT NULL,
    balance_after_minor   BIGINT NOT NULL,
    overspend_delta_minor BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id, card_id, event_id)
);

CREATE TABLE snapshot_loyalty_accounts (
    snapshot_id  BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    account_id   TEXT   NOT NULL,
    earned_total BIGINT NOT NULL,
    burned_total BIGINT NOT NULL,
    balance      BIGINT NOT NULL,
    status       TEXT   NOT NULL,
    PRIMARY KEY (snapshot_id, account_id)
);

CREATE TABLE snapshot_loyalty_negative_flags (
    snapshot_id          BIGINT NOT NULL REFERENCES ledger_snapshots(snapshot_id) ON DELETE CASCADE,
    account_id           TEXT   NOT NULL,
    trigger_event_id     UUID   NOT NULL,
    deficit_points       BIGINT NOT NULL,
    resolved_by_event_id UUID,
    PRIMARY KEY (snapshot_id, account_id, trigger_event_id)
);
```

#### Executable `SECURITY DEFINER` Admin Procedures & Role Grants

```sql
CREATE OR REPLACE FUNCTION admin_seed_store(
    p_store_id      TEXT,
    p_iana_tz       TEXT,
    p_terminal_ids  TEXT[],
    p_simulated_now TIMESTAMPTZ DEFAULT NULL
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_cfg           ledger_config%ROWTYPE;
    v_existing_tz   TEXT;
    v_now           TIMESTAMPTZ;
    v_term_count    INT;
    v_slice_gc      BIGINT;
    v_slice_loyalty BIGINT;
BEGIN
    IF p_terminal_ids IS NULL OR cardinality(p_terminal_ids) = 0 THEN
        RAISE EXCEPTION 'EMPTY_STORE_ROSTER: p_terminal_ids must be non-empty for store %', p_store_id USING ERRCODE = '22023';
    END IF;

    SELECT iana_timezone INTO v_existing_tz FROM stores WHERE store_id = p_store_id;
    IF FOUND AND v_existing_tz <> p_iana_tz THEN
        RAISE EXCEPTION 'STORE_TIMEZONE_CONFLICT: store % already exists with timezone %, cannot re-seed with %',
            p_store_id, v_existing_tz, p_iana_tz USING ERRCODE = '22023';
    END IF;

    SELECT * INTO v_cfg FROM ledger_config WHERE id = 1;
    IF p_simulated_now IS NOT NULL AND NOT v_cfg.allow_simulated_clock THEN
        RAISE EXCEPTION 'SIMULATED_CLOCK_DISABLED' USING ERRCODE = '22023';
    END IF;
    v_now        := COALESCE(p_simulated_now, now());
    v_term_count := cardinality(p_terminal_ids);
    v_slice_gc   := v_cfg.giftcard_offline_store_cap_minor / v_term_count;
    v_slice_loyalty := CASE
        WHEN v_cfg.loyalty_offline_store_burn_cap = 0 THEN NULL
        ELSE v_cfg.loyalty_offline_store_burn_cap / v_term_count
    END;

    INSERT INTO stores (store_id, iana_timezone, terminal_count, created_at)
    VALUES (p_store_id, p_iana_tz, v_term_count, v_now)
    ON CONFLICT (store_id) DO NOTHING;

    INSERT INTO store_terminals (store_id, terminal_id, active)
    SELECT p_store_id, tid, true
    FROM unnest(p_terminal_ids) AS t(tid)
    ON CONFLICT (store_id, terminal_id) DO NOTHING;

    INSERT INTO store_offline_epochs (store_id, active_epoch_gen, roster_terminal_count, state, updated_at)
    VALUES (p_store_id, 1, v_term_count, 'OPEN', v_now)
    ON CONFLICT (store_id) DO NOTHING;

    INSERT INTO store_epoch_history (store_id, epoch_gen, roster_terminal_count, pg_start_ts, created_at)
    VALUES (p_store_id, 1, v_term_count, pg_postmaster_start_time(), v_now)
    ON CONFLICT (store_id, epoch_gen) DO NOTHING;

    INSERT INTO store_epoch_roster (store_id, epoch_gen, terminal_id)
    SELECT p_store_id, 1, st.terminal_id
    FROM store_terminals st
    WHERE st.store_id = p_store_id AND st.active = true
    ORDER BY st.terminal_id ASC
    ON CONFLICT (store_id, epoch_gen, terminal_id) DO NOTHING;

    INSERT INTO terminal_offline_slices (
        store_id, terminal_id, epoch_gen,
        slice_minor, loyalty_slice_points,
        issued_at_utc, slice_expires_at_utc,
        pg_start_ts, state
    )
    SELECT
        r.store_id, r.terminal_id, 1,
        v_slice_gc, v_slice_loyalty,
        v_now, v_now + make_interval(secs => v_cfg.offline_slice_ttl_seconds::INT),
        pg_postmaster_start_time(), 'ACTIVE'
    FROM store_epoch_roster r
    WHERE r.store_id = p_store_id AND r.epoch_gen = 1
    ORDER BY r.terminal_id ASC
    ON CONFLICT (store_id, terminal_id, epoch_gen) DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION admin_seed_gift_card(
    p_card_id               TEXT,
    p_initial_balance_minor BIGINT,
    p_eligible_stores       TEXT[]
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_existing gift_cards%ROWTYPE;
BEGIN
    SELECT * INTO v_existing FROM gift_cards WHERE card_id = p_card_id;
    IF FOUND THEN
        IF v_existing.initial_balance_minor <> p_initial_balance_minor
           OR v_existing.eligible_stores <> p_eligible_stores THEN
            RAISE EXCEPTION 'GIFT_CARD_SEED_CONFLICT: card % already exists with different initial_balance_minor or eligible_stores',
                p_card_id USING ERRCODE = '22023';
        END IF;
        RETURN;
    END IF;

    INSERT INTO gift_cards (card_id, initial_balance_minor, balance_minor, status, eligible_stores)
    VALUES (p_card_id, p_initial_balance_minor, p_initial_balance_minor, 'ACTIVE', p_eligible_stores);
END;
$$;

CREATE OR REPLACE FUNCTION admin_seed_loyalty_account(
    p_account_id TEXT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO loyalty_accounts (account_id, earned_total, burned_total, balance, status)
    VALUES (p_account_id, 0, 0, 0, 'ACTIVE')
    ON CONFLICT (account_id) DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION admin_reset_read_models() RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    TRUNCATE TABLE
        store_day_adjustments,
        store_day_totals,
        store_business_days,
        parked_events,
        sales,
        event_application_status,
        giftcard_overspend_flags,
        loyalty_negative_flags,
        loyalty_accounts
    CASCADE;

    UPDATE gift_cards
    SET balance_minor = initial_balance_minor,
        status = CASE WHEN status = 'ADMIN_FROZEN' THEN 'ADMIN_FROZEN' ELSE 'ACTIVE' END;
END;
$$;

-- Executed inside Writer Tx after SET TRANSACTION SNAPSHOT '<export_id>' (derives watermark directly from active snapshot)
CREATE OR REPLACE FUNCTION admin_capture_snapshot(
    p_read_model_sha256 TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_snap    pg_snapshot := pg_current_snapshot();
    v_snap_id BIGINT;
BEGIN
    INSERT INTO ledger_snapshots (pg_snapshot_text, xmin, xmax, read_model_sha256)
    VALUES (v_snap::TEXT, pg_snapshot_xmin(v_snap), pg_snapshot_xmax(v_snap), p_read_model_sha256)
    RETURNING snapshot_id INTO v_snap_id;

    INSERT INTO snapshot_store_business_days SELECT v_snap_id, store_id, business_day, cutoff_utc, tz_version, status, closed_at_utc, close_event_id FROM store_business_days;
    INSERT INTO snapshot_store_day_totals SELECT v_snap_id, store_id, business_day, sales_minor, returns_minor, voids_minor, giftcard_redeem_minor, adjustments_net_minor, net_total_minor FROM store_day_totals;
    INSERT INTO snapshot_store_day_adjustments SELECT v_snap_id, store_id, posting_business_day, origin_business_day, event_id, entry_kind, revision, caused_by_event_id, event_type, amount_delta_minor FROM store_day_adjustments;
    INSERT INTO snapshot_sales SELECT v_snap_id, sale_id, store_id, origin_business_day, status, original_amount_minor, returned_amount_minor, revision, primary_event_id FROM sales;
    INSERT INTO snapshot_parked_events SELECT v_snap_id, event_id, parent_sale_id, status, resolved_at_utc, resolved_by_sale_event_id FROM parked_events;
    INSERT INTO snapshot_event_application_status SELECT v_snap_id, event_id, status, rejection_reason, applied_amount_minor, applied_store_id, applied_posting_day, applied_is_adjustment FROM event_application_status;
    INSERT INTO snapshot_gift_cards SELECT v_snap_id, card_id, initial_balance_minor, balance_minor, status, eligible_stores FROM gift_cards;
    INSERT INTO snapshot_giftcard_overspend_flags SELECT v_snap_id, card_id, event_id, store_id, balance_after_minor, overspend_delta_minor FROM giftcard_overspend_flags;
    INSERT INTO snapshot_loyalty_accounts SELECT v_snap_id, account_id, earned_total, burned_total, balance, status FROM loyalty_accounts;
    INSERT INTO snapshot_loyalty_negative_flags SELECT v_snap_id, account_id, trigger_event_id, deficit_points, resolved_by_event_id FROM loyalty_negative_flags;

    RETURN v_snap_id;
END;
$$;

CREATE OR REPLACE FUNCTION admin_restore_snapshot(p_snapshot_id BIGINT) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ledger_snapshots WHERE snapshot_id = p_snapshot_id) THEN
        RAISE EXCEPTION 'SNAPSHOT_NOT_FOUND: snapshot_id % does not exist', p_snapshot_id
            USING ERRCODE = '22023';
    END IF;

    PERFORM admin_reset_read_models();

    -- Insert store_business_days as OPEN (NULL close metadata) first so totals/adjustments inserts satisfy triggers cleanly
    INSERT INTO store_business_days (store_id, business_day, cutoff_utc, tz_version, status, closed_at_utc, close_event_id)
    SELECT store_id, business_day, cutoff_utc, tz_version, 'OPEN', NULL, NULL
    FROM snapshot_store_business_days WHERE snapshot_id = p_snapshot_id;

    INSERT INTO store_day_totals
    SELECT store_id, business_day, sales_minor, returns_minor, voids_minor, giftcard_redeem_minor, adjustments_net_minor, net_total_minor
    FROM snapshot_store_day_totals WHERE snapshot_id = p_snapshot_id;

    INSERT INTO store_day_adjustments
    SELECT store_id, posting_business_day, origin_business_day, event_id, entry_kind, revision, caused_by_event_id, event_type, amount_delta_minor
    FROM snapshot_store_day_adjustments WHERE snapshot_id = p_snapshot_id;

    -- Now transition originally CLOSED days to CLOSED with their exact persisted close metadata
    UPDATE store_business_days sbd
    SET status = 'CLOSED',
        closed_at_utc = ssbd.closed_at_utc,
        close_event_id = ssbd.close_event_id
    FROM snapshot_store_business_days ssbd
    WHERE ssbd.snapshot_id = p_snapshot_id
      AND ssbd.status = 'CLOSED'
      AND sbd.store_id = ssbd.store_id
      AND sbd.business_day = ssbd.business_day;

    INSERT INTO sales
    SELECT sale_id, store_id, origin_business_day, status, original_amount_minor, returned_amount_minor, revision, primary_event_id
    FROM snapshot_sales WHERE snapshot_id = p_snapshot_id;

    INSERT INTO parked_events
    SELECT event_id, parent_sale_id, status, resolved_at_utc, resolved_by_sale_event_id
    FROM snapshot_parked_events WHERE snapshot_id = p_snapshot_id;

    INSERT INTO event_application_status
    SELECT event_id, status, rejection_reason, applied_amount_minor, applied_store_id, applied_posting_day, applied_is_adjustment
    FROM snapshot_event_application_status WHERE snapshot_id = p_snapshot_id;

    UPDATE gift_cards gc
    SET balance_minor = sgc.balance_minor,
        status = sgc.status
    FROM snapshot_gift_cards sgc
    WHERE sgc.snapshot_id = p_snapshot_id AND gc.card_id = sgc.card_id;

    INSERT INTO giftcard_overspend_flags
    SELECT card_id, event_id, store_id, balance_after_minor, overspend_delta_minor
    FROM snapshot_giftcard_overspend_flags WHERE snapshot_id = p_snapshot_id;

    INSERT INTO loyalty_accounts
    SELECT account_id, earned_total, burned_total, balance, status
    FROM snapshot_loyalty_accounts WHERE snapshot_id = p_snapshot_id;

    INSERT INTO loyalty_negative_flags
    SELECT account_id, trigger_event_id, deficit_points, resolved_by_event_id
    FROM snapshot_loyalty_negative_flags WHERE snapshot_id = p_snapshot_id;
END;
$$;

-- Strict ownership and privilege isolation
ALTER FUNCTION admin_seed_store(TEXT, TEXT, TEXT[], TIMESTAMPTZ) OWNER TO ledger_admin;
ALTER FUNCTION admin_seed_gift_card(TEXT, BIGINT, TEXT[]) OWNER TO ledger_admin;
ALTER FUNCTION admin_seed_loyalty_account(TEXT) OWNER TO ledger_admin;
ALTER FUNCTION admin_reset_read_models() OWNER TO ledger_admin;
ALTER FUNCTION admin_capture_snapshot(TEXT) OWNER TO ledger_admin;
ALTER FUNCTION admin_restore_snapshot(BIGINT) OWNER TO ledger_admin;

REVOKE ALL ON FUNCTION admin_seed_store(TEXT, TEXT, TEXT[], TIMESTAMPTZ) FROM PUBLIC, ledger_app;
REVOKE ALL ON FUNCTION admin_seed_gift_card(TEXT, BIGINT, TEXT[]) FROM PUBLIC, ledger_app;
REVOKE ALL ON FUNCTION admin_seed_loyalty_account(TEXT) FROM PUBLIC, ledger_app;
REVOKE ALL ON FUNCTION admin_reset_read_models() FROM PUBLIC, ledger_app;
REVOKE ALL ON FUNCTION admin_capture_snapshot(TEXT) FROM PUBLIC, ledger_app;
REVOKE ALL ON FUNCTION admin_restore_snapshot(BIGINT) FROM PUBLIC, ledger_app;

GRANT EXECUTE ON FUNCTION admin_seed_store(TEXT, TEXT, TEXT[], TIMESTAMPTZ) TO ledger_admin;
GRANT EXECUTE ON FUNCTION admin_seed_gift_card(TEXT, BIGINT, TEXT[]) TO ledger_admin;
GRANT EXECUTE ON FUNCTION admin_seed_loyalty_account(TEXT) TO ledger_admin;
GRANT EXECUTE ON FUNCTION admin_reset_read_models() TO ledger_admin;
GRANT EXECUTE ON FUNCTION admin_capture_snapshot(TEXT) TO ledger_admin;
GRANT EXECUTE ON FUNCTION admin_restore_snapshot(BIGINT) TO ledger_admin;

-- Runtime ledger_app grants (NO DELETE on gift_cards; SELECT-only on store_terminals, store_offline_epochs, store_epoch_history, store_epoch_roster, terminal_offline_slices)
GRANT SELECT ON
    ledger_config, stores, store_terminals, v_stale_unresolved_orphans,
    store_offline_epochs, store_epoch_history, store_epoch_roster, terminal_offline_slices
TO ledger_app;
GRANT SELECT, INSERT ON ledger_events TO ledger_app;
GRANT USAGE, SELECT ON SEQUENCE ledger_applied_order_seq, ledger_events_seq_id_seq TO ledger_app;
GRANT SELECT, INSERT, UPDATE ON gift_cards TO ledger_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    store_business_days, store_day_totals, store_day_adjustments,
    sales, parked_events, event_application_status,
    giftcard_overspend_flags,
    loyalty_accounts, loyalty_negative_flags,
    online_action_receipts, ingest_batches
TO ledger_app;
```

---

## 3. Formal State Machines, Central Validation & Online/Batch Race SQL

### 3.1 Per-Sale State Machine & Prefix-Scoped Delta/Reclassification
For any `sale`, `return`, or `void` event $e$ at `applied_order = A`, `projectEvent(tx, e)` queries:
```sql
SELECT * FROM ledger_events
WHERE sale_id = $e.sale_id AND applied_order <= $A
ORDER BY occurred_at ASC, event_id ASC;
```
| Current State | Incoming Event (canonical order) | Guard Condition | Next State | Event Status / Reason |
| :--- | :--- | :--- | :--- | :--- |
| *(none)* | `return` / `void` | No `sale` in prefix | `PLACEHOLDER` | `PARKED` (row in `parked_events`) |
| *(none)* / `PLACEHOLDER` | `sale` | First `sale` in prefix | `ACTIVE` | `APPLIED` (`original = amt`, drains `parked_events` with `resolved_at_utc = sale.ingested_at`) |
| `ACTIVE` / `PARTIALLY_RETURNED` / `FULLY_RETURNED` / `VOIDED` | `sale` (distinct `event_id`) | Second `sale` for `sale_id` | *(unchanged)* | `REJECTED_INVALID` (`DUPLICATE_SALE_ID`) |
| `ACTIVE` | `return(amt)` | `0 < amt < original` | `PARTIALLY_RETURNED` | `APPLIED` (`returned += amt`) |
| `ACTIVE` | `return(amt)` | `amt == original` | `FULLY_RETURNED` | `APPLIED` (`returned = original`) |
| `ACTIVE` | `return(amt)` | `amt > original` | `ACTIVE` | `REJECTED_INVALID` (`RETURN_EXCEEDS_REMAINING`) |
| `ACTIVE` | `void(amt)` | `void.store_id == sale.store_id` AND `amt == original` | `VOIDED` | `APPLIED` |
| `ACTIVE` | `void(amt)` | `void.store_id <> sale.store_id` | `ACTIVE` | `REJECTED_INVALID` (`VOID_STORE_MISMATCH`) |
| `PARTIALLY_RETURNED` | `return(amt)` | `returned + amt < original` | `PARTIALLY_RETURNED` | `APPLIED` (`returned += amt`) |
| `PARTIALLY_RETURNED` | `return(amt)` | `returned + amt == original` | `FULLY_RETURNED` | `APPLIED` (`returned = original`) |
| `PARTIALLY_RETURNED` | `return(amt)` | `returned + amt > original` | `PARTIALLY_RETURNED` | `REJECTED_INVALID` (`RETURN_EXCEEDS_REMAINING`) |
| `PARTIALLY_RETURNED` / `FULLY_RETURNED` | `void` | Always invalid | *(unchanged)* | `REJECTED_INVALID` (`CANNOT_VOID_RETURNED_SALE`) |
| `FULLY_RETURNED` | `return` | Always invalid | `FULLY_RETURNED` | `REJECTED_INVALID` (`ALREADY_FULLY_RETURNED`) |
| `VOIDED` | `return` | Always invalid | `VOIDED` | `REJECTED_INVALID` (`CANNOT_RETURN_VOIDED_SALE`) |
| `VOIDED` | `void` | Always invalid | `VOIDED` | `REJECTED_INVALID` (`ALREADY_VOIDED`) |

When a late earlier event flips any prefix event $x$ (`APPLIED <-> REJECTED_INVALID`), `sales.revision` increments once and a signed `RECLASSIFICATION` adjustment (`delta = new_effect - old_effect`, `caused_by_event_id = e.event_id`, `revision = sales.revision`) posts to the current open `posting_business_day` (evaluated at `e.ingested_at`) linked to `x.origin_business_day`.

### 3.2 Net Totals Sign Convention (`store_day_totals`)
- `sales_minor`, `returns_minor`, `voids_minor`, `giftcard_redeem_minor`: positive magnitudes for direct-posted events on `business_day`.
- `adjustments_net_minor`: signed sum of `amount_delta_minor` across `store_day_adjustments` on `business_day` for `sale`/`return`/`void` (`giftcard_redeem` adjustments are recorded in `store_day_adjustments` for `economic_store_day_totals` rollup without altering net sales).
- `net_total_minor = sales_minor - returns_minor - voids_minor + adjustments_net_minor`.

### 3.3 Central Gift-Card & Loyalty Offline Validation + State Machines
- **Live-Ingestion-Only Slice Spendability Gate (`POST /v1/batches`)**:
  - Checking `terminal_offline_slices.state IN ('ACTIVE', 'FENCED')` for offline `giftcard_redeem` and `loyalty_burn` events is enforced **strictly during live ingestion (`POST /v1/batches`) before inserting into `ledger_events`** (rejecting new offline events targeting an already-`RECONCILED` slice with `409 SLICE_ALREADY_RECONCILED`).
  - It is **never** evaluated inside `projectEvent()` during `full` or `snapshot` rebuilds—where `ledger_events` already carries FK-validated `(store_id, terminal_id, epoch_gen)` provenance and deterministic cap/eligibility rules—ensuring `live == full rebuild == snapshot + tail rebuild` even after slices transition to `RECONCILED`.
- **Gift Cards** (prefix-recomputed over `WHERE card_id = $C AND applied_order <= $A ORDER BY occurred_at ASC, event_id ASC`):
  - **Deterministic Offline Eligibility & Per-Epoch Cap Validation**: For every `giftcard_redeem` with server-derived `channel = 'OFFLINE'`, `projectEvent()` verifies:
    1. `e.store_id = ANY(gift_cards.eligible_stores)` (else `REJECTED_INVALID` / `OFFLINE_STORE_NOT_ELIGIBLE`).
    2. Cumulative accepted offline spend for `(card_id, store_id, epoch_gen)` within `applied_order < e.applied_order` does not exceed `ledger_config.giftcard_offline_store_cap_minor` (`2500`):
       ```sql
       SELECT COALESCE(SUM(le.amount_minor), 0)
       FROM ledger_events le
       JOIN event_application_status eas ON eas.event_id = le.event_id
       WHERE le.event_type = 'giftcard_redeem'
         AND le.channel = 'OFFLINE'
         AND le.card_id = $e.card_id
         AND le.store_id = $e.store_id
         AND le.epoch_gen = $e.epoch_gen
         AND le.applied_order < $e.applied_order
         AND eas.status = 'APPLIED';
       ```
       If `prior_epoch_spend + e.amount_minor > cap_minor` → mark `REJECTED_INVALID` (`OFFLINE_CAP_EXCEEDED`) in `event_application_status` without mutating `gift_cards` or `store_day_totals`.
  - For all valid redemptions (`channel = 'ONLINE'` or valid `channel = 'OFFLINE'`): start `running = initial_balance_minor`; for each valid `giftcard_redeem(amt)`, `running -= amt`. If `running < 0`, emit `giftcard_overspend_flags(card_id, event_id, store_id, balance_after_minor = running, overspend_delta_minor = LEAST(amt, -running))`. Final status: `'ADMIN_FROZEN'` if admin-frozen, else `'OVERSPENT_FROZEN'` if `running < 0`, else `'ACTIVE'`.
- **Loyalty Accounts** (prefix-recomputed over `WHERE account_id = $Acc AND applied_order <= $A ORDER BY occurred_at ASC, event_id ASC`):
  - **Deterministic Store/Epoch Total Burn Cap Validation**: For every `loyalty_burn` with server-derived `channel = 'OFFLINE'`, when `ledger_config.loyalty_offline_store_burn_cap > 0` (i.e., `loyalty_slice_points IS NOT NULL`, default `5000` store-wide), `projectEvent()` verifies cumulative accepted offline `loyalty_burn` points across **all accounts** for `(store_id, epoch_gen)` within `applied_order < e.applied_order` plus `e.points <= loyalty_cap` (else `REJECTED_INVALID` / `OFFLINE_LOYALTY_CAP_EXCEEDED`). When `loyalty_offline_store_burn_cap = 0` (`loyalty_slice_points IS NULL`), offline loyalty burns are unlimited.
  - Over all accepted events (`eas.status = 'APPLIED'`): `earned_total = SUM(loyalty_earn.points)`, `burned_total = SUM(loyalty_burn.points)`, `balance = earned_total - burned_total`, `status = 'ACTIVE'` if `balance >= 0` else `'NEGATIVE_RESTRICTED'`.
  - Walking canonical order: any accepted `loyalty_burn` driving/deepening `running < 0` records `loyalty_negative_flags(account_id, trigger_event_id, deficit_points = -running, resolved_by_event_id = NULL)`; any subsequent `loyalty_earn` restoring `running >= 0` stamps `resolved_by_event_id = earn.event_id` on open flags.

### 3.4 Explicit Online-vs-Batch Race SQL Flow (`POST /v1/batches`, `POST /v1/giftcards/redeem`, `POST /v1/loyalty/burn`)

**Batch Path (`POST /v1/batches` — Claim `PENDING`, Project, Then Finalize Receipt)**:
Because `SELECT ... FOR UPDATE` in `READ COMMITTED` cannot see an uncommitted `PENDING` row from a concurrent online transaction, `POST /v1/batches` **claims `online_action_receipts` as `'PENDING'` first** for every `giftcard_redeem` or `loyalty_burn` event in ascending `event_id` order, and finalizes the receipt **after** `projectEvent` completes:
```sql
-- Step A (before ledger_events insert, ordered by event_id ASC):
INSERT INTO online_action_receipts (event_id, action_type, payload_hash, status)
VALUES ($event_id, $event_type, $payload_hash, 'PENDING')
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;

-- If 0 rows returned (conflict with committed OR previously uncommitted PENDING online tx):
SELECT payload_hash, status
FROM online_action_receipts
WHERE event_id = $event_id
FOR UPDATE;
-- * Blocks until any in-flight PENDING online tx commits ('APPROVED' or 'DECLINED').
-- * If existing.payload_hash <> incoming.payload_hash -> ROLLBACK + 409 EVENT_ID_PAYLOAD_MISMATCH.
-- * If status = 'DECLINED' -> skip event (already declined online; do not insert into ledger_events).
-- * If status = 'APPROVED' -> already committed via online path; skip projection as idempotent no-op.

-- Step B (for winner of PENDING claim, AFTER projectEvent completes):
-- * If event_application_status.status = 'APPLIED':
UPDATE online_action_receipts
SET status = 'APPROVED', http_status = 200, response_body = $canonical_approved_json
WHERE event_id = $event_id;
-- * If event_application_status.status = 'REJECTED_INVALID':
UPDATE online_action_receipts
SET status = 'DECLINED', http_status = 409, response_body = $canonical_rejection_json
WHERE event_id = $event_id;
```

**Online Path (`POST /v1/giftcards/redeem` & `POST /v1/loyalty/burn`)**:

```sql
-- Step 1: Claim-first idempotency receipt
INSERT INTO online_action_receipts (event_id, action_type, payload_hash, status)
VALUES ($event_id, $action_type, $payload_hash, 'PENDING')
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;
-- If 0 rows returned -> SELECT payload_hash, status, http_status, response_body
--   FROM online_action_receipts WHERE event_id = $event_id FOR UPDATE;
--   (mismatch -> ROLLBACK + 409 EVENT_ID_PAYLOAD_MISMATCH; match -> return stored response)

-- Step 2 (winner): Lock store_business_days + gift_cards/loyalty_accounts in canonical order,
-- validate status & balance (if insufficient/frozen -> UPDATE receipt DECLINED/409, COMMIT, return 409).

-- Step 3 (approved): Attempt atomic insert into ledger_events with ON CONFLICT RETURNING
INSERT INTO ledger_events (event_id, applied_order, event_type, channel, ...)
VALUES ($event_id, nextval('ledger_applied_order_seq'), $action_type, 'ONLINE', ...)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;

-- Step 4 (explicit online-vs-batch race branch):
-- If 0 rows returned (a concurrent or earlier batch already committed this event_id):
SELECT payload_hash FROM ledger_events WHERE event_id = $event_id;
--   * If existing.payload_hash <> $payload_hash:
--       ROLLBACK (also rolls back the PENDING receipt) and return 409 EVENT_ID_PAYLOAD_MISMATCH.
--   * If existing.payload_hash = $payload_hash:
--       DO NOT mutate gift_cards / loyalty_accounts / store_day_totals again!
--       UPDATE online_action_receipts
--       SET status = 'APPROVED', http_status = 200, response_body = $canonical_idempotent_body
--       WHERE event_id = $event_id;
--       COMMIT and return 200 OK.
-- Else (1 row returned):
--   Apply projection mutations, UPDATE online_action_receipts SET status = 'APPROVED', http_status = 200,
--   response_body = $response_body WHERE event_id = $event_id, COMMIT and return 200 OK.
```

---

## 4. Trusted Local Time, HQ Monotonic Guard & Serialized Epoch-Advance Protocol

### Terminal Durable Slice Grant (`LocalSliceGrant`) & Clock-Tamper Guard
- Issued on sync (`POST /v1/stores/{id}/terminals/{tid}/sync`) with `boot_id` + suspend-aware `CLOCK_BOOTTIME` (`grant_boottime_ms`) captured **before** sending the sync request (discarded if sync RTT $> 30\text{ s}$).
- Hardware envelope: $\rho_{\max} = 1000\text{ ppm}$ ($0.1\%$); `CLOCK_BOOTTIME` advances across OS suspend (or fails closed on suspend/resume if unsupported).
- Every offline spend checks: (1) `local_fenced == false`, (2) `current_boot_id == grant.boot_id`, (3) `current_wall_utc >= grant.hwm_utc`, (4) `current_boottime_ms - grant_boottime_ms < 21,600,000` ms AND `current_wall_utc < slice_expires_at_utc`, (5) for gift cards: `store_id IN card.eligible_stores`, card `ACTIVE` in snapshot, and `spent_by_card_minor[card_id] + amount <= min(snapshot_balance_if_known, grant.slice_minor)`, (6) for loyalty burns: if `grant.loyalty_slice_points != nil` (`NULL` = unlimited), verify `grant.total_loyalty_burned_points + points <= *grant.loyalty_slice_points` across **all accounts** on this terminal slice.

### HQ DB-Anchored Clock Guard & Serialized Two-Replica Epoch-Advance
- **DB-Anchored Clocks & `allow_simulated_clock` Gate**: `terminal_offline_slices.pg_start_ts` captures `pg_postmaster_start_time()` at issuance. `fence_terminal_slice()` and `advance_store_epoch()` derive timestamps directly inside PostgreSQL (`now()` and `clock_timestamp()`), verifying `pg_postmaster_start_time() = slice.pg_start_ts` (failing closed on DB/HQ restart with `409 HQ_CLOCK_STEP_EXPLICIT_ACK_REQUIRED`) and accepting `p_simulated_now` **only** when admin-owned `ledger_config.allow_simulated_clock = true`. Zero untrusted caller-supplied boot/mono parameters exist.
- Locked parameters: `REQUIRE_EXPLICIT_ACK_ONLY=false`, `TERMINAL_CLOCK_MAX_DRIFT_PPM=1000`, `HQ_CLOCK_MAX_DRIFT_PPM=1000`, `SYNC_MAX_RTT_SECONDS=30`, `HQ_FENCE_MARGIN_SECONDS=600`.
- **Per-Card Gift-Card Template & Epoch-Scoped Store Loyalty Cap**: `terminal_offline_slices.slice_minor` applies independently per eligible `(card_id, store_id)` (`sum(ACTIVE + FENCED) <= 2500` minor units); `loyalty_slice_points` (`NULL` = unlimited; `>= 0` = finite) applies as a store/epoch total across all accounts (`sum((epoch_gen = NEW.epoch_gen AND ACTIVE) OR FENCED) <= 5000` points when finite).
- **Single Atomic Sync/Reconcile/Advance Transaction**: Follows global lock order (`sales` → `store_business_days` → `gift_cards` → `loyalty_accounts` → `store_offline_epochs FOR UPDATE` → `terminal_offline_slices`), ingests buffered events (with contiguous `terminal_epoch_seq`), calls `reconcile_terminal_slice(...)` (proving exact 1..N manifest completeness), and calls `advance_store_epoch(...)` (verifying exact set-coverage over immutable `store_epoch_roster`, computing `C_store_avail`/`loyalty_avail`, incrementing `active_epoch_gen`, persisting the new `store_epoch_roster`, and pre-inserting new `ACTIVE` slices).

---

## 5. HTTP API Contract (Phase 1 v10 Aligned)

All endpoints run behind Nginx (`:8080`) across `api-1` and `api-2`. Optional `X-Simulated-Now` (RFC3339 UTC) is honored only when `ALLOW_SIMULATED_CLOCK=true`.

| Method & Path | Role | Isolation | Semantics & Exact Error Codes |
| :--- | :--- | :--- | :--- |
| `GET /healthz` | `ledger_app` | — | Liveness/readiness + replica ID (`200`). |
| `POST /v1/batches` | `ledger_app` | `READ COMMITTED` | Single ACID batch tx (rejects any client-supplied `channel` field with `400 CLIENT_CHANNEL_FORBIDDEN`, always forcing `channel = 'OFFLINE'` server-side; requires `epoch_gen` on offline `giftcard_redeem`/`loyalty_burn` enforced by composite FK; claims `online_action_receipts` first; pre-sorted topologically, post-lock `applied_order`, prefix-scoped projection). Returns `200`, `400` (`CLIENT_CHANNEL_FORBIDDEN`, `FUTURE_TIMESTAMP_EXCEEDED`, `EVENT_TOO_OLD`, `INVALID_AMOUNT`), `409` (`EVENT_ID_PAYLOAD_MISMATCH`). |
| `POST /v1/giftcards/redeem` | `ledger_app` | `READ COMMITTED` | Claim-first online gift-card redeem (`event_id`, `amount_minor > 0`). Returns `200`, `400` (`INVALID_AMOUNT`, `FUTURE_TIMESTAMP_EXCEEDED`), `409` (`INSUFFICIENT_FUNDS`, `CARD_FROZEN`, `EVENT_ID_PAYLOAD_MISMATCH`). |
| `POST /v1/loyalty/burn` | `ledger_app` | `READ COMMITTED` | Claim-first online loyalty burn (`event_id`, `points > 0`). Returns `200`, `400` (`INVALID_AMOUNT`), `409` (`ACCOUNT_NEGATIVE_RESTRICTED`, `INSUFFICIENT_POINTS`, `EVENT_ID_PAYLOAD_MISMATCH`). |
| `POST /v1/stores/{id}/days/{day}/close` | `ledger_app` | `READ COMMITTED` | Explicit day close appending deterministic `UUIDv5` `day_close` event. Returns `200` (idempotent), `409` (`BUSINESS_DAY_CUTOFF_NOT_REACHED`). |
| `POST /v1/stores/{id}/terminals/{tid}/sync` | `ledger_app` | `READ COMMITTED` | Single atomic tx: ingests buffered events (with contiguous `terminal_epoch_seq`), calls `reconcile_terminal_slice(...)` (exact 1..N manifest proof), and calls `advance_store_epoch(...)` (exact `store_epoch_roster` set-coverage barrier + DB-anchored `pg_postmaster_start_time()`). Returns `200` (`LocalSliceGrant` or `BLOCKED_PENDING_SIBLINGS`), `409` (`EVENT_ID_PAYLOAD_MISMATCH`). |
| `POST /v1/stores/{id}/terminals/{tid}/fence` | `ledger_app` | `READ COMMITTED` | Calls DB-anchored `fence_terminal_slice(...)` verifying `pg_postmaster_start_time() = slice.pg_start_ts` and `clock_timestamp()` elapsed $\ge 22{,}200\text{ s}$ (gated by `ledger_config.allow_simulated_clock` for `X-Simulated-Now`); requires `epoch_gen = active_epoch_gen` and `state = 'ACTIVE'`. Returns `200`, `409` (`SLICE_NOT_EXPIRED_YET`, `HQ_CLOCK_STEP_EXPLICIT_ACK_REQUIRED`, `FENCE_NON_ACTIVE_EPOCH`). |
| `GET /v1/stores/{id}/totals?day=YYYY-MM-DD` | `ledger_app` | `REPEATABLE READ READ ONLY` | Returns `store_business_days`, `store_day_totals`, and linked `store_day_adjustments` atomically. |
| `GET /v1/giftcards/{id}` | `ledger_app` | `REPEATABLE READ READ ONLY` | Returns balance, status, `eligible_stores`, and `giftcard_overspend_flags`. |
| `GET /v1/loyalty/{id}` | `ledger_app` | `REPEATABLE READ READ ONLY` | Returns `earned_total`, `burned_total`, `balance`, `status`, and `loyalty_negative_flags`. |
| `GET /v1/ledger/checksum` | `ledger_app` | `REPEATABLE READ READ ONLY` | Returns `{ "economic_sha256", "read_model_sha256", "event_count" }` per **Phase 1 v10** canonical serialization. |
| `POST /v1/admin/seed` | `ledger_admin` | `READ COMMITTED` | Atomically seeds `stores`, `store_terminals`, initial `gift_cards` (with immutable 5-store clusters), and epoch-1 `store_offline_epochs`, `store_epoch_history` (with `pg_start_ts`), `store_epoch_roster`, and `terminal_offline_slices` (satisfying deferred roster-count constraint triggers on commit). |
| `POST /v1/admin/snapshots` | `ledger_admin` | Reader Tx (`REPEATABLE READ READ ONLY` + `pg_export_snapshot()`) + Writer Tx (`REPEATABLE READ READ WRITE` + `SET TRANSACTION SNAPSHOT`) | Captures consistent watermark snapshot via `pg_export_snapshot()`, `SET TRANSACTION SNAPSHOT`, and `admin_capture_snapshot()`. |
| `POST /v1/admin/rebuild?mode=full\|snapshot` | `ledger_admin` | `READ COMMITTED` | Executes `admin_reset_read_models()` (or `admin_restore_snapshot()`) and replays `ledger_events` in `ORDER BY applied_order ASC` with prefix visibility. |
