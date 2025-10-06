## AMOP SIM Status Sync — Immediate Unknown On Missing (PostgreSQL)

This document provides a production-ready PostgreSQL stored function that resolves the SIM status discrepancy by immediately flipping devices to `Unknown` when they are missing from a valid carrier feed. It includes:
- A feed-completeness guard (skip mutations if the feed run is invalid)
- A feature flag (`immediate-unknown-on-missing`) to safely enable/disable the new behavior
- Freshness tracking (`last_seen_at_carrier`), audit logging, and KPI queries

Adjust table and column names to match your schema if they differ. The implementation is safe-by-default: it mutates nothing when a feed run is invalid.

---

## Guiding principles encoded
- **Carrier is source of truth when data is present**: present-in-feed rows directly set AMOP `effective_status` from a mapping of carrier status.
- **Missing from a valid feed implies immediate `Unknown`**: absent rows are marked `Unknown` right away when the feature flag is enabled.
- **Protect against bad feeds**: if the feed run is invalid, status mutations are skipped entirely, and the run is flagged.
- **Make freshness explicit**: `last_seen_at_carrier` and `last_seen_feed_run_id` are updated only when present in a valid feed.

---

## Prerequisites (schema expectations)

The procedure assumes the following logical model. If your schema differs, adapt the names in the stored function accordingly.

- `devices(tenant_id uuid, iccid text, raw_carrier_status text, last_seen_at_carrier timestamptz, last_seen_feed_run_id bigint, effective_status device_effective_status, previous_effective_status device_effective_status, status_reason device_status_reason, status_changed_at timestamptz, created_at timestamptz, updated_at timestamptz)`
  - Unique key on `(tenant_id, iccid)`
- `staging_carrier_devices(feed_run_id bigint, tenant_id uuid, iccid text, carrier_status text)`
  - Contains one row per device present in a specific carrier feed run
- `carrier_feed_runs(id bigint primary key, tenant_id uuid null, is_valid boolean, processed_at timestamptz, unknown_flips_count integer)`
  - `is_valid=true` means completeness checks passed for the run; when invalid, the procedure exits early
- `feature_flags(key text, enabled boolean, tenant_id uuid null, created_at timestamptz)`
  - Key `immediate-unknown-on-missing` controls the new behavior
- `device_status_audit(iccid text, tenant_id uuid, feed_run_id bigint, previous_effective_status device_effective_status, new_effective_status device_effective_status, status_reason device_status_reason, recorded_at timestamptz)`

If you need helper DDL to add missing columns and types, see the sample migrations below.

---

## Enums (safe creation helper)

```sql
-- Create enums if they do not exist
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'device_effective_status') THEN
    CREATE TYPE device_effective_status AS ENUM ('Active','Suspended','Inactive','Unknown');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'device_status_reason') THEN
    CREATE TYPE device_status_reason AS ENUM ('CARRIER_STATUS','NOT_FOUND_IN_FEED','FEED_INVALID');
  END IF;
END$$;
```

---

## Table adjustments (optional helper DDL)

```sql
-- Add columns to devices if missing
ALTER TABLE devices
  ADD COLUMN IF NOT EXISTS raw_carrier_status text,
  ADD COLUMN IF NOT EXISTS last_seen_at_carrier timestamptz,
  ADD COLUMN IF NOT EXISTS last_seen_feed_run_id bigint,
  ADD COLUMN IF NOT EXISTS effective_status device_effective_status DEFAULT 'Unknown' NOT NULL,
  ADD COLUMN IF NOT EXISTS previous_effective_status device_effective_status,
  ADD COLUMN IF NOT EXISTS status_reason device_status_reason DEFAULT 'CARRIER_STATUS' NOT NULL,
  ADD COLUMN IF NOT EXISTS status_changed_at timestamptz,
  ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS updated_at timestamptz DEFAULT now();

-- Ensure uniqueness on tenant + iccid
CREATE UNIQUE INDEX IF NOT EXISTS ux_devices_tenant_iccid ON devices(tenant_id, iccid);

-- Ensure useful indexes
CREATE INDEX IF NOT EXISTS ix_staging_feed_tenant_iccid ON staging_carrier_devices(feed_run_id, tenant_id, iccid);
CREATE INDEX IF NOT EXISTS ix_devices_effective_status ON devices(effective_status);
```

---

## Mapping helper (carrier status -> AMOP effective status)

```sql
CREATE OR REPLACE FUNCTION amop_map_carrier_status(_raw text)
RETURNS device_effective_status
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF _raw IS NULL THEN
    RETURN 'Unknown';
  END IF;

  CASE upper(_raw)
    WHEN 'ACTIVE', 'ACTIVATED', 'LIVE' THEN RETURN 'Active';
    WHEN 'SUSPENDED', 'BARRED' THEN RETURN 'Suspended';
    WHEN 'INACTIVE', 'DISCONNECTED', 'DEACTIVATED', 'CANCELLED', 'TERMINATED' THEN RETURN 'Inactive';
    ELSE RETURN 'Unknown';
  END CASE;
END;
$$;
```

---

## Stored function: immediate Unknown on missing (feature-flagged)

```sql
CREATE OR REPLACE FUNCTION amop_sync_device_status(
  p_feed_run_id bigint,
  p_tenant_id uuid DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_now timestamptz := now();
  v_is_valid boolean;
  v_ff_enabled boolean := false;
  v_unknown_flips integer := 0;
BEGIN
  -- 1) Guard: feed must be valid (completeness check performed upstream)
  SELECT r.is_valid
    INTO v_is_valid
    FROM carrier_feed_runs r
   WHERE r.id = p_feed_run_id
     AND (p_tenant_id IS NULL OR r.tenant_id = p_tenant_id OR r.tenant_id IS NULL)
   LIMIT 1;

  IF COALESCE(v_is_valid, false) = false THEN
    -- Optionally record a run-level issue; choose one of these patterns based on your schema
    -- UPDATE carrier_feed_runs SET processed_at = v_now WHERE id = p_feed_run_id;
    RAISE NOTICE 'Feed run % invalid; skipping status mutations', p_feed_run_id;
    RETURN;
  END IF;

  -- 2) Feature flag for immediate Unknown-on-missing (tenant-specific overrides global)
  SELECT COALESCE(ff.enabled, false)
    INTO v_ff_enabled
    FROM feature_flags ff
   WHERE ff.key = 'immediate-unknown-on-missing'
     AND (ff.tenant_id = p_tenant_id OR ff.tenant_id IS NULL)
   ORDER BY (ff.tenant_id IS NULL), ff.created_at DESC
   LIMIT 1;

  -- 3) Present-in-feed: update existing devices
  WITH s AS (
    SELECT s.iccid, s.tenant_id, s.carrier_status
      FROM staging_carrier_devices s
     WHERE s.feed_run_id = p_feed_run_id
       AND (p_tenant_id IS NULL OR s.tenant_id = p_tenant_id)
  ),
  upd AS (
    UPDATE devices d
       SET raw_carrier_status    = s.carrier_status,
           last_seen_at_carrier  = v_now,
           last_seen_feed_run_id = p_feed_run_id,
           previous_effective_status = CASE
             WHEN d.effective_status IS DISTINCT FROM amop_map_carrier_status(s.carrier_status) THEN d.effective_status
             ELSE d.previous_effective_status
           END,
           effective_status      = amop_map_carrier_status(s.carrier_status),
           status_reason         = 'CARRIER_STATUS'::device_status_reason,
           status_changed_at     = CASE
             WHEN d.effective_status IS DISTINCT FROM amop_map_carrier_status(s.carrier_status) THEN v_now
             ELSE d.status_changed_at
           END,
           updated_at            = v_now
      FROM s
     WHERE d.iccid = s.iccid
       AND d.tenant_id = s.tenant_id
    RETURNING d.iccid, d.tenant_id, d.previous_effective_status, d.effective_status
  ),
  ins AS (
    INSERT INTO devices (
      iccid, tenant_id, raw_carrier_status, last_seen_at_carrier, last_seen_feed_run_id,
      effective_status, previous_effective_status, status_reason, status_changed_at,
      created_at, updated_at
    )
    SELECT s.iccid, s.tenant_id, s.carrier_status, v_now, p_feed_run_id,
           amop_map_carrier_status(s.carrier_status), NULL, 'CARRIER_STATUS', v_now,
           v_now, v_now
      FROM s
     WHERE NOT EXISTS (
             SELECT 1 FROM devices d
              WHERE d.iccid = s.iccid AND d.tenant_id = s.tenant_id
           )
    RETURNING iccid, tenant_id, previous_effective_status, effective_status
  )
  INSERT INTO device_status_audit (
    iccid, tenant_id, feed_run_id, previous_effective_status, new_effective_status, status_reason, recorded_at
  )
  SELECT iccid, tenant_id, p_feed_run_id, previous_effective_status, effective_status,
         'CARRIER_STATUS'::device_status_reason, v_now
    FROM (
      SELECT * FROM upd
      UNION ALL
      SELECT * FROM ins
    ) AS changes
   WHERE changes.previous_effective_status IS DISTINCT FROM changes.effective_status;

  -- 4) Absent-from-feed: immediate Unknown (if feature flag enabled)
  IF v_ff_enabled THEN
    WITH st AS (
      SELECT s.iccid, s.tenant_id
        FROM staging_carrier_devices s
       WHERE s.feed_run_id = p_feed_run_id
         AND (p_tenant_id IS NULL OR s.tenant_id = p_tenant_id)
    ),
    upd_unknown AS (
      UPDATE devices d
         SET previous_effective_status = CASE
               WHEN d.effective_status IS DISTINCT FROM 'Unknown'::device_effective_status THEN d.effective_status
               ELSE d.previous_effective_status
             END,
             effective_status      = 'Unknown'::device_effective_status,
             status_reason         = 'NOT_FOUND_IN_FEED'::device_status_reason,
             status_changed_at     = CASE
               WHEN d.effective_status IS DISTINCT FROM 'Unknown'::device_effective_status THEN v_now
               ELSE d.status_changed_at
             END,
             updated_at            = v_now
       WHERE (p_tenant_id IS NULL OR d.tenant_id = p_tenant_id)
         AND NOT EXISTS (
               SELECT 1 FROM st s
                WHERE s.iccid = d.iccid AND s.tenant_id = d.tenant_id
             )
      RETURNING d.iccid, d.tenant_id, d.previous_effective_status, d.effective_status
    )
    INSERT INTO device_status_audit (
      iccid, tenant_id, feed_run_id, previous_effective_status, new_effective_status, status_reason, recorded_at
    )
    SELECT iccid, tenant_id, p_feed_run_id, previous_effective_status, effective_status,
           'NOT_FOUND_IN_FEED'::device_status_reason, v_now
      FROM upd_unknown
     WHERE previous_effective_status IS DISTINCT FROM effective_status;

    GET DIAGNOSTICS v_unknown_flips = ROW_COUNT;  -- number of Unknown flips recorded

    UPDATE carrier_feed_runs
       SET unknown_flips_count = COALESCE(unknown_flips_count, 0) + COALESCE(v_unknown_flips, 0),
           processed_at        = v_now
     WHERE id = p_feed_run_id;
  ELSE
    -- Feature flag disabled: still mark run processed, but do not flip to Unknown
    UPDATE carrier_feed_runs
       SET processed_at = v_now
     WHERE id = p_feed_run_id;
  END IF;
END;
$$;
```

### Notes
- The function updates present devices in all cases when the feed is valid, regardless of the feature flag. The flag only controls the immediate `Unknown` transition for devices absent from the feed.
- `last_seen_at_carrier` and `last_seen_feed_run_id` update only for present devices. Absent devices preserve prior `last_seen_at_carrier`.
- Auditing records only when effective status actually changes (avoids noisy inserts).
- The `NOT EXISTS` anti-join ensures devices not present in the current valid feed are targeted without needing a transient `seen_this_run` flag in `devices`.

---

## Usage

- Validate the feed run and set `carrier_feed_runs.is_valid = true` only if the completeness checks pass (manifest, counts, hashes, etc.).
- Call the function:

```sql
-- For a specific tenant
SELECT amop_sync_device_status(p_feed_run_id => 123456, p_tenant_id => '00000000-0000-0000-0000-000000000000');

-- For all tenants present in the feed run
SELECT amop_sync_device_status(p_feed_run_id => 123456);
```

- To (temporarily) disable immediate Unknown on missing:

```sql
-- Disable globally
INSERT INTO feature_flags(key, enabled, tenant_id, created_at)
VALUES ('immediate-unknown-on-missing', false, NULL, now());
```

---

## Monitoring and KPIs

- **Unknown flips per run (by tenant)**
```sql
SELECT a.feed_run_id,
       d.tenant_id,
       COUNT(*) AS unknown_flips
  FROM device_status_audit a
  JOIN devices d
    ON d.iccid = a.iccid AND d.tenant_id = a.tenant_id
 WHERE a.new_effective_status = 'Unknown'
 GROUP BY a.feed_run_id, d.tenant_id
 ORDER BY a.feed_run_id DESC, d.tenant_id;
```

- **Feed-invalid runs (last 7 days)**
```sql
SELECT COUNT(*) AS invalid_runs
  FROM carrier_feed_runs
 WHERE is_valid = false
   AND processed_at >= now() - interval '7 days';
```

- **Devices stuck in Unknown > X days (by tenant)**
```sql
SELECT tenant_id,
       COUNT(*) AS devices_stuck_unknown
  FROM devices
 WHERE effective_status = 'Unknown'
   AND COALESCE(status_changed_at, created_at) < now() - interval '7 days'  -- adjust horizon
 GROUP BY tenant_id
 ORDER BY devices_stuck_unknown DESC;
```

- **Spike detection for Unknown flips (example threshold)**
```sql
WITH flips AS (
  SELECT a.feed_run_id, d.tenant_id, COUNT(*) AS unknown_flips
    FROM device_status_audit a
    JOIN devices d ON d.iccid = a.iccid AND d.tenant_id = a.tenant_id
   WHERE a.new_effective_status = 'Unknown'
   GROUP BY a.feed_run_id, d.tenant_id
)
SELECT *
  FROM flips
 WHERE unknown_flips >= 1000;  -- example threshold
```

---

## Rollout plan (recommended)
- Enable the feature flag in lower environments and run against historical snapshots.
- Compare discrepancy counts before/after with the KPI queries.
- Gradually enable the flag per tenant; monitor Unknown flips spikes and stuck-Unknown metrics.
- Keep the invalid-feed guard in place permanently.

---

## Why this resolves the discrepancy
- When a device is missing from a valid feed, its `effective_status` flips to `Unknown` in the same sync, removing drift with the carrier.
- When the carrier provides a concrete status, AMOP mirrors it immediately.
- Bad or incomplete feeds do not cause mass flips because the procedure exits early unless `is_valid=true`.
- Freshness (`last_seen_at_carrier`) and audit trails make data quality transparent and traceable.
