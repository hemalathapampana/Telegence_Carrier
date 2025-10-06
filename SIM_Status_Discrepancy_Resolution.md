## AMOP SIM Status Discrepancy — Proposed Resolution

### Context
- **Issue**: SIMs are suspended/unknown at the carrier but shown as `Active` in AMOP.
- **Root cause**: AMOP 1.0 holds the previous status for 3 syncs when a device is missing from the carrier daily feed, only then flipping to `Unknown`.
- **Current note**: Presently, statuses reflect carrier except for `Unknown` devices.

### Guiding principles
- **Carrier is the source of truth when data is present**.
- **Missing from a valid feed implies immediate `Unknown`** (do not wait 3 syncs).
- **Protect against bad/incomplete feeds** via a feed-completeness check before mutating statuses.
- **Make freshness explicit** (track last-seen timestamps) to separate data quality from business status.

### Proposed approach (replace the 3-sync hold)
- **Add/ensure fields** (logical or physical):
  - `raw_carrier_status` (latest status as provided by carrier)
  - `last_seen_at_carrier` (timestamp when device was last present in a valid carrier feed)
  - `status_reason` (e.g., `NOT_FOUND_IN_FEED`, `CARRIER_STATUS`, `FEED_INVALID`)
  - `effective_status` (AMOP-computed: `Active`, `Suspended`, `Inactive`, `Unknown`)
- **Feed completeness guard**:
  - Before processing, validate the carrier feed is complete (e.g., control/manifest record, totals hash, or count parity). If invalid, do not update statuses; mark a run-level flag and log/alert.
- **Sync logic** (if feed is valid):
  - For each device present in staging: update `raw_carrier_status`, set `last_seen_at_carrier = now`, compute and set `effective_status` from carrier mapping, and set `status_reason = CARRIER_STATUS`.
  - For devices NOT present in staging ("not matched"):
    - Set `effective_status = Unknown` immediately.
    - Do NOT change `last_seen_at_carrier` (preserve last known good seen-at).
    - Set `status_reason = NOT_FOUND_IN_FEED`.
- **No 3-sync waiting window**: remove/deprecate the holdover logic to avoid discrepancies.

### What to do when the device is not in staging tables
- If the feed is valid: **set `effective_status = Unknown` immediately** with `status_reason = NOT_FOUND_IN_FEED`.
- If the feed is invalid/incomplete: **do not mutate device status**; flag the run and alert.

### Example (the given case)
- Previously `Active` in AMOP; the device doesn’t appear in today’s valid carrier feed.
  - Old behavior: keep `Active` for 3 syncs → discrepancy when carrier shows `Unknown`/not found.
  - New behavior: set `effective_status = Unknown` immediately for that device; discrepancy removed.

### Pseudocode
```pseudo
validate_feed()
if !feed.is_valid:
  record_run_issue("FEED_INVALID"); alert(); return

// Mark all devices as unseen for this run
update devices set seen_this_run = false

// Upsert/merge staging into devices
for each s in staging:
  upsert devices d using key (iccid):
    d.raw_carrier_status = s.carrier_status
    d.last_seen_at_carrier = now()
    d.effective_status = mapCarrierToAmop(s.carrier_status)
    d.status_reason = 'CARRIER_STATUS'
    d.seen_this_run = true

// Any device not seen in a valid feed becomes Unknown immediately
update devices
  set effective_status = 'Unknown', status_reason = 'NOT_FOUND_IN_FEED'
where seen_this_run = false

// Cleanup transient flag
update devices set seen_this_run = null
```

### Edge cases and safeguards
- **Systemic feed issue**: If the entire feed is missing or fails validation, skip status mutations; do not flip many devices to `Unknown` at once unintentionally.
- **Reappearance**: If a previously `Unknown` device reappears in a later valid feed, immediately set `effective_status` from `raw_carrier_status` and update `last_seen_at_carrier`.
- **New devices**: Insert on first sight with mapped `effective_status`.
- **Auditing**: Retain `previous_status`, `status_changed_at`, and `status_reason` for traceability.

### Rollout plan
- **Feature flag** the new logic (`immediate-unknown-on-missing`) for safe enable/disable.
- **Backfill**: Initialize `last_seen_at_carrier` for devices seen in the last N valid feeds.
- **Dry-run** in lower envs with real carrier snapshots; compare discrepancy counts.
- **Incremental enablement** by account/tenant to monitor impact.

### Monitoring and alerts
- **KPIs**: count of devices flipped to `Unknown` per run, by tenant/account; number of feed-invalid runs.
- **Alerts**: spike threshold on Unknown flips; any FEED_INVALID; devices stuck in `Unknown` > X days.

### Acceptance criteria
- When a device is missing from a valid carrier feed, AMOP shows `Unknown` in the same sync.
- When carrier provides a concrete status, AMOP mirrors it in the same sync.
- No waiting window exists that can keep a device `Active` while the carrier shows `Unknown`.
- Bad/incomplete feeds never cause mass flips; they are detected, logged, and alerted.
