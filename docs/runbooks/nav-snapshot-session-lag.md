# Runbook: NAV readiness snapshot session lag (schema apply)

## What changes

`fund_nav_snapshot_current_at_v1` used to require the due session to equal the
snapshot's `as_of_session` and the closed session to equal its
`latest_closed_session`. Each session closes at 20:00 UTC and is due at
22:05 UTC, but the chain only publishes the next snapshot after Tiingo delivers
NAVs (06:00-07:15 UTC). The builder was therefore unavailable for about 12 hours
every trading day.

A snapshot for session D now stays current while:

- `due_session >= as_of_session` and at most `MAX_SNAPSHOT_SESSION_LAG` (1)
  sessions after it, and
- `closed_session >= latest_closed_session` and at most 1 session after it.

Sessions are counted as `nav_valuation_schedules` rows of the snapshot's own
calendar, never as calendar days. Two sessions of lag is stale, so a missed
chain run still fails closed on the next trading day. All other terms are
unchanged: policy window and hash pins, risk publication pins, NAV head
revision, re-expression holds, lifecycle evidence and feature-evidence binding.
Light computes on the snapshot's own as-of session and grid within the
allowance.

The constant lives in `src/workers/_nav_policy.py` and as a literal in the SQL
function. Light mirrors it as `app.optimizer.nav_policy.MAX_SNAPSHOT_SESSION_LAG`.
Tests in both repositories pin the three values to each other.

Timeline for a snapshot published after the session-D NAVs arrive
(`as_of = D`, `latest_closed = D`):

| Instant (UTC) | due / closed | Current |
|---|---|---|
| publication, D+1 07:30 | D / D | yes |
| D+1 20:00 | D / D+1 | yes (closed lag 1) |
| D+1 22:05 | D+1 / D+1 | yes (due lag 1) |
| D+2 20:00, no new publication | D+1 / D+2 | no (closed lag 2) |

## Schema pins

| Artifact | SHA-256 |
|---|---|
| `schemas/fund_nav_readiness_v1.sql` | `daf13576421d744a081f3d5478fb6c273bc5b21be7778d3d6833173a1cba2533` |
| `schemas/fund_nav_readiness_v1.catalog.json` (file) | `cbbd886b2290184925e0a9c4d6417c63c41818c9d60a63c9394aa0cb72556704` |
| catalog `signature_sha256` | `04d523f6f1a60752988e8d59b29058c19f9cb37a891953bd994c0fb4af7dd218` |
| access profile `signature_sha256` (unchanged) | `c8fbce2a57f795e1fc713e6fbbdcbd1b8f6f4c83f5066755016fd0cba6c223a6` |

The previous DDL was `687b019c...`. Production still carries its snapshot
function body (`572502da...`). The operator classifies that exact body, with
every other attribute equal to the manifest, as `repairable`
(`PREDECESSOR_FUNCTION_BODIES`). Any other divergence stays `incompatible`. The
apply runs `CREATE OR REPLACE FUNCTION`, which keeps the owner, the PUBLIC
revoke and the `app_runtime` EXECUTE grant.

## Order

1. Wait until any in-flight governed policy publication planned against
   `687b019c...` has committed. A plan-v4 digest binds the DDL SHA, so such a
   publication must finish on the old pins.
2. Merge the workers PR. Merging `main` redeploys the git-connected cron
   services. Runtime workers (chain, readiness, risk metrics) do not read the
   catalog manifest and keep running. Governed operators running merged code
   (`fund_nav_readiness_schema`, `rebase_fund_nav_window`) report
   `upgrade_required` and exit 3 without writes until step 3 is done.
3. Apply the schema (below). The change only relaxes the predicate, so Light's
   current build keeps working: its own context check is still exact-session.
4. Deploy the Light API from the companion PR.

## Apply (production)

From a clean clone at the merged SHA (`core.autocrlf false`), with
`NAV_READINESS_DATABASE_URL` set to the `worker_writer` DSN. Run it after the
day's `nav-current-daily-chain` and outside ingestion: the DDL takes
`lock_timeout = 2s`, so a busy writer rolls the whole transaction back and the
apply can simply be retried.

```bash
SQL=$(sha256sum schemas/fund_nav_readiness_v1.sql | cut -d' ' -f1)
test "$SQL" = daf13576421d744a081f3d5478fb6c273bc5b21be7778d3d6833173a1cba2533
PINS=(--schema public --expected-sql-sha256 "$SQL")

# Read-only. Expect status "planned", compatibility "repairable", code null.
python -m scripts.fund_nav_readiness_schema --mode check "${PINS[@]}"   # prints plan_sha256

# Expect status "applied", ddl "applied", compatibility "exact".
python -m scripts.fund_nav_readiness_schema --mode apply "${PINS[@]}" --plan-sha256 <plan_sha256>

# Expect status "ready".
python -m scripts.fund_nav_readiness_schema --mode check "${PINS[@]}"
```

Verify the live read model afterwards: `SELECT bool_and(snapshot_current) FROM
fund_nav_readiness_current_v1 WHERE admissible` stays true after 22:05 UTC. It
stays true until the next ingestion writes NAV rows for the new session. Each
write advances that instrument's NAV head revision, and the unchanged head pin
makes it stale until readiness is republished. That ingestion-to-publication
window is the outage that remains each morning.

## Rollback

Re-apply the previous DDL from a clone of the commit before this change with
`--expected-sql-sha256 687b019cfd546aa69e7a6024e3fd3cb615b3e84622f24ec3b7b6ff2e5a5c9cf4`.
That operator does not recognise the new body, so restore it first with the
Round4 function in `tests/fixtures/nav_snapshot_current_at_round4.sql`, applied
with `SET search_path TO public, pg_temp`. Light needs no rollback: against the
old predicate it fails closed (`SNAPSHOT_STALE`) during the lag window, as
before.
