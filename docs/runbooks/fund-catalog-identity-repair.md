# Fund catalog identity repair (v1) and the NAV policy republication it enables

`scripts/repair_fund_catalog_identity_v1.py` repairs two legacy catalog defects
that keep most funds out of the builder. The NAV policy generator
(`scripts/generate_fund_nav_policy_v1.py`) classifies them `UNKNOWN`, and the
builder only admits funds whose lifecycle evidence says `ACTIVE`. Lifecycle
evidence is immutable per policy version, so the repair takes effect only after
a NEW policy version is built, audited and published (sections 4 to 7).

## 1. The defects and the rules

Rows written by the retired 2026-03/04 `universe_sync` (allocation monolith):

| | Defect | First failure | Repair rule |
|---|---|---|---|
| (a) | `instruments_universe.isin` holds the SEC series id (`^S[0-9]{9}$`) | `isin.unsupported_prefix` | Set it to NULL only where it equals that instrument's `instrument_identity.sec_series_id`. |
| (b) | `instrument_identity.ticker`/`sec_class_id` name a sibling share class of the IU class (same series). NAV ingestion fetches by the IU ticker, so the IU class is the instrument. | `ticker.mismatch` | Align the registry `ticker`/`sec_class_id` to the IU class. Applied only when all of these hold: the registry row is `canonical` with an empty `conflict_state`; exactly one fresh SEC class of the same series carries the IU ticker; the generator's own SEC judge admits (IU ticker, series, class); and no other instrument claims the ticker, the (series, ticker) pair or the class. The rule iterates to a fixpoint. |

Rule (b) does not touch the registry ISIN, CUSIP or FIGI. On the production
candidates, every CUSIP/FIGI with `sec_cusip_ticker_map` evidence belongs to the
IU ticker, not the registry ticker. Those claims were resolved before the
registry ticker was overwritten, so they already describe the IU class.

`funds_v` projects the registry, so its `ticker` follows. Membership of
`funds_v` does not change: the eligibility gate depends only on `sec_series_id`.

## 2. Safety properties

- **Dry run is the default.** It reads ONE `REPEATABLE READ READ ONLY`
  snapshot and never assigns an xid. It prints aggregates and digests only;
  `--plan-output` writes the per-row plan to a new file.
- **Apply runs in ONE transaction.** It takes `LOCK TABLE ... SHARE ROW
  EXCLUSIVE` before the snapshot, then the NAV writer advisory locks
  (ingestion, then readiness; exit 4 if busy). It then:
  1. recomputes the plan and refuses on any `--plan-sha256` mismatch;
  2. refuses if any ACTIVE fund would be demoted, or if a repaired registry
     row would end in an SEC integrity failure;
  3. writes the receipts, then the updates, each with a compare-and-swap on
     the before-values;
  4. re-reads the catalog and requires an empty re-plan plus exactly the
     predicted classification;
  5. COMMITs.
- **Re-running is a no-op.** After apply, the plan is empty, nothing is
  written and the command exits 0.
- **Exact rollback.** The append-only ledger
  `fund_catalog_identity_repair_runs` / `_receipts`
  (`schemas/fund_catalog_identity_repair_v1.sql`) stores every before/after
  value, including `updated_at` and `identity_sources`. `--rollback RUN_ID`
  restores them byte for byte, all or nothing, only if every touched row still
  holds the run's after-values.

## 3. Repair procedure (production)

Run from the repository root of a clean clone at the merged SHA, with
`NAV_READINESS_DATABASE_URL` set to the `worker_writer` DSN. Avoid these
windows:

- 03:30 UTC Tue-Sat, when `nav-current-daily-chain` runs (apply exits 4 if a
  NAV writer holds its lock);
- 08:00 UTC, when Light's `fund-classification` cron runs.

Before applying, confirm that neither legacy writer in the allocation monolith
is scheduled:

- `universe_sync`: Phase 2 de-duplicates new series by `iu.isin =
  series_id`. After (a), it would insert sibling share classes as new IU rows.
- `identity_resolver`: Source 2 sets `ticker`/`sec_class_id` from the first
  SEC class of a series, at the same authority as the repair's provenance.
  It would record a `conflict_state` on every repaired row.

Production shows no writes to `instruments_universe` since 2026-06-20 and none
to `instrument_identity` since 2026-08-03. If either job is ever re-enabled, it
must be fixed first.

```bash
# 1. Dry run: review the counts and copy plan_sha256.
python -m scripts.repair_fund_catalog_identity_v1 --plan-output /secure/repair-plan.json

# 2. Apply exactly that plan (one transaction). Record run_id.
python -m scripts.repair_fund_catalog_identity_v1 --apply \
  --confirm repair_fund_catalog_identity_v1 --plan-sha256 <plan_sha256>

# 3. Verify: the plan is now empty, and classification.before reports the
#    repaired catalog (ACTIVE and the A4/A8 numbers used in section 5).
python -m scripts.repair_fund_catalog_identity_v1

# Rollback, only if needed:
python -m scripts.repair_fund_catalog_identity_v1 --rollback <run_id> \
  --confirm repair_fund_catalog_identity_v1
```

Refresh the Light read models in the order of Light's `fund-classification`
job, or let its 08:00 UTC cron do it. `funds_profile_mv`, `funds_list_mv` and
`fund_class_resolution_mv` project `funds_v.ticker`:

```sql
REFRESH MATERIALIZED VIEW funds_profile_mv;
REFRESH MATERIALIZED VIEW funds_list_mv;
REFRESH MATERIALIZED VIEW fund_class_resolution_mv;
REFRESH MATERIALIZED VIEW fund_benchmark_candidates_mv;
```

## 4. Blocker before any new policy can be published: A8 integrity

The strict audit requires zero SEC integrity first failures (contract round 7,
`integrity_ceiling = 0`), and the operator refuses publication otherwise.

On 2026-10-06 the catalog already has 1 `sec.contradiction` (WINC, whose
ticker is in two series) before the repair. After the repair it has 6. The
other 5 are VVPLX, VVPSX, STNC, EASG and MMLG. Their registry ticker/class are
untouched, but SEC now maps those tickers to a new series or the class to a
new ticker. (a) only unmasks them; the series-id ISIN used to fail first.

These need an explicit decision. One option is to record the conflict in the
registry `conflict_state`, which moves them to the pre-claim failure
`registry.conflict_state_not_empty`; they stay `UNKNOWN` either way. The other
is to align them to SEC's current mapping. This repair does neither.

## 5. Audit config re-pin (reviewed commit to this repository)

The operator pins the SHA-256 of `configs/nav_identity_audit_v3.json`. Take
the A4 numbers from the post-apply dry run (`classification.before.gates.a4`).
On 2026-10-06 the predicted values were:

| Key | Current | Required after repair | Note |
|---|---|---|---|
| `structural_daily_ceiling` | 5103 | at least P | P is 7,410 predicted |
| `structural_baseline` | 5103 | keep | |
| `accepted_structural_delta` | null | B − 5103 | B is 7,411 predicted, so 2,308 |

`accepted_structural_delta` must equal the delta at audit time exactly.

Also confirm or re-pin `builder.light_revision`, the cohort query and
`stage1_quotas`. The config is pinned to Light `aeb59337`; Light `main` has
moved since.

## 6. Build and audit a new policy version (POSIX host only)

`build` refuses non-POSIX hosts and needs a private custody root: mode 0700,
outside any git checkout. Reuse the requested coverage of `2026-09-25.3`
(sessions 2024-01-02..2027-12-31). The SEC crosswalk must have been synced
within 7 days, otherwise `sec_source_stale` aborts the build.

```bash
R=/srv/nav-custody/<YYYY-MM-DD>; install -d -m 700 "$R"
V=<YYYY-MM-DD>.1        # new, never reused
python -m scripts.generate_fund_nav_policy_v1 calendar \
  --coverage-start 2024-01-01 --coverage-end 2027-12-31 --output "$R/calendar.json"
python -m scripts.generate_fund_nav_policy_v1 build \
  --coverage-start 2024-01-01 --coverage-end 2027-12-31 \
  --policy-id current-daily-nav-xnys-usd-adjusted --policy-version "$V" \
  --custody-root "$R" --output "$R/policy.json" \
  --source-snapshot-output "$R/source-snapshot.json"
python -m scripts.generate_fund_nav_policy_v1 verify \
  --policy-file "$R/policy.json" --source-snapshot-file "$R/source-snapshot.json"
# Strict live audit; --canary-output implies --strict. Exit 0 only if A1-A8 all PASS.
python -m scripts.verify_fund_nav_identity_v2 \
  --policy-file "$R/policy.json" --source-snapshot-file "$R/source-snapshot.json" \
  --dsn-env NAV_READINESS_DATABASE_URL --capture-output "$R/capture.json" \
  --audit-config configs/nav_identity_audit_v3.json \
  --previous-policy-file "$R/previous-v1-policy.json" --previous-policy-sha256 <sha256> \
  --custody-root "$R" --output "$R/audit.json" --canary-output "$R/canary.json"
```

## 7. Publish (governed operator), then re-run the chain

```bash
SQL=$(sha256sum schemas/fund_nav_readiness_v1.sql | cut -d' ' -f1)
h() { sha256sum "$1" | cut -d' ' -f1; }
PINS=(--schema public --expected-sql-sha256 "$SQL" --custody-root "$R"
  --policy-file "$R/policy.json" --policy-sha256 "$(h "$R/policy.json")"
  --audit-dossier-file "$R/audit.json" --audit-dossier-sha256 "$(h "$R/audit.json")"
  --canary-manifest-file "$R/canary.json" --canary-manifest-sha256 "$(h "$R/canary.json")"
  --capture-file "$R/capture.json" --capture-sha256 "$(h "$R/capture.json")")
python -m scripts.fund_nav_readiness_schema --mode check "${PINS[@]}"   # prints plan_sha256
python -m scripts.fund_nav_readiness_schema --mode apply "${PINS[@]}" --plan-sha256 <plan_sha256>
```

The operator enforces the receipt window at check and again at apply:
`captured_at <= now <= policy.valid_through`, and `now <= sec_valid_until`,
where `sec_valid_until` is the oldest matched SEC `updated_at` + 7 days.
Publish within that window.

Then run `nav-current-daily-chain` once by triggering a new deployment of the
service, so readiness is re-published under the new pointer. Re-test
`https://hub.investintell.com/builder`. Newly ACTIVE funds still need NAV
coverage (401 endpoints) and a current last NAV before the builder admits them.
