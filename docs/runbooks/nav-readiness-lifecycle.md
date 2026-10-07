# NAV readiness lifecycle

This change preserves the existing one-session allowance. It does not extend
policy expiry, accept unverified NAVs, or reuse a replaced risk generation.
The companion Light change reads the published snapshot's policy rather than
requiring that policy to remain the policy pointer's current target. Its
immutable publication instant must be no later than the active policy version's
publication instant. Rolling the pointer back rejects a newer snapshot; the
fresh pointer movement timestamp never changes version ordering.

## Design and remaining intervals

| Window | Cause | Change | Remaining interval |
|---|---|---|---|
| Policy publication | Moving the policy pointer invalidated the previous readiness policy join. | Readiness remains bound to its own immutable published policy and hash. The current policy pointer must exist and target a version published no earlier than the snapshot policy; current lifecycle restrictions still veto admission. | No policy-pointer-only outage for an otherwise valid snapshot. Expiry, session lag and restrictive lifecycle changes still reject immediately. |
| Risk still on the previous policy | Readiness could replace the last good snapshot with rows that cannot match the new policy's risk evidence. | Defer without publishing until risk is complete and published for the exact policy and target session. | The last snapshot remains usable until another correctness pin expires or changes. A risk-generation fence remains as described below. |
| Provider has not published the due session | Wall-clock due time preceded the provider's mutual-fund publication. | The chain and rebase planner recognize fresh successful provider observations ending at the previous session for a majority of the active cohort, and return a retryable outcome. The chain stops before risk and readiness. | No new snapshot or coverage alarm from a proven provider-pending deferral. Availability still ends at the existing session-lag or policy-expiry boundary. Retry after the provider publishes. |
| Ingestion advances the NAV head | Any new head invalidated a snapshot even when its 401 input levels were unchanged. | Permit a higher head only when the immutable revision ledger proves a provider-attributed append strictly after the snapshot's as-of session, including that new row's derived-return initialization in the same transaction. | No append-only ingestion outage. Historical inserts, later updates, deletes, unsupported lineage, holds and risk changes remain fail-closed until recomputation. |
| Policy evidence publication | Per-row reads and writes held the publication transaction open across network round trips. | COPY into private staging before writer locks, then validate conflicts and publish using set operations in the governed transaction; batch replay validation too. | Upload and local validation occur before the publication locks. Transaction time still includes server validation, inserts and commit; no production timing is claimed. |

A proven provider-pending majority plus a fresh failed provider attempt returns
`PROVIDER_SESSION_PENDING_WITH_ERRORS`, an explicit retryable blocked result.
It also stops before risk and readiness, preserving both pointers while making
the failure visible. Pure provider delay uses `PROVIDER_SESSION_PENDING`.
The rebase planner uses the same outcomes and creates no plan for either case.
Neither outcome schedules an automatic retry or changes the cron.

The append exception is not a blanket tolerance for a higher head. Ingestion
may restamp overlapping historical rows when the calendar version changes;
those updates still invalidate the old snapshot even if NAV levels are equal.

Risk features currently come from the mutable `fund_risk_latest_mv`. The risk
worker invalidates its singleton before a potentially served write. Keeping an
old readiness snapshot through that mutation would accept an unpinned feature
generation. This fence is deliberately retained: the residual interval starts
when risk invalidates its publication and ends when matching readiness commits.
It is the actual risk-plus-readiness duration, not a fixed timeout. Provider
deferral avoids entering that interval before NAVs are available. Removing it
would require a separately versioned risk read model or atomic generation
publication, beyond this change.

## Governed schema apply and deployment order

Run these steps only during the separately authorized production change. No
production mutation is part of preparing these PRs.

| Artifact | SHA-256 |
|---|---|
| `schemas/fund_nav_readiness_v1.sql` | `bbad6e2ba30054da517c3a673a9cf0d8a454d647b69fbc3ec1d104a48bed55a3` |
| Catalog manifest file | `9fcf1de008c64f6aca498e36895570d21b72235dfc04ea45b994bcb263dd9da6` |
| Catalog signature | `169db7aad9f1d5395bfd88a540204bc0f8f8f4fff4a3c1db0a4df00547a34ac2` |
| Access profile signature (unchanged) | `c8fbce2a57f795e1fc713e6fbbdcbd1b8f6f4c83f5066755016fd0cba6c223a6` |

The catalog manifest was regenerated with
`python -m scripts.generate_fund_nav_readiness_catalog --write` and verified
without `--write` on the pinned local PostgreSQL 18.4 / TimescaleDB 2.27.2
reference server. The pins above include the snapshot policy rollback guard.

The immediately preceding released SQL is
`daf13576421d744a081f3d5478fb6c273bc5b21be7778d3d6833173a1cba2533`.
Its snapshot function body is explicitly recognized by
`PREDECESSOR_FUNCTION_BODIES` as repairable only when all other catalog
attributes match. The fixture
`tests/fixtures/nav_snapshot_current_at_session_lag.sql` preserves that exact
body. Unknown function changes remain incompatible. No access grant changes
are part of this release. The unreleased `ffc0fa5d...` body is replaced, not
added as another recognized in-place predecessor.

1. Wait for ingestion, risk, rebase and governed policy jobs to finish. Merging
   workers `main` redeploys the git-connected services and can stop running jobs.
2. Merge the workers PR and use a clean checkout of that exact merge commit with
   LF SQL bytes (`git -c core.autocrlf=false clone ...`). Record the commit,
   SQL SHA and catalog manifest SHA. Deploy no Light change yet.
3. Run the governed schema operator's check/apply/check below from the workers
   checkout. Use the existing authorized operator connection, preferably in the
   private network, with `NAV_READINESS_DATABASE_URL` set out of band. Never
   print the DSN. Do not invoke `psql -f` or edit the function manually.
4. Verify schema compatibility is `exact`, the access profile is unchanged,
   and the apply did not move either policy or readiness pointer.
5. Merge and deploy the companion Light API change. The old Light code is
   conservative against the new predicate; deploying Light first does not
   provide the new database semantics.
6. After due-session NAVs are available, run the existing full
   `nav_current_daily_chain` lane once and verify its risk and readiness
   publications. Keep the existing cron schedule and coverage floors.

The SQL and manifest hashes are pinned by the committed catalog and verified
by the operator; the release values and validation evidence are recorded in
the PR. Obtain the check plan from the target database immediately before
apply. Any stale plan, unknown function body, incompatible catalog, receipt
failure, or busy writer requires a fresh check; do not bypass a refusal.

```bash
SQL=$(python -c 'import hashlib; from pathlib import Path; print(hashlib.sha256(Path("schemas/fund_nav_readiness_v1.sql").read_bytes()).hexdigest())')
test "$SQL" = bbad6e2ba30054da517c3a673a9cf0d8a454d647b69fbc3ec1d104a48bed55a3
python -c 'import hashlib; from pathlib import Path; assert hashlib.sha256(Path("schemas/fund_nav_readiness_v1.catalog.json").read_bytes()).hexdigest() == "9fcf1de008c64f6aca498e36895570d21b72235dfc04ea45b994bcb263dd9da6"'
python -c 'import json,sys; from pathlib import Path; m=json.loads(Path("schemas/fund_nav_readiness_v1.catalog.json").read_text()); print("SQL SHA:",sys.argv[1]); print("catalog signature:",m["signature_sha256"])' "$SQL"
PINS=(--schema public --expected-sql-sha256 "$SQL")

python -m scripts.fund_nav_readiness_schema --mode check "${PINS[@]}" > nav-lifecycle-check.json
cat nav-lifecycle-check.json
# Inspect the target identity and compatibility; then use this check's plan.
PLAN=$(python -c 'import json; r=json.load(open("nav-lifecycle-check.json")); assert r["status"] in ("planned", "ready"); assert r["code"] is None; print(r["plan_sha256"])')
python -m scripts.fund_nav_readiness_schema --mode apply "${PINS[@]}" --plan-sha256 "$PLAN"
python -m scripts.fund_nav_readiness_schema --mode check "${PINS[@]}"
```

Schema-only apply requires no new policy artifact. A subsequent policy publish
uses the existing governed policy, audit dossier, capture, custody root and
canary manifest arguments and their exact hashes. Regenerate its plan with
this operator; an old plan pins the old SQL and cannot be reused. Staging
changes transport cost only: publication remains atomic with the receipt,
partition count, audited previous pointer and policy-content checks intact.
Policy publication now requires the operator connection to have the database's
`TEMP` privilege for its private staging tables. Verify it with
`SELECT has_database_privilege(current_user, current_database(), 'TEMP');`
before a policy publish; use the existing authorized operator role. A failure
to stage occurs before the governed publication transaction.

Read back using a read-only connection:

```sql
SELECT readiness_profile, policy_id, policy_version, published_at
FROM nav_policy_current;
SELECT p.run_id, p.published_at, r.policy_id, r.policy_version,
       r.as_of_session, r.risk_publication_revision, r.published_risk_run_id
FROM fund_nav_readiness_current p
JOIN fund_nav_readiness_runs r USING (run_id);
SELECT state, revision_id, published_risk_run_id
FROM fund_nav_risk_publication;
SELECT fund_status, admissible, snapshot_current, reason_code, count(*)
FROM fund_nav_readiness_current_v1 GROUP BY 1,2,3,4 ORDER BY 1,2,3,4;
```

Check the builder with a previously admitted fund as well as a deliberately
ineligible fund. During a provider-pending result confirm no readiness pointer
movement, no risk invocation and no `nav_coverage_alarm`; the process still
reports a retryable non-success, so a scheduler must retry explicitly.

For rollback, revert the Light consumer first if needed; it restores the
stricter policy-pointer behavior. Do not apply an older SQL file directly:
its operator does not recognize the successor function body. Restore older
database semantics through a reviewed governed successor, or roll forward.

## Local verification

One bounded local measurement used 20,077 synthetic lifecycle rows and 401
calendar sessions on PostgreSQL 18.4 / TimescaleDB 2.27.2 through loopback:

| Phase | Observed elapsed | Explicit execute calls |
|---|---|---|
| Temporary staging, before writer locks | 0.1048 s | 3, plus 2 streamed COPY operations |
| `_publish_policy_tx` within the transaction | 0.3596 s | 10 |
| Full `_apply_dml` publication transaction, including locks, receipt and commit | 0.4648 s | 29 (32 for the apply including staging) |
| Exact replay fact check | 0.0943 s | 9 |

All 20,077 lifecycle rows and one publication receipt were verified afterward.
The execute counts exclude the two COPY streams and the temporary staging
transaction's context-managed BEGIN/COMMIT.
The previous per-row algorithm would issue 41,362 execute calls for this shape
(`2 * 20,077 + 3 * 401 + 5`); that full old run was not executed. The regression
test measured 1,210 calls for the old one-fund/401-session fixture before the
fix. This is local evidence of bounded round trips and short lock duration,
not a production latency guarantee. Shared-database load and network latency
still affect the operator's total elapsed time.

Use a disposable `timescale/timescaledb:2.27.2-pg18` container bound to a
non-5432 loopback port. Create `nav_cross_repo`, `nav_readiness_w1_lifecycle`
and `nav_policy_test_lifecycle`, install `timescaledb` and `pgcrypto`, and create
`app_runtime` and `worker_writer` as `NOLOGIN`. These databases contain only
synthetic test fixtures. The focused regressions cover policy rollover,
lifecycle revocation, append preservation and historical-mutation rejection,
provider deferral, stale risk policy, and batched publication conflicts and
replay. Run the cross-repository and affected workers DB suites against the
exact pair of worktrees.

Validation for this release:

- Light: 73 repository/builder-capability unit cases and 52 cross-repository
  DB cases passed; Ruff and mypy passed (449 application source files).
- Workers readiness/access/lifecycle: 365 cases covered. The complete pass
  produced 361 passes and four failures. Three fixture cases used the invalid
  ingestion ID `stub`; they now use UUIDs. All four focused reruns passed,
  with the fourth maintenance assertion unchanged. That case also passed on
  the idle original database; concurrent advisory-lock interference is a
  possible cause of the initial result, not a confirmed application defect.
- Workers policy generation/artifact/operator: 116 DB cases passed on Linux
  with POSIX custody checks intact. After the final calendar-source race
  correction, the two affected publication/replay cases passed again.
- Workers rebase/cohort/provider: 53 DB cases passed. The existing chain and
  cohort unit checks also passed, including both provider stop codes.
- Six new batching DB cases passed. The 20 new lifecycle cases are included
  in the 365-case run; the extra upgrade preservation assertions passed in a
  focused rerun. Catalog regeneration reproduces the committed bytes exactly.

The policy-rollover, real provider append, stale-risk publication, provider
deferral, mixed-error and batching/race regressions were demonstrated failing
before their respective fixes. All tests used owned disposable local
PostgreSQL 18.4 / TimescaleDB 2.27.2 fixtures; no production state was mutated.
