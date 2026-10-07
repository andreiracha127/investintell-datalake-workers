# Workers Railway configuration

This package prepares the `investintell-workers` named IaC partial for the
`investintell-db` project's `production` environment. It owns only the Workers
services listed in `services.json`; Light, Customer Services, databases, volumes,
and other project resources remain outside this partial.

The committed baseline records a read-only production snapshot. `services.json`
captures the effective source, build, and deployment settings, including legacy
Config-as-Code overrides. Variable names become `preserve()` references; variable
values and credentials are absent. Networking stays omitted so existing domains
retain their settings under the pinned activation CLI.

Use Node 24.14.0 or newer. Install and verify from this directory:

```powershell
npm ci --ignore-scripts --no-audit --no-fund
npm run check
npm run evaluate
```

The local checks typecheck `railway.ts`, evaluate the actual pinned `railway`
3.13.0 SDK, compare every source/build/deploy field with `baseline.json`, verify
the service set and preserved variable names, and exercise the project/environment
guards. These commands make no Railway API calls, run no workers, and apply no
configuration. The TypeScript factory normally removes nulls; this authoring file
reattaches the exact captured settings so null cron, parked cron, empty arrays,
and Dockerfile paths survive evaluation.

Refresh the production snapshot with the accompanying read-only `capture.py` and
compare it using `compare.py` before cutover; a passing local check proves snapshot
equivalence, not that production has stayed unchanged. Review the
[equivalence report](equivalence.md) and
[shared cutover runbook](../docs/runbooks/railway-iac-cutover.md) supplied with this PR.

Legacy TOMLs remain unchanged for the preparation stage and rollback. The loader
TOML also remains part of its image/evidence hash. A Git merge or redeploy alone
does not activate `.railway/railway.ts`. Activation requires the separate reviewed
runbook, Railway CLI **5.63.4**, and explicit authorization for production changes.
There is no automatic IaC apply workflow in this package. The installed global
CLI is not upgraded by `npm ci`.

Official references: [Infrastructure as Code](https://docs.railway.com/infrastructure-as-code)
and the [IaC reference](https://docs.railway.com/infrastructure-as-code/reference).
