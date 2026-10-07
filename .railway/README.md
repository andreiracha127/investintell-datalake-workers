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
npm test
python -m unittest discover -s . -p "test_*.py" -v
```

The local checks typecheck `railway.ts`, evaluate the actual pinned `railway`
3.13.0 SDK, compare every source/build/deploy field with `baseline.json`, verify
the service set and preserved variable names, and invoke the callback with a
literal empty context. A populated SDK context produces the same graph. Target
validation runs separately before operational commands. These local checks make
no Railway API calls, run no workers, and apply no
configuration. The TypeScript factory normally removes nulls; this authoring file
reattaches the exact captured settings so null cron, parked cron, empty arrays,
and Dockerfile paths survive evaluation.

Refresh the production snapshot with the accompanying read-only `capture.py` and
compare it using `compare.py` before cutover; a passing local check proves snapshot
equivalence, not that production has stayed unchanged. Capture reconstructs
connected repository/image source from the live service source and GitHub trigger
branch/wait-for-CI settings, retaining other allowlisted source properties. Incomplete
or ambiguous trigger evidence fails loudly. Comparison checks service IDs as well
as names and rejects deleted/recreated services. It checks the complete
`iacPartials` map for unexpected addresses owned by `investintell-workers`,
including services outside this inventory, volumes, and buckets. With
`--executions`, capture validates the fresh service IDs before querying jobs,
then follows every execution cursor to the final page. Incomplete or nonadvancing
pagination fails without writing a partial snapshot. The unit tests cover these
failure paths and the read-only target check. Review the
[equivalence report](equivalence.md) and
[shared cutover runbook](../docs/runbooks/railway-iac-cutover.md) supplied with this PR.

Legacy TOMLs remain unchanged for the preparation stage and rollback. The loader
TOML also remains part of its image/evidence hash. A Git merge or redeploy alone
does not activate `.railway/railway.ts`. Activation requires the separate reviewed
runbook, Railway CLI **5.63.4**, and explicit authorization for production changes.
There is no automatic IaC apply workflow in this package. The installed global
CLI is not upgraded by `npm ci`.

From the repository root, verify the local link and live target immediately
before each separately authorized operational command:

```powershell
node .railway/check-target.mjs $railwayCli
if ($LASTEXITCODE -ne 0) { throw "Railway target preflight failed" }
```

`$railwayCli` must be the explicit path to CLI 5.63.4 from the runbook. The check
verifies the nearest ancestor link's project/environment IDs against the baseline,
then reads `status --environment <linked-ID> --json` to verify live IDs and names.
It rejects token/project/environment targeting overrides and nonproduction
`RAILWAY_ENV`; use the same local link and shell for the following operation.
Plain `status --json` lists all environments and does not identify the selected
one. The preflight issues only a version check and a status read; it does not run
a plan or apply. A passing preflight does not authorize an operational change.

Official references: [Infrastructure as Code](https://docs.railway.com/infrastructure-as-code)
and the [IaC reference](https://docs.railway.com/infrastructure-as-code/reference).
