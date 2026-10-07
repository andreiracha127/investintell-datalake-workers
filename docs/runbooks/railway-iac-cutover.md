# Railway IaC cutover for the shared project

The complete two-repository runbook is maintained in Light:
[Railway IaC cutover](https://github.com/andreiracha127/investintell-light/blob/daf23c8c4e2250bc2e07a54e8b369c0d453d6706/docs/runbooks/railway-iac-cutover.md).

This Workers PR prepares the `investintell-workers` partial with 44 services.
Its [equivalence report](../../.railway/equivalence.md) records the exact reviewed
settings. The linked runbook covers both partials, the job-idle gate before merges,
materializing effective settings, explicit/implicit legacy removal, verification,
and rollback before and after 2026-12-01.

No activation is performed by this preparation. Legacy TOMLs remain unchanged;
merging this PR does not apply `.railway/railway.ts`.

Use Railway CLI **5.63.4**, SDK **3.13.0**, and Node **24.14.0 or newer**. The
Workers [package checks](../../.railway/README.md) cover TypeScript, SDK
equivalence, empty-context evaluation, and regression tests. The service inputs
and equivalence report are unchanged by the review fixes.

Before each separately authorized operational command, run from this repository
root with the explicit pinned executable path from the shared runbook:

```powershell
node .railway/check-target.mjs $railwayCli
if ($LASTEXITCODE -ne 0) { throw "Railway target preflight failed" }
```

The preflight checks the local linked project/environment IDs and scoped live
status. It runs only version/status reads. The callback accepts an empty context;
it does not validate the operational target or grant permission to apply.

Fresh capture reconstructs connected source from the live repository/image and
deployment triggers, including branch and wait-for-CI. Compare rejects service
identity replacements and unexpected addresses owned by this partial anywhere
in `iacPartials`. Execution capture uses fresh validated service nodes and
follows all cursors, aborting on incomplete evidence. Inspect execution/instance
states separately to establish the shared runbook's job-idle gate.

Do not run `railway config plan` in this read-only review workflow: the pinned
CLI preview calls the mutation-shaped `environmentPreviewChangeSet` operation.
