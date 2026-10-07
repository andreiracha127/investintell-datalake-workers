# Railway IaC cutover for the shared project

The complete two-repository runbook is maintained in Light:
[Railway IaC cutover](https://github.com/andreiracha127/investintell-light/blob/6f9d4da29ad21f6d4fa845191d7609d475d6d409/docs/runbooks/railway-iac-cutover.md).

This Workers PR prepares the `investintell-workers` partial with 44 services.
Its [equivalence report](../../.railway/equivalence.md) records the exact reviewed
settings. The linked runbook covers both partials, the job-idle gate before merges,
materializing effective settings, explicit/implicit legacy removal, verification,
and rollback before and after 2026-12-01.

No activation is performed by this preparation. Legacy TOMLs remain unchanged;
merging this PR does not apply `.railway/railway.ts`.
