# Bond pilot Option A internal runbook

This is an internal, manual, offline-oriented pilot procedure. It creates no
worker registration, schedule, API, public DTO, frontend page, HTML, JavaScript
bundle, deployment, database write, DDL, DML, temporary database object, or
production cutover. All output directories must be new and outside the Git
checkout.

Phase 4 is currently **IN PROGRESS / DB DML NOT STARTED**. Consequently, the
only executable development lane is qualification and a synthetic fixture run;
calibration remains gated before any connection until its evidence and every
human approval are valid. A real database DML operation or real calibration run
has not started.

## Confidentiality boundary

Full provenance is mandatory in internal manifests, restricted reports, local
checksums, and controlled operational evidence. It includes the exact locator,
archive and member identity, hashes, schema and cutoff, terms evidence,
approvals, row lineage, mapping versions and hashes, and provider-specific
failures.

Absolutely no identity of a data source, provider, vendor, dataset, upstream
project, pipeline, source family, URL, path, filename, repository, source
version, row identifier, hash, lineage, license, entitlement, or provider error
may cross into a frontend, API/public DTO, public error/client log, page props,
HTML, or JavaScript bundle. `144A` may appear publicly only as a security
attribute. Option A creates no public DTO, API, frontend, page props, HTML, or
bundle.

Any later public serializer must use an explicit allowlist containing only
neutral values and units, observation/as-of dates, freshness,
quality/availability, a neutral methodology version, and `is_144a`. It must not
remove forbidden fields after serialization; it must construct the payload from
that allowlist. Internal failures must become neutral typed public states without
their source-specific text. No frontend-facing example in this runbook names a
source.

The internal pack parent and storage must be restricted to the operational
identity that runs and reviews the pilot. On POSIX, each reporting attempt is
explicitly hardened and verified as mode `0700` before any unredacted evidence
is written. On Windows, mode bits do not establish an ACL: the operator must use
a parent directory and storage restricted to that identity. Random attempt
names and no-replace publication remain mandatory on both platforms, but do not
substitute for the storage access controls.

## Approval order and manual commands

Run these commands from the workers repository root in PowerShell. Values marked
`<USER_SUPPLIED_...>` are supplied by the approving operator and are never
committed. The generated output names below are local-only paths.

```powershell
$RunRoot = '<USER_SUPPLIED_OUTPUT_DIRECTORY_OUTSIDE_CHECKOUT>'
$SourceArchive = '<USER_SUPPLIED_LOCAL_ARCHIVE_PATH>'
$ExpectedSha256 = '<USER_SUPPLIED_ARCHIVE_SHA256>'
$QualificationRun = Join-Path $RunRoot 'qualification'

python scripts/run_bond_pilot.py qualify `
  --source $SourceArchive `
  --expected-sha256 $ExpectedSha256 `
  --run-dir $QualificationRun
```

Qualification creates an unapproved `source-manifest.json`; it does not create
an approval. A human first verifies local-use and redistribution terms, exact
artifact and schema pins, cutoff, and duplicate handling. The human then creates
and stores the separate source-approval JSON in controlled internal storage.
Only the exact candidate manifest it binds may proceed.

```powershell
$SourceManifest = '<USER_SUPPLIED_QUALIFIED_SOURCE_MANIFEST_PATH>'
$SourceApproval = '<USER_SUPPLIED_SOURCE_APPROVAL_PATH>'
$Fixture = '<USER_SUPPLIED_SYNTHETIC_FIXTURE_PATH>'
$FixtureMapping = '<USER_SUPPLIED_SYNTHETIC_MAPPING_PATH>'
$FixtureRun = Join-Path $RunRoot 'fixture-run'

python scripts/run_bond_pilot.py fixture-run `
  --source-manifest $SourceManifest `
  --source-approval $SourceApproval `
  --fixture $Fixture `
  --mapping $FixtureMapping `
  --run-dir $FixtureRun
```

The fixture result is internal and must state `calibration: "not_started"`,
`phase4: "pre_backfill"`, and `representative_post_backfill: false`. It performs
no database reads or writes. It cannot establish representative coverage or
authorize a real run.

Only after the source approval, Phase 4 reconciliation, Phase 4B/V2 publication
evidence, the separate evidence approval, observed-values debt mapping, mapping
approval, and bounded read authorization are all approved may an operator attempt
calibration. The evidence approval must bind the exact evidence hash and the
approver identity must be supplied through the two environment variables below.

```powershell
$ApprovedMapping = '<USER_SUPPLIED_APPROVED_MAPPING_PATH>'
$MappingApproval = '<USER_SUPPLIED_MAPPING_APPROVAL_PATH>'
$Phase4Evidence = '<USER_SUPPLIED_PHASE4B_V2_EVIDENCE_PATH>'
$Phase4Approval = '<USER_SUPPLIED_PHASE4B_V2_EVIDENCE_APPROVAL_PATH>'
$SeriesId = '<USER_SUPPLIED_APPROVED_SERIES_ID>'
$CalibrationRun = Join-Path $RunRoot 'calibration'
$env:BOND_PILOT_PHASE4_V2_APPROVAL_SHA256 = '<USER_SUPPLIED_EVIDENCE_APPROVAL_SHA256>'
$env:BOND_PILOT_PHASE4_V2_APPROVER_ID = '<USER_SUPPLIED_EVIDENCE_APPROVER_ID>'

python scripts/run_bond_pilot.py calibrate `
  --source-manifest $SourceManifest `
  --source-approval $SourceApproval `
  --mapping $ApprovedMapping `
  --mapping-approval $MappingApproval `
  --phase4-evidence $Phase4Evidence `
  --phase4-approval $Phase4Approval `
  --mode calibration `
  --series $SeriesId `
  --run-dir $CalibrationRun
```

The only modes are `calibration` and `first_bounded`; no relation argument is
accepted. `calibration` is limited to 1,000 rows per page, five pages, 5,000
rows, ten minutes, a 20-second statement timeout, concurrency one, and zero
automatic retries. `first_bounded` is limited to 2,500 rows per page, twenty
pages, 50,000 rows, thirty minutes, and a 30-second statement timeout; it keeps
the same concurrency and retry limits. Both use a read-only transaction, a
two-second lock timeout, a 60-second idle transaction timeout, static bound SQL,
and `EXPLAIN (FORMAT JSON)` without `ANALYZE`. Unsafe/global/sequential plans,
missing lineage predicates, absent indexed or partitioned access, or any write
attempt stop the run. Full execution is deferred pending separate authorization
and a new plan. The current CLI rejects `--mode full` through argparse choices
before `PilotError` handling; it reports an argument error and does not create a
typed stop pack.

Reaching a normal configured row or page cap publishes a successful partial
calibration pack: `partial` is `true`, the checkpoint has
`output_state: "budget_reached"`, and `calibration-report.json` is present.
That outcome does not raise and contains no `stop-report.json`.

To resume only a validated stopped calibration pack, use the same governed
inputs, mode, ordered series, and cumulative budget. The resume pack is checked
for exact file set, regular files, checksums, checkpoint pins, and matching
provenance; it never resets counters or widens a limit.

```powershell
$ResumePack = '<USER_SUPPLIED_VALIDATED_STOPPED_CALIBRATION_PACK_PATH>'
$ResumedCalibrationRun = Join-Path $RunRoot 'calibration-resume'

python scripts/run_bond_pilot.py calibrate `
  --source-manifest $SourceManifest `
  --source-approval $SourceApproval `
  --mapping $ApprovedMapping `
  --mapping-approval $MappingApproval `
  --phase4-evidence $Phase4Evidence `
  --phase4-approval $Phase4Approval `
  --mode calibration `
  --series $SeriesId `
  --run-dir $ResumedCalibrationRun `
  --resume-pack $ResumePack
```

## JSON contracts

`source-manifest.json` has schema `source-candidate-v1` and records exactly the
candidate locator, local archive and extracted paths, artifact byte count and
SHA-256, member identity and uncompressed byte count, schema SHA-256 and column
lists, row and row-group counts, global start and cutoff, duplicate-check scope,
and unapproved state. The separate source approval has schema
`source-approval-v1`; it pins the same locator, artifact SHA-256, schema
SHA-256, and cutoff, and records terms evidence, local-use and redistribution
decisions, approver, and UTC approval time. Its exact pins must match the
candidate.

The synthetic fixture mapping has schema `debt-mapping-test-v2` and is only for
the fixture lane. A real mapping has schema `debt-mapping-v2`, a mapping version,
an observed-composite-values SHA-256, and an ordered table of exact rules keyed
by `issuer_category`, `asset_class`, and `instrument_structure`. A decision is
only `eligible_debt` or `non_debt_excluded`; absent, null, or empty components
are `missing_category`, while a complete unmatched tuple is
`ambiguous_category`. There is no normalization, inference, legacy mapping, or
fallback. Its separate `debt-mapping-approval-v2` binds the exact mapping bytes
and observed-composite-values SHA-256 to internal evidence, approver, and UTC
approval time.

The real-read evidence has schema `phase4b-v2-evidence-v2`; it must declare a
completed, reconciled, published V2 state, the allowlisted seam and relation,
the required columns (including the three composite fields), five governance
hashes, the composite mapping contract/version/artifact hash/approval reference,
approved series, approver, and UTC approval time. Its separate
`phase4b-v2-evidence-approval-v2` binds the evidence SHA-256 and every one of
those governance and composite-mapping pins. Phase 4 remains blocked until a
real composite mapping and its new hash-bound approval exist; current status
alone does not satisfy this contract.

Phase 4 evidence/approval and resume checkpoint/control inputs use the strict,
bounded parsing implemented for those inputs: duplicate keys and non-finite
values are rejected. Source manifest and source approval inputs instead load
through their exact domain contracts and pin validation; this runbook makes no
universal parser claim for every JSON control input. The CLI prints sorted JSON
on success. A typed stop exits with code 2 and writes an internal stop report
and checksum manifest when it can safely publish them.

## Internal artifacts, checksums, and typed stops

Confidentiality is fail-closed: no source identity, provider, dataset,
upstream, locator, path, row identifier, hash, lineage, license, entitlement,
or provider-error detail may cross into frontend, API, public DTOs, public
errors, client logs, page props, HTML, or JavaScript. `144A` is only a security
attribute. Internal artifacts may retain the governed provenance needed for
review; public diagnostics never serialize raw mapping rules or canonical
classification values.

Fixture packs retain `source-manifest.json`, `nport-extract-manifest.json`,
`calibration-report.json`, `bond-observed-daily.parquet`,
`bond-latest.parquet`, `fund-asof-match.parquet`,
`fund-series-metrics.parquet`, `quality-summary.json`, `pilot-report.md`, and
`checksums.sha256`. A normal capped partial database pack contains its
checkpoint, internal calibration provenance, and `calibration-report.json`, but
no `stop-report.json`. A typed stopped pack contains internal provenance,
`stop-report.json`, and checksums where safely publishable; it retains a
checkpoint only when one was created before the stop. `bond-latest.parquet` is
an internal diagnostic only and never supplies historical matching.

`checksums.sha256` verifies each artifact before review or resume. Checkpoints
retain the run identity; evidence, approval, publication, query, and method
pins; mode and series; resolved reports and stable key; cumulative pages, rows,
and elapsed time; output hash and state; and a typed stop reason. The original
full provenance remains inside these internal artifacts.

Typed stops are fail-closed: a qualification, hash, schema, terms, approval,
mapping, evidence, lineage, relation, unsafe plan, read-only violation,
checkpoint, or resume mismatch stops without fallback;
`phase4b_v2_unavailable` means the real-read prerequisites or approval authority
are absent or invalid; `calibration_connection_failed` means no safe connection
was opened; and `calibration_resume_invalid` means the prior pack cannot be
trusted. An unsafe plan, timeout, or write attempt raises a typed stop and
publishes a stopped pack where safely possible. A normal configured budget cap
is instead the successful partial outcome described above. There is no retry,
counter reset, timeout increase, broadening, legacy fallback, or silent
continuation.

## Deferred scope

The following remain deferred: full or unbounded runs; Phase 4B/V2 creation,
backfill, indexing, reconciliation, or evidence production; approval of a real
mapping; long-term rights/cadence decisions; registered workers and schedules;
API routes, public DTOs/serializers/caches, frontend pages, HTML, JavaScript
bundles, and the standalone Bonds surface; public implementation of the future
allowlist; deployment, telemetry, database writes, FX conversion, and claims of
representative real-fund coverage. No part of this runbook authorizes any of
those actions.
