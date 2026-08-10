# Runbook — Rule 144A to Regulation S distribution-series backfill

## Purpose and non-negotiable identity rule

`bond_curated_universe` is the approved identification universe, but its current
CUSIP9 is the Rule 144A **reference** leg. The panel for non-US investors must
execute on the paired Regulation S leg. A pair is admissible only when an SEC
filing or exhibit explicitly labels both sides in the same issue block.

Never infer a pair from `db_type`, identifier shape or prefix, a US ISIN, matching
issuer/coupon/maturity, or the absence of a Rule 144A label. A Regulation S CINS
is stored as `cusip9`; a pair with only an ISIN or Common Code on the Regulation S
side is retained in the registry but is not executable by the CUSIP-keyed panel.

This workflow is additive and non-activating. The script downloads immutable
evidence and emits a dry-run registry preview. It cannot write the database,
approve a mapping snapshot, move the panel pointer, deploy, or restart Stage 6.

## Source

Discovery uses the SEC-API Full-Text Search API over EDGAR filings and exhibits.
The API result is document metadata, not the mapping itself. Download and parse
the referenced exhibit before adjudication. High-yield document families include
`EX-4.x` exhibits attached to 8-K, 6-K, S-4/F-4, and 20-F filings.

Set credentials outside the command line when possible:

```powershell
$env:SEC_API_IO_KEY = '<secret>'
$env:SEC_USER_AGENT = 'InvestIntell Operations <operations@example.com>'
```

Do not commit either value or any raw artifact that is outside the repository's
artifact policy.

## 1. Prepare the reference list

Export one normalized CUSIP9 per line from the approved reference universe. The
file is an input to a bounded search; omission from search results never means
that a mapping does not exist.

```powershell
$out = 'artifacts/bond-distribution-series'
$references = Join-Path $out 'reference-cusips.txt'
```

Record the SQL/export command, row count, source timestamp, and SHA-256 of this
file in the execution evidence.

## 2. Bounded discovery

Use broad discovery to seed likely exhibits. `--limit` is the maximum number of
API requests in one invocation; repeat the identical command to resume from its
checkpoint.

```powershell
uv run python scripts/backfill_bond_distribution_series.py `
  --output-root $out discover `
  --start-date 2000-01-01 --end-date 2026-08-09 --limit 100
```

Then run the completeness lane over the exact reference universe. It searches
both compact and 6-2-1 formatted CUSIP variants and is independently bounded and
resumable.

```powershell
uv run python scripts/backfill_bond_distribution_series.py `
  --output-root $out targeted-discover `
  --reference-cusips-file $references `
  --start-date 2000-01-01 --end-date 2026-08-09 --limit 250
```

A checkpoint is valid only for the same normalized reference list, date window,
and query/parser version. A changed scope must be refused rather than silently
reusing indexes from the prior campaign.

## 3. Download and parse immutable evidence

```powershell
uv run python scripts/backfill_bond_distribution_series.py --output-root $out download
uv run python scripts/backfill_bond_distribution_series.py --output-root $out parse
uv run python scripts/backfill_bond_distribution_series.py --output-root $out adjudication-export
```

For each downloaded document retain accession, document URL/type, retrieval time,
raw SHA-256, parser version, exact source label/value, and a stable table/section
locator. A parser candidate is not approval.

`explicit-label-v2` bounds the durable parse artifact without weakening evidence:
every `candidate` and `ambiguous` block is retained. Blocks with no explicit
label are not repeated by table row; a document with no actionable block retains
one deterministic `zero_match`, while zero blocks beside actionable evidence are
collapsed. The parse result reports both zero-only documents and the number of
collapsed zero blocks. This version change requires regenerating parse and
adjudication artifacts; do not mix a v1 manifest with v2 records.
Adjudication manifest v2 binds both version fields into its checksum. Export,
seal, dry-run bundle construction, and publish all reject a stale manifest or
any record whose parser version differs from the current parser.

## 4. Human adjudication

Review `adjudication/manifest.json` against the raw document. An approval must
select one same-issue Rule 144A reference and its explicitly labelled Regulation S
identifiers. Multi-issue tables, conflicting identifiers, cross-block matches,
and uncertain labels remain ambiguous or rejected. Do not approve a record only
because its terms appear economically similar.

Each approved record must name the new `draft_snapshot_id` that will own the
complete prospective composition. It must not name an already approved snapshot,
because approved compositions are closed. After editing the decisions, seal the
unchanged records array and its human decisions:

```powershell
uv run python scripts/backfill_bond_distribution_series.py `
  --output-root $out seal-adjudication
```

Sealing validates the adjudication vocabulary and snapshot IDs, then replaces
only the manifest checksum artifacts. It does not rerun parsing, alter evidence,
approve the database snapshot, or write the database. Never alter raw evidence
to make the checksum pass.

## 5. Dry-run registry preview

```powershell
uv run python scripts/backfill_bond_distribution_series.py `
  --output-root $out publish --dry-run
```

Expected healthy signal: exit 0, `database_writes=0`, one prospective draft
snapshot row, its computed composition hash, a matching prospective approval row,
and decision/identifier rows only for explicitly approved records. The approval
row is an ordered future action, not evidence that approval or any database write
occurred. Pending, rejected, ambiguous, invalid-label, and
Regulation-S-without-CUSIP records must not become executable mappings.

## 6. Separately authorized database load and panel rebuild

Database publication is a later, separately authorized operation:

The collector remains permanently zero-write. The productive path is the
registered one-off worker `bond_distribution_registry_backfill`, and the
additive registry schema must already be installed before that worker starts.
Deploy is execution on Railway, so every invocation must be pinned to one exact
merged revision and must set all of the following variables:

```text
WORKER=bond_distribution_registry_backfill
BOND_DISTRIBUTION_OUTPUT_ROOT=<sealed artifact path>
BOND_DISTRIBUTION_SNAPSHOT_ID=<draft snapshot id>
BOND_DISTRIBUTION_LOAD_MODE=draft|approve
CODE_REVISION=<exact merged revision>
BOND_DISTRIBUTION_LOAD_AUTHORIZATION=<same exact revision>
```

`WORKER_LIMIT` and `WORKER_CALC_DATE` must be absent. The worker rejects inherited
scope controls, a missing schema, an authorization/revision mismatch, a bundle
whose snapshot or content hash differs from the sealed artifact, and lock
contention. The `draft` run loads immutable evidence, observations, decisions,
identifiers, and the unapproved snapshot through the public registry loader.
Read the runtime JSON and reconcile every row count and content hash directly
from PostgreSQL before proceeding.

The `approve` run first replays that complete bundle inside the same outer
transaction. Every replay insert count must be zero, proving that production
already contains the exact immutable composition; only then may the worker add
the approval row. A mismatch, a missing row, or an attempted repair aborts and
rolls back the approval. After database read-back, restore
`WORKER=bond_live_daily`, remove all distribution-loader variables, and verify
the one-off deployment is terminal before any panel activation. Neither mode
updates the panel pointer.

1. install the additive registry schema;
2. load evidence and parser observations;
3. load the complete draft snapshot and decisions atomically;
4. verify content hash, collision checks, and coverage;
5. approve/finalize the immutable snapshot atomically;
6. measure Regulation S price, rating, and liquidity coverage;
7. rebuild a new historical Regulation S base under the new config identity;
8. validate actual row counts, complete month partitions, lineage,
   `max(computed_at)`, and the pointer candidate without changing the pointer;
9. bind immutable `gate_evidence.config_transition` to contract
   `rule_144a_to_reg_s_base_v1`, the current Rule 144A publication, both config
   hashes, and the authorized base code revision;
10. promote the parentless base through the guarded cross-config transition;
    an ordinary materializer base is not allowed to overwrite the pointer;
11. activate the first Stage 6 delta only under a separate production
    authorization.

The frozen Rule 144A base and its T3 parity result cannot be reused as the
Regulation S base or as a like-for-like activation gate.
