# Foreign listed-security evidence (W1c)

This loader supplies dated SEC evidence for foreign listed security types and
ordinary shares per ADS. It does not change W1 admission, share counts, or Light
sizing. The existing `foreign_issuer_listing_unverified` and
`depositary_ratio_unsourced` refusals remain in place. Wiring is a separate PR
after Workers #173.

## Sources and dates

The universe is every CIK appearing on a W1 foreign cover (20-F, 40-F, 6-K and
their amendments). Save the production read-only universe and observation
exports before discovery. Preserve historical symbols, including punctuation
aliases; do not replace them with a vendor's current ticker mapping.

The collector searches original SEC filings through sec-api.io, paginates the
results, and downloads the SEC documents into a task-owned directory outside
the repository. The access-layer API key comes from `SEC_API_IO_KEY`,
`SEC_API_KEY`, or the explicitly configured dotenv file. It is never written to
the evidence. Requests use
`InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)` and a shared
per-process rate limiter. Account for other processes before increasing the
default five requests per second; the SEC maximum is ten.

PDF exhibits use Poppler's `pdftotext` when available, with the declared `pypdf`
dependency as a fallback. Extracted evidence retains PDF page locations and the
hash of the original PDF bytes. Textless PDFs produce an explicit extraction
failure; they are not treated as evidence that no ratio exists.

Section 12(b) cover tables and their associated footnotes establish `ads`,
`ordinary_direct`, or `unknown`. A title for the underlying ordinary shares
does not establish a direct listing when the associated footnote says those
shares are not for trading. A legacy cover without a symbol can be linked using
a unique W1 equity line on that same accession, public by the cover's
availability date. Today's symbol alone is insufficient.

F-6, F-6EF and F-6 POS provide exact ADS ratios. Cover or Item 12.D ratio text
corroborates them. The collector also follows a 20-F's attached Section 12(b)
securities description, preserving the distinct `securities_description` source
kind. TSM's Exhibit 2a.1 supplies this corroboration: its securities table names
TSM ADS and five common shares per ADS. Such an exhibit supplies ratios only;
the primary filing cover remains the source of listed type. Dated 6-K
announcements establish changes. Numerator and
denominator are integers representing **ordinary shares per one ADS**: `1/2`
means one ADS represents half an ordinary share. Direct ordinary listings
return the identity ratio `1/1`.

An F-6 exhibit can inherit issuer identity from an actual issuer-name cover field
in the **same accession**, with the same issuer CIK and official filing date.
The exhibit retains the parent cover URL and original-byte hash. A depositary's
registrant CIK alone does not establish this attachment relationship. Shards use
the complete parent manifest for these lookups, including covers assigned to the
other shard; final offline replay uses the fully populated shared original cache.

Filing-search `filedAt` is not treated as the official filing date. Discovery
finishes by matching accessions to SEC quarterly master indexes across adjacent
quarters. Same-date cofilers agree; conflicting dates remain ambiguous. Exact
historical daily-index records, same-accession SEC submission headers, and the
read-only W1 export provide explicit fallbacks. API acceptance dates never fill
an unresolved filing date. The manifest preserves the original query date and
the authoritative date's source URL, hash and matching index/header evidence.

Normal source availability is the official filing date plus one day. A
backdated replacement document must also pass its publication floor: the maximum
discovery-reported publication date across that accession, without another day
added. Thus source availability is
`greatest(filed + 1, publication_floor_on)`. For example, AEM's March 22, 2024
acceptance and March 25 filing give March 26 availability. Vodafone's replacement
accession reported on November 9, 2022, with a legal filing date of June 8, 2018,
cannot become visible before November 9, 2022. Its legal effective date remains
June 9, 2018, so it cannot supersede a newer annual filing merely because it was
republished later. The floor is labelled as discovery-reported publication
evidence, not as an official filing date.

Version `sec-official-filing-date-v2` and matching per-document date/floor proofs
are required before parsing, combining shards or applying evidence. Initial
imports retain the resulting source availability. Corrections to an accession
already loaded become available no earlier than the reconciliation date, while
retired versions remain queryable at earlier dates. Explicit ratio effective
dates are separate: a known future change is not in force before its stated date.
The original document hash, accession, source URL, source text, parser version,
location, date authority and publication-floor evidence travel with every fact.

An unavailable or invalid HTML/text primary document can be recovered from its
SEC complete submission only by an exact accession and unique `FILENAME` match.
The loader retains original `TEXT` bytes, removing only the SGML separator line
break. It records the complete-submission URL and hash. If SEC has moved the
archival CIK, a unique accession mapping in a cached official quarterly index
can establish the retrieval location; the original discovery identity remains
unchanged, and the actual retrieval URL and index URL/hash/row are retained.
These recovery paths also work offline after their authoritative sources have
been cached. They do not invent missing content or substitute another accession.

## Local collection and validation

Use a new external cache directory. Example PowerShell commands:

```powershell
New-Item -ItemType Directory -Path E:/investintell-data/w1c -ErrorAction Stop | Out-Null
$env:PGOPTIONS = '-c default_transaction_read_only=on -c statement_timeout=30000'
$psql = 'C:/Program Files/PostgreSQL/18/bin/psql.exe'
& $psql -X -A -t -h 127.0.0.1 -p 65432 -U mcp_ro -d market -v ON_ERROR_STOP=1 `
  -f docs/runbooks/sql/sec-foreign-listing-universe.sql `
  -o E:/investintell-data/w1c/universe.json
& $psql -X -A -t -h 127.0.0.1 -p 65432 -U mcp_ro -d market -v ON_ERROR_STOP=1 `
  -f docs/runbooks/sql/sec-foreign-listing-observations.sql `
  -o E:/investintell-data/w1c/foreign_observations.json
python scripts/load_sec_foreign_listing_evidence.py `
  --universe E:/investintell-data/w1c/universe.json `
  --observations E:/investintell-data/w1c/foreign_observations.json `
  --cache-dir E:/investintell-data/w1c/cache `
  --discover --download `
  --output E:/investintell-data/w1c/evidence.jsonl
```

For a deterministic replay, replace `--discover --download` with `--offline`.
Search responses and downloaded files are verified against their stored hashes.
Downloaded originals use lossless XZ storage (older gzip/raw caches remain
readable); their evidence hashes are
computed over the original uncompressed bytes. The manifest records unsuccessful discovery, download, parsing and issuer
bindings separately. A complete manifest is required for database application.
The loader does not install the schema. Normal `--discover` includes authoritative
filing-date enrichment. A legacy manifest containing only API dates cannot be
parsed or applied by bypassing this stage.

For a large collection, complete discovery first, then use the two-process
wrapper below. Run the two `collect` commands in separate terminals. Each
process is fixed at four requests per second, so their combined limit is eight.
They share original documents, partition by URL, and cannot load their partial
manifests into a database. `combine` verifies exact coverage of the immutable
parent discovery, source identities, observation hashes, fact hashes and counts
before publishing the complete evidence artifact.

```powershell
python scripts/load_sec_foreign_listing_evidence.py `
  --universe E:/investintell-data/w1c/universe.json `
  --observations E:/investintell-data/w1c/foreign_observations.json `
  --cache-dir E:/investintell-data/w1c/cache --discover `
  --output E:/investintell-data/w1c/evidence.jsonl
python scripts/run_sec_foreign_listing_evidence_shards.py prepare `
  --cache-dir E:/investintell-data/w1c/cache
python scripts/run_sec_foreign_listing_evidence_shards.py collect `
  --cache-dir E:/investintell-data/w1c/cache --part 0 `
  --observations E:/investintell-data/w1c/foreign_observations.json
python scripts/run_sec_foreign_listing_evidence_shards.py collect `
  --cache-dir E:/investintell-data/w1c/cache --part 1 `
  --observations E:/investintell-data/w1c/foreign_observations.json
python scripts/run_sec_foreign_listing_evidence_shards.py combine `
  --cache-dir E:/investintell-data/w1c/cache `
  --output E:/investintell-data/w1c/evidence.jsonl
```

On Windows, preparation reports any shared-document directory links that need
to be created as junctions before collection. The wrapper has no database
operation; use the main loader's explicit `--apply` only after combination.
Once the full source cache exists, append `--offline` to both `collect` commands
to reparse with the current parser and complete same-accession binding context,
then run `combine` again. Offline mode cannot make SEC requests.

To upgrade a previously collected acceptance-date manifest, write a new parent
manifest and preserve the old manifest, evidence and shard plan. The following
commands use a new `verified` directory; select another new directory if that
name already holds a previous run:

```powershell
python scripts/enrich_sec_foreign_listing_filing_dates.py `
  --input-manifest E:/investintell-data/w1c/cache/manifest.json `
  --output-manifest E:/investintell-data/w1c/cache/verified/manifest.json `
  --cache-dir E:/investintell-data/w1c/cache `
  --observations E:/investintell-data/w1c/foreign_observations.json `
  --download-indexes --requests-per-second 2
New-Item -ItemType Junction `
  -Path E:/investintell-data/w1c/cache/verified/documents `
  -Target E:/investintell-data/w1c/cache/documents | Out-Null
python scripts/run_sec_foreign_listing_evidence_shards.py prepare `
  --cache-dir E:/investintell-data/w1c/cache/verified
```

Omit `--download-indexes` when the authoritative indexes and header sources are
already cached; enrichment then performs no network requests. Run both shard
collections with `--cache-dir E:/investintell-data/w1c/cache/verified --offline`
and combine that same cache. On systems supporting symbolic links, a directory
symlink to the shared originals can replace the Windows junction.

In a separate writer shell without the production read-only `PGOPTIONS`, apply
the additive schema to a disposable PostgreSQL 16 database, then set
`FOREIGN_EVIDENCE_DATABASE_URL` to that local database. Load the already combined
artifact with `--apply` alone; it verifies the artifact hash and date/floor
proofs, without another parse or any network operation:

```powershell
python scripts/load_sec_foreign_listing_evidence.py `
  --universe E:/investintell-data/w1c/universe.json `
  --cache-dir E:/investintell-data/w1c/cache `
  --output E:/investintell-data/w1c/evidence.jsonl --apply
```

Use the `verified` cache instead when the combined artifact came from the
upgraded parent. The observed-on date defaults to the actual UTC date.
Do not choose an earlier date to make a correction appear historically known.
The database operation is one transaction; unchanged facts remain unchanged,
and changed or removed facts create or retire versions.
For initial historical-coverage validation, use fresh disposable tables. Loading
a corrected date artifact over an earlier incorrect import would intentionally
retain that earlier history as part of the bitemporal correction record.

```powershell
$env:SEC_FOREIGN_TEST_DATABASE_URL = $env:FOREIGN_EVIDENCE_DATABASE_URL
python scripts/validate_sec_foreign_listing_evidence.py `
  --universe E:/investintell-data/w1c/universe.json `
  --observations E:/investintell-data/w1c/foreign_observations.json `
  --output-dir E:/investintell-data/w1c/validation
```

The validator reads only. It reports 2010, 2015, 2020 and 2025 year-end coverage
against the fixed foreign universe, separately identifies lines W1 already
evidenced at each date, and generates a reproducible sample of thirty resolved
2025 lines for manual review, split between ADS and direct ordinary listings
where available. Pass `--current-statuses` with the saved production W1 results
to measure overlap with today's refusals. A generated review packet is not itself a
precision result: a reviewer must compare each result with the source document.

The function returns exactly one row:

```sql
SELECT * FROM public.sec_foreign_listing_at(1046179, 'TSM', DATE '2025-12-31');
```

`status` is `resolved`, `ambiguous`, or `none`. `listing_status` and
`ratio_status` explain partial coverage, and `evidence_ids` identify supporting
facts. Conflicting observations on the same effective date remain ambiguous.
Different current source streams cannot be resolved by choosing a preferred
provider. An issuer-only registration can support a symbol only when the dated
cover evidence identifies its sole ADS program; it is not copied across all
of today's symbols.

## Exact production procedure (not executed by this PR)

1. Review and merge this evidence PR. Keep admission/sizing integration in its
   separately reviewed PR after Workers #173. No scheduled worker is installed
   by this change.
2. Outside 06:00–08:30 UTC, take fresh read-only W1 exports with `mcp_ro`, host
   `127.0.0.1`, port `65432`, database `market`, and
   `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=30000'`.
   Keep the query text, extraction timestamp and JSON hashes with the run.
3. Collect/replay the full universe locally with the commands above. Review the
   manifest, year-end coverage, conflicts and manual sample before loading.
4. In a separate shell with an explicitly authorized production writer and
   without the read-only `PGOPTIONS`, apply only the new schema:
   `psql -X -v ON_ERROR_STOP=1 -f schemas/sec_foreign_listing_evidence.sql`.
   Supply the production connection through the operator's normal credential
   mechanism. Do not use the read-only `mcp_ro` role for this step.
5. Set `FOREIGN_EVIDENCE_DATABASE_URL` securely to that authorized writer.
   Run the apply-only command above with the reviewed universe, complete verified
   cache and exact combined JSONL artifact. Its manifest must have v2 filing-date
   and publication-floor proofs. Use the actual reconciliation date; do not run
   discovery or reparse during application.
6. With `mcp_ro`, read back source/fact counts and execute the dated resolver
   checks and year-end report. Confirm existing W1 refusal behavior is unchanged.
   No Railway change, deployment, or admission switch is part of these steps.

If an authorized operator needs to remove this evidence-only installation,
apply `schemas/sec_foreign_listing_evidence.rollback.sql` with
`psql -X -v ON_ERROR_STOP=1`. It removes only the new function and two tables;
it does not modify any W1 table or function. Retain the immutable external
source artifacts and validation report before removing database history.
