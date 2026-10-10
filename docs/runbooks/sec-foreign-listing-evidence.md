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

Annual discovery includes 20-F/40-F and their supported registration forms,
including 40FR12B and 40FR12B/A. The 6-K ratio search includes ADS, ADR, GDR and
GDS, depositary/depository shares and receipts, and American shares evidenced by
receipts. Full-text totals count documents, including exhibits: completeness
requires unique accession, canonical URL and document-type hits to exactly meet
the declared total. Repeated hits/pages fail discovery. Distinct exhibits may
share an accession; legacy SGML documents may also share a URL.

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

An explicit Item 9 ADS listing declaration for a symbol already bound by the
cover, with its exchange identified, is retained separately as type-only
`listing_description` evidence. It cannot override a conflicting cover row or
supply a ratio. OTLY's 2024 cover names ordinary shares while its Item 9 names
Nasdaq-listed ADSs under the same symbol; the two co-effective assertions return
`ambiguous`. This preserves the source conflict rather than treating the cover's
underlying-class title as an unqualified direct-listing answer.
A table of contents, risk discussion or unrelated historical ADS mention is
insufficient for this declaration.

F-6, F-6EF and F-6 POS provide exact ADS ratios. Cover or Item 12.D ratio text
corroborates them. The collector also follows a 20-F's attached Section 12(b)
securities description, preserving the distinct `securities_description` source
kind. A verified 20FR12G or 20FR12G/A securities-description attachment uses the
same ratio-only path. TSM's Exhibit 2a.1 supplies this corroboration: its
securities table names TSM ADS and five common shares per ADS. Such an exhibit supplies ratios only;
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
imports retain the resulting source availability. Reconciliation preserves fact
versions and records `retired_reason` (schema v2), using the project's
restatement rule, as in W1 and W1b:

- `source` (or NULL on rows retired before v2): **the public record changed**.
  Different source bytes or a genuinely new document on an accession already
  loaded become known no earlier than reconciliation. The old version stays
  visible before its retirement.
- `parser_correction`: **our reading changed, not the public record**. The same
  document bytes (the same `source_sha256`) read by another parser version.
  The old reading was never true and is visible at no date. The new reading is
  known when the reading it replaces was: from `source_available_on` for a
  document first loaded with its accession, from the republication date for
  republished content. A reading that replaces none is known from the filing's
  public date, including its governed publication floor. A parser addition on
  already loaded bytes therefore restates history; a genuinely new source
  document retains the first-loaded protection.

Facts and sources already record `parser_version`; `loaded_on`, `retired_on`,
and source load dates retain the reconciliation audit. Every resolver branch
excludes parser-corrected old readings. A row is visible at D iff
`available_on <= D`, its reason is not `parser_correction`, and `retired_on` is
NULL or after D. The loader requires the v2 column, CHECK and exact resolver;
it refuses both a v1 installation and a v2 rollback.

The governed schema is applied in order: `schemas/sec_foreign_listing_evidence.sql`
(the unchanged production v1), then `schemas/sec_foreign_listing_evidence_v2.sql`.
Explicit ratio effective
dates are separate: a known future change is not in force before its stated date.
`effective_date_explicit` distinguishes a date stated in the document from the
filing-plus-one fallback. An F-6 using that fallback is deferred when a 6-K
already public by the F-6's publication announces the same program, underlying
class and exact ratio on one unique future date. Deferral happens before the
latest-filing selection, so the previous ratio remains available until the
announced change. It does not change stored publication dates, override an
explicit F-6 effective date, or choose between conflicting future dates. For
ANPC, the October 18, 2022 announcement places the October 24 F-6's new 1:20
ratio into force on November 4, rather than on its filing-plus-one fallback.

An F-6 that expressly says its amended ratio awaits a Depositary-announced
date is recorded as pending, with the literal condition retained. Its filing
date does not make that ratio operative. A publicly available, same-program and
class announcement must establish the exact ratio and effective date before
the pending registration can enter selection. This also governs matching
fee-table or counsel facts from the same registration. A later announcement
changes knowledge prospectively; a later annual history cannot backfill the
announcement into an earlier query date.

A 6-K ratio announcement that remains subject to approval or another stated
condition is pending evidence. It cannot activate a registration on its own.
Once its proposed date and public availability have arrived, the unresolved
condition makes the ratio `ambiguous`, including when older sources agree on a
different ratio. A later public completion, definitive Depositary notice or
actual approval result must establish the same program, underlying class and
event. The proof must address the outstanding conditions; shareholder approval
does not establish a separate regulatory approval. Unknown conditions require
affirmative operative evidence. ANTE's conditional consolidation notices are
settled by its later public shareholder result, with the new ratio still
deferred until the established December 9 date.

Contradictory operative dates are retained with their literal proof and all
candidate dates. They never become an empty parse or a filing-plus-one start.
The uncertainty controls the ratio from the later of the earliest stated date
and the source's availability. It cannot disappear through latest-filing
selection or an unflagged fee statement in the same registration. A later
authoritative single-date source can settle the clock prospectively; the
original conflict metadata remains immutable and the resolver includes the
confirming source. A confirmed future date defers the new number until that
date, and a retrospective confirmation cannot change answers before it was
public. Independent ratio corroboration is still required.

An explicit correcting-and-replacing 6-K preserves its literal correction
headline and exact source old CUSIP. Once public, it replaces an older claim
only for that same issuer, event date, program, symbol and underlying class,
with a later legal filing date. Ordinary amendments, different programs and
independent contradictions remain separate. Earlier queries retain the
previously public claim. ANTE's contemporaneous 2022 announcements link its
10/1-to-1/1 change to the December 9 share consolidation; December 12 is a
distinct trading-price date. XIN's correcting announcement replaces 16/1 with
20/1 for the same November 28, 2022 event.

The parser retains the new side of an explicitly ordered ADS transition. A
former ratio, or a pre-change "currently" statement, cannot inherit the new
event's effective date. Distinct earlier and later events remain separate,
including when one report describes both. A concurrent ordinary-share
consolidation requires the source's explicit ADS entitlement and a linked
implementation date; the parser does not calculate an ADS ratio from the
ordinary-share split. NNDM's contemporaneous sources establish its 50/1-to-1/1
change on June 29, 2020.

An F-6's explicit "prior to" and "commencing" clauses retain separate operative
intervals. The prior interval ends exclusively at the cutover, and the new
entitlement starts on the stated date. An expired prior interval stays expired;
conflicting commencement dates cannot become a filing-date fallback. DQ's
agreement specifies 25/1 before November 17, 2020 and 5/1 from that date, with
the completed change corroborated by its November 23 6-K. Expanded contractual
"beneficial ownership interests" wording requires an ordinary/common-share
definition; preferred securities and units do not supply an ordinary ratio.

Every F-6 entitlement is bound to its primary deposited-security title or
contractual share definition. Preferred programmes and CPO baskets cannot
supply an ordinary-share conversion through a nested component clause. Annual
ratios also respect the source's exact named-class definitions: "Class B" does
not itself establish common or preferred status. A receipt containing both
common and preferred shares cannot be reduced to its common-share component.
Separate common and preferred programmes retain separate source locations,
including when they have equal numeric ratios. SQL excludes nonordinary ratio
facts before binding, pending-contract activation and latest selection.

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
before publishing the complete evidence artifact. Every child input document
must match all metadata in the immutable parent before collection, including
issuer binding, symbols, attachment roles and filing-date proofs. Combination
pins all discovery metadata and permits only explicit parser-output fields to
change; an original-byte hash supplied by the parent remains pinned.

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
the base schema and v2 migration, in that order, to a disposable
`timescale/timescaledb:2.27.2-pg18` database,
then set `FOREIGN_EVIDENCE_DATABASE_URL` to that local database. Load the already combined
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
Validate parser corrections by applying the v2 migration and corrected artifact
over the previously loaded artifact in disposable tables. The corrected reading
must match an isolated import at every query date; source revisions retain their
prospective history.

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

1. Review this evidence PR and the superseding
   [final6 validation report](../validation/sec-foreign-listing-final6.md),
   which replaces the earlier
   [run2 final5 report](../validation/sec-foreign-listing-20261009-run2-final5.md)
   for production artifact selection.
   Keep admission/sizing integration in its separately reviewed PR after
   Workers #173. No scheduled worker is installed by this change.
2. Outside 06:00-08:30 UTC, take fresh read-only W1 exports with `mcp_ro`, host
   `127.0.0.1`, port `65432`, database `market`, and
   `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=30000'`.
   Keep the query text, extraction timestamp and JSON hashes with the run.
3. Use the reviewed owner-load artifact at
   `C:/investintell-data/w1c-final6`, preserving the loaded final5, final4 and run1.
   Verify `SHA256SUMS` and these exact SHA-256 values before loading:

   - `manifest.json`:
     `0c6e6d776c2df22c7738ac6ce748ceee65539204fbedaf92179eaa700fbe53a1`
   - `evidence.jsonl`:
     `2636dbb882829faf23167f34b88decfe097e3bbd67130ea5803d3cb53355044a`

   The complete manifest contains 47,329 source entries and the JSONL contains
   29,655 evidence rows. Its fixed universe is the preserved
   `E:/investintell-data/w1c-20261009-baseline/universe.json`, SHA-256
   `06d052a96fdc8759c6d443f67fbf20ecaacac4655dd7cb306e31f558e5c5b43b`.
   Review the final6 coverage, source audits and the 30+10 precision
   results. Fresh read-only W1 exports are reconciliation evidence; a changed
   universe requires a new reviewed collection rather than replacing this
   artifact's pinned input.
4. Verify the loaded v1 resolver and final5 counts before migrating. The v1
   file remains SHA-256
   `f334d08d3d3b496bd613495d59ee2a3a12365f58c422b3d51bd77530e61d2957`.
   Its `pg_proc.prosrc` MD5 is `111f988c5bede3442fa60a7d6e983172`;
   read back the owner, reader grants and comments against that governed v1
   definition. Verify 47,329 sources, 29,709 active facts and no retired facts
   for the final5 starting state. Any different starting state needs an
   accounted-for reconciliation before proceeding.
   In a separate shell with the authorized owner connection in `DATABASE_URL`
   and without the read-only `PGOPTIONS`, verify and apply v2:

   ```powershell
   Get-FileHash schemas/sec_foreign_listing_evidence_v2.sql -Algorithm SHA256
   psql -X -v ON_ERROR_STOP=1 --dbname=$env:DATABASE_URL --file=schemas/sec_foreign_listing_evidence_v2.sql
   ```

   v2 SHA-256:
   `e17d0523885bc67cc3495e1c9dda1405084f02027c2a6cfc135d5e1e0d5f1a7c`.
   It is one idempotent transaction, adds the nullable reason/CHECK without
   rewriting evidence rows, and retains `worker_writer` ownership, revoked
   PUBLIC privileges and the three reader roles. Fresh environments first
   install the unchanged base SQL, then v2. Do not reapply v1 over v2.
5. Set `FOREIGN_EVIDENCE_DATABASE_URL` securely to that authorized writer.
   Run the following apply-only command with the reviewed universe, complete
   verified cache and exact combined JSONL artifact. Its manifest must have v2
   filing-date and publication-floor proofs. Use the actual reconciliation date;
   do not run discovery or reparse during application.

   ```powershell
   python scripts/load_sec_foreign_listing_evidence.py `
     --universe E:/investintell-data/w1c-20261009-baseline/universe.json `
     --cache-dir C:/investintell-data/w1c-final6 `
     --output C:/investintell-data/w1c-final6/evidence.jsonl --apply
   ```

6. With `mcp_ro`, read back source/fact counts by `retired_reason` and execute
   all 12,155 saved line/date queries against final6's expected answers, plus
   TRIB (888721) and SVRE (1894693) at 2026-10-11. The local production-sequence
   replay matches 12,155/12,155 and both later queries: TRIB is `ambiguous`
   (ADS listing resolved, ratio ambiguous); SVRE is `none` (ADS listing resolved,
   ratio none). The pinned 2026 TRIB F-6 retains conflicting 1/1 and 20/1
   assertions, as documented in the final6 validation report. For the verified
   final5 starting state, expect
   47,329 sources, 59,364 total facts, 29,655 active facts and 29,709 retired
   facts, all `parser_correction`; expect zero `source` or NULL retirements.
   Confirm existing W1 refusal behavior is unchanged.
   Year-end counts with both statuses resolved were 2, 83, 513 and 1,151 for 2010, 2015, 2020
   and 2025. The 2025 overlap was 819 of the preserved 1,373 refusals. Production
   reconciliation must account for existing versions and the actual load date;
   these local results do not assert production acceptance.
   No Railway change, deployment, or admission switch is part of these steps.

   In that separate read-only shell, configure `W1C_READBACK_DATABASE_URL` with
   the operator's `mcp_ro` connection and retain these query results:

   ```sql
   SELECT md5(prosrc) AS resolver_body_md5, pg_get_userbyid(proowner) AS owner
   FROM pg_proc WHERE oid = 'public.sec_foreign_listing_at(bigint,text,date)'::regprocedure;
   -- v1 before migration: 111f988c5bede3442fa60a7d6e983172
   -- v2 after migration:  60f5d1bf86a645a41fb7e23328c7ab8a
   SELECT count(*) AS sources FROM public.sec_foreign_listing_sources;
   SELECT count(*) AS total,
          count(*) FILTER (WHERE retired_on IS NULL) AS active,
          count(*) FILTER (WHERE retired_on IS NOT NULL) AS retired
   FROM public.sec_foreign_listing_evidence;
   SELECT retired_reason, count(*) FROM public.sec_foreign_listing_evidence
   WHERE retired_on IS NOT NULL GROUP BY retired_reason;
   SELECT 'TRIB' AS symbol, DATE '2025-12-31' AS as_of, r.*
   FROM public.sec_foreign_listing_at(888721, 'TRIB', DATE '2025-12-31') r
   UNION ALL SELECT 'SVRE', DATE '2025-12-31', r.*
   FROM public.sec_foreign_listing_at(1894693, 'SVRE', DATE '2025-12-31') r
   UNION ALL SELECT 'TRIB', DATE '2026-10-11', r.*
   FROM public.sec_foreign_listing_at(888721, 'TRIB', DATE '2026-10-11') r
   UNION ALL SELECT 'SVRE', DATE '2026-10-11', r.*
   FROM public.sec_foreign_listing_at(1894693, 'SVRE', DATE '2026-10-11') r;
   ```

   This read-only PowerShell check compares all saved semantic answers without
   reparsing or writing the pinned artifacts:

   ```powershell
   @'
   import json, os
   from pathlib import Path
   import psycopg
   from psycopg.rows import dict_row
   from scripts.validate_sec_foreign_listing_final6 import semantic_answer
   queries = json.loads(Path('C:/investintell-data/w1c-final6/validation/final6/snapshot.json').read_text(encoding='utf-8'))['results']
   expected = {(q['cik'], q['symbol'], q['as_of']): q['answer'] for q in queries}
   assert len(queries) == len(expected) == 12155
   mismatches = []
   with psycopg.connect(os.environ['W1C_READBACK_DATABASE_URL'], row_factory=dict_row,
                         options='-c default_transaction_read_only=on -c statement_timeout=30000') as conn:
       for offset in range(0, len(queries), 100):
           batch = [{k: q[k] for k in ('cik', 'symbol', 'as_of')} for q in queries[offset:offset+100]]
           rows = conn.execute('SELECT q.cik,q.symbol,q.as_of,r.* FROM jsonb_to_recordset(%s::jsonb) AS q(cik bigint,symbol text,as_of date) CROSS JOIN LATERAL public.sec_foreign_listing_at(q.cik,q.symbol,q.as_of) r', (json.dumps(batch),)).fetchall()
           assert len(rows) == len(batch)
           for row in rows:
               key = (row['cik'], row['symbol'], row['as_of'].isoformat())
               if semantic_answer(row) != expected[key]:
                   mismatches.append({'key': key, 'expected': expected[key], 'actual': semantic_answer(row)})
   print(json.dumps({'queries': len(queries), 'matches': len(queries)-len(mismatches), 'mismatches': mismatches}, indent=2))
   raise SystemExit(bool(mismatches))
   '@ | python -
   ```

To roll back only v2, use
`schemas/sec_foreign_listing_evidence_v2.rollback.sql`, SHA-256
`136a5baa8b75591d1564ce9dbd5749e13e90b9cb738c63d30b2d4fd071e23c9f`,
with the exact command:

```powershell
psql -X -v ON_ERROR_STOP=1 --dbname=$env:DATABASE_URL --file=schemas/sec_foreign_listing_evidence_v2.rollback.sql
```

As W1b does, it restores the exact v1 resolver, comments and privileges while
retaining rows and the reason column/CHECK as audit data. V1 ignores the reason:
parser-corrected old readings become visible before retirement again. The v2
loader refuses the rolled-back resolver. Reapply v2 to restore the rule.

If an authorized operator needs to remove this evidence-only installation,
apply `schemas/sec_foreign_listing_evidence.rollback.sql` with
`psql -X -v ON_ERROR_STOP=1`. It removes only the new function and two tables;
it does not modify any W1 table or function. Retain the immutable external
source artifacts and validation report before removing database history.
