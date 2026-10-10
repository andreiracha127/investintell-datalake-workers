# Foreign cover share-class census (B2 phase 2)

The census records the positive capital-stock statement on a 20-F or 40-F
cover, including unlisted classes. A section 12(b) listed-class set and one
undimensioned XBRL total do not establish a sole ordinary class. DLO's 2024
20-F, for example, gives an A+B total while only A appears on its US listed
line. Neither its listed-class set nor its tagging supplies the missing B
allocation.

This product does not change `sec_cover_ticker_size_basis_at`, W1 admission,
ADS ratios, or Light sizing. A later sizing migration must bind the selected
line to a positive ordinary-class count, prove units and corporate actions,
and retain the existing price, age, currency and duplicate-class gates.

## Sources and statement grammar

Replay reads the pinned W1c raw cache in place with offline mode and a separate
output directory. It does not discover filings or fetch missing documents.
Only primary documents whose exact forms are `20-F`, `20-F/A`, `40-F` or
`40-F/A` enter the corpus; annual-form `securities_description` attachments
are excluded. The final5 manifest contains 19,009 such primary filings and
743 annual-form securities exhibits. Coverage reports official filing year;
the census also retains the economic statement date.

HTML parsing reuses W1c's normalized visible text and cover tables, excluding
hidden inline-XBRL headers, scripts and styles. PDF parsing reuses W1c's PDF
text extraction and records page locations. The original document bytes and
SHA256 remain the source identity. An extraction error or missing raw document
is a reported cache gap, not proof that the issuer has no shares or classes.

The principal anchor is the cover instruction to indicate outstanding shares
of each class at period end. The response runs to the following cover check-mark
instruction. Supported alternate declarations explicitly name outstanding
shares and the class, such as QGEN's "The number of outstanding Common Shares
as of December 31, 2024 was 222,290,848." A nearby section 12(b) title cannot
supply a class name omitted by the capital-stock statement.

Supported responses include count-before-name prose, name-before-count prose,
separate class rows and multi-line cover tables. Counts are exact nonnegative
integers; `nil` is zero. Thousands/millions/billions expressions, negative
notation including Unicode dashes and parenthesized quantities, and unsupported
signs remain incomplete. Supported par-value and date clauses are recognized
as separate grammar, not converted into share counts. Footnote numbers and
other unsupported numeric residue refuse completeness. An explicit date wins;
otherwise the cover's unambiguous fiscal period end supplies the measurement
date. The output retains the original class name, normalized class key,
class kind, count, date, stated total, computed sum, source quote and location.

The parser checks the whole cover, including notes below checkmark questions.
A footnote marker or count qualification leaves the statement incomplete;
the evidence retains the later note rather than silently truncating it at the
first checkmark. SAP's 2024 cover amount includes treasury shares according
to its later note. The census does not subtract those treasury shares to
manufacture an outstanding count. ASX's later option-count/date note likewise
prevents unqualified completeness under this contract.

Report-wide declarations that all share numbers or all share and per-share
amounts were adjusted also qualify a count. A declaration expressly limited
to financial statements, or to named management discussion and financial
statement sections, does not qualify an unmarked cover count. The validator
checks the quoted scope instead of treating every later use of "adjusted"
as cover evidence. Share-price or per-share earnings adjustments alone do
not establish an outstanding-share quantity adjustment.

Table cells and row relationships are source evidence. Two numeric cells or
lines `100` and `200` cannot become `100,200`; a blank Class A row cannot borrow
the following Class B row's leading amount. Unexplained numeric cells,
unresolved bare class tokens such as `A`, and ambiguous row/count associations
refuse completeness. Bare `A and B` or `A & B` combined amounts retain both
named classes without allocating the total. Accepted custom cover declarations
participate in repeat/disagreement checks together with standard prompts.
Hierarchical tables whose class heading has a blank count and whose series
counts occur in later rows remain unsupported. For example, a blank
"Preferred Shares" row above separate "Series 11" and "Series 12" rows
cannot supply a class name or kind to those quantities under the hard-row
rule. This limitation concerns association and class scope; it does not
assert that the source arithmetic is wrong.

Class keys preserve Class versus Series. Letters and Roman numerals follow
the sizing contract's `sec_foreign_class_key` semantics, including numeric
normalization of Roman numerals, and plain `ordinary`/`common` identities.
Ordinary/common, preferred/preference, and other classes such as deferred or
founder shares remain separate. Bare A/B share names identify their explicit
Class A/B tokens; no missing class is invented from a total or a ticker.

## Completeness and conflicts

A census is complete only when every class named by the response has a parsed
count, its date is supported, every numeric token is explained by the grammar,
and any stated total exactly equals the sum of all classes. A zero count is
retained, although a sizing consumer must still require a positive count.
Duplicate or ambiguous class keys, a combined A+B amount without allocations,
an unnamed amount, unsupported numbers, or an ambiguous date leave the census
incomplete. No cover statement found yields `none`.
An aggregate-only declaration such as "Total ordinary shares" or "An aggregate
of ordinary shares" also remains incomplete: it can describe a sum across
ordinary classes without enumerating those classes. Count and date accuracy
alone do not establish positive class scope.

The following cover responses deliberately remain incomplete:

- DLO: "285,475,136 Class A and Class B common shares, as of December 31, 2024."
  The source names A and B but does not allocate the total to either class.
- ZIM: "120,423,333." The response supplies no class name.

Validation cross-checks each census against active W1 counts from the same
CIK and accession. An undimensioned dei amount is compared with the stated
total, or the sum of parsed class counts. A dimensioned count is compared only
when its normalized class token positively binds to the census class. Unknown
tokens and depositary members remain `unbound`; missing counts remain diagnostic.
A positively normalized ordinary class in W1 that is absent from an otherwise
complete census is contrary class evidence and marks it conflicting. Both values and
their dates are stored. Any numerical mismatch marks the census `conflicting`,
including differing dates within the same filing; no value is overwritten or
"fixed" by another source. A complete parser result can therefore become a
conflicting census. Consumers must refuse both incomplete and conflicting
censuses, and must inspect completeness separately from absence of conflict.

Coverage categories are mutually exclusive: conflicting takes priority,
followed by complete, incomplete and none. A filing without a census record
is counted as none and listed as a gap. Cross-check statistics retain
same-date versus different-date mismatch counts. The precision sample ranks
complete nonconflicting filings deterministically by the SHA256 of a fixed
seed and source identity. A sample packet is not a precision certificate:
each of its 40 source statements needs an explicit review judgment.

## Storage, availability and restatement

Apply `schemas/sec_foreign_share_census_v1.sql` after W1c base and v2. The
additive relation is `public.sec_foreign_share_census`; the point API is
`public.sec_foreign_share_census_at(p_cik bigint, p_as_of date)`. It returns
the latest census public by D, ordered by statement date and then filing
order, retaining its classes and status. It does not elect an older complete
census to hide a newer incomplete or conflicting disclosure. The function
uses one fully qualified SQL statement, `LANGUAGE sql STABLE`, and no
function-level `SET` so it can inline in a later sizing statement.

Source availability follows W1c: the greatest of the official filing date
plus one day and the governed publication floor. `source_available_on`
records that source clock. `available_on`, `retired_on`, `retired_reason`,
parser version, source accession/URL/hash, quoted text and location retain
the reconciliation history.

- `source`: public bytes or documents changed. The replacement is known no
  earlier than reconciliation; the preceding version remains visible before
  retirement.
- `parser_correction`: the parser reread the same source bytes. The superseded
  reading is excluded at every date; the corrected reading inherits the
  replaced reading's availability. A new reading of already-loaded bytes can
  restate history at the governed source date. A genuinely new document keeps
  the first-loaded protection.

DDL and census reconciliation take the same transaction advisory lock
`(79311, 173)` as W1c. `worker_writer` owns the relation and function. PUBLIC
privileges are revoked; `app_runtime`, `app_analytics_ro` and `mcp_ro` receive
read/execute access. The existing W1c evidence reconciliation is independent
and unchanged; census application does not reparse or replace listing facts.

## Offline validation

All new artifacts for this run belong under `C:/investintell-data/w1c-census-r2/`.
The raw final5 cache is read-only. Use no more than 12 parse workers on the
shared machine. Preserve the pinned input manifest, census JSONL, run metrics
and `SHA256SUMS`. Missing documents are reported without downloading them.

```powershell
python scripts/load_sec_foreign_share_census.py `
  --manifest E:/investintell-data/w1c-20261009-run2/final5/manifest.json `
  --cache-dir C:/investintell-data/w1c-census-r2 `
  --raw-cache-dir E:/investintell-data/w1c-20261009-run2/final5 `
  --output C:/investintell-data/w1c-census-r2/census.jsonl `
  --w1-counts C:/investintell-data/w1c-census-r2/w1-counts.json `
  --offline --workers 12
```

After replay and local W1 cross-check export, run:

```powershell
python scripts/validate_sec_foreign_share_census.py `
  --census C:/investintell-data/w1c-census-r2/census.jsonl `
  --corpus-manifest E:/investintell-data/w1c-20261009-run2/final5/manifest.json `
  --raw-cache-dir E:/investintell-data/w1c-20261009-run2/final5 `
  --sizing-rows C:/investintell-data/w1c-census-r2/sizing-2025.json `
  --sample-seed workers-pr188-r2-independent-source-v1 `
  --exclude-review C:/investintell-data/w1c-census/validation/precision-reviewed.json `
  --exclude-review C:/investintell-data/w1c-census-r2/prior-gate-reviewed-accessions.json `
  --compare-census C:/investintell-data/w1c-census/census.jsonl `
  --output-dir C:/investintell-data/w1c-census-r2/validation
```

The optional raw-cache argument verifies all 40 original source hashes and
quotes and includes the whole cover window, source table cells, a separately
identified post-statement cover boundary, and notes after checkmark questions.
The Round 2 precision seed selects distinct CIKs across four forms and five
filing-year bands and excludes previously reviewed accessions. Review the
deterministic `precision-sample.json` against the original cached
documents, then save a separate reviewed packet with the exact census hash,
sample IDs, `correct`/`incorrect` judgments and source notes. Re-run validation
with `--precision-review` pointing to that packet. Preserve all incorrect
judgments; a parser fix requires regenerating the census, reselecting its
deterministic complete sample and reviewing the new packet.

The old Round 1 artifact directory is read-only. `--compare-census` produces
the full old/new status flip matrix, overlapping COMPLETE-to-INCOMPLETE reason
counts, and a deterministic review packet with a representative of every new
reason plus at least 30 filings when available. Review those sources against
the whole cover, including table row/count association and later footnotes.
Record both necessary refusals and conservative coverage losses; a status
change alone does not prove that the original source census was invalid.
Keep unreviewed packets pending and report any false completeness before
certifying precision or recalculating accepted impact.

Named review includes DLO, BIDU, NVO, TSM, ASML, QGEN, ZIM, NTES, SAP and CNQ.
Quotes must come from their actual capital-stock statements, and a source that
does not provide a per-class allocation remains incomplete. The impact report
joins the corrected #186 year-end-2025 refusal cohort to the count's exact
same filing. The census must be public by the cutoff and its own statement
date must be 0 to 400 days old. It supplies its own class count; the subset
with the exact W1 measurement date is reported separately. It distinguishes a positive sole ordinary
class from an explicit positive count for the line's class. It reports
class-scope proof potential, not the number of fully admitted sizes.
The sizing cohort is measured on a local PG18 restore of the pinned W1/W1c
exports with corrected sibling #186 SQL bytes. Its recorded SQL SHA256
identifies the measured code; it does not claim those local changes are a
committed PR head or currently active in production.
Sole-ordinary proof also requires every other named class to be positively
preferred/preference, deferred or founder. An untyped `other` class cannot
prove that no second ordinary class exists. NVO's bare A/B share counts remain
useful explicit census evidence, but their ordinary/common kind is not proved
by that cover statement alone.
`Founder` and `Founders` use the same nonordinary proof predicate; plural
wording does not reduce coverage. A zero/nil untyped second class still blocks
sole-ordinary proof, and a zero ordinary count cannot qualify.

## Production procedure (deferred; not executed by this PR)

1. Review the final PR head, migration/rollback hashes, parser version, pinned
   input manifest, census artifact, source/cross-check hashes and validation
   report. Verify completeness, named source quotes and all 40 manual reviews.
   Keep Light on its current sizing API; this PR supplies evidence only.
2. Stage the exact reviewed loader and immutable artifacts outside the raw
   cache. Confirm W1c base+v2, ownership, reader roles and the shared advisory
   lock. Set bounded lock and statement timeouts. Apply the additive census
   migration as owner with `psql -X -v ON_ERROR_STOP=1`.
3. Use the reviewed artifact's apply-only loader path with the securely supplied
   authorized writer connection and the actual reconciliation date. Apply must
   verify hashes, schema and parser contract after obtaining the shared lock.
   Do not discover, download or reparse during publication. Listing-evidence
   apply remains its separate existing path.

   ```powershell
   python scripts/load_sec_foreign_share_census.py `
     --manifest C:/investintell-data/w1c-census-r2/manifest.json `
     --cache-dir C:/investintell-data/w1c-census-r2 `
     --output C:/investintell-data/w1c-census-r2/census.jsonl `
     --apply --observed-on YYYY-MM-DD
   ```

   Supply `FOREIGN_CENSUS_DATABASE_URL` securely for the authorized writer.
   Substitute the actual reconciliation date; the command reads the reviewed
   artifact and does not invoke offline replay unless `--offline` is supplied.
4. Read back as `app_runtime` or `mcp_ro` in a repeatable-read, read-only
   transaction with `jit=off`, bounded statement/lock/idle timeouts, and no DSN
   logging. Compare active census/source identities, class/count JSON,
   completeness/conflict status, cross-check results and retirement reasons
   with the reviewed artifact. Test named PIT boundaries and confirm an EXPLAIN
   has no `Function Scan on sec_foreign_share_census_at`.
5. A later sizing v2 migration must consume this proof explicitly and pass its
   own gate before any Light activation. A source conflict or incomplete latest
   statement continues to refuse; production acceptance is not local coverage.
6. Before rollback, drain or deactivate any future dependent consumer. Apply
   `schemas/sec_foreign_share_census_v1.rollback.sql` as owner with
   `ON_ERROR_STOP`, then verify the census objects are absent and existing
   W1/W1c APIs and evidence remain intact.

This phase authorizes no production writes, deployment, merge or consumer
activation. The procedure above is an operator handoff for a later authorized
window.
