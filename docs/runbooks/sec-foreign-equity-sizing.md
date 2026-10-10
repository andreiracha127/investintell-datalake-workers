# Foreign equity sizing basis (B2 phase 1)

This additive contract uses the installed W1 v1-v3 and W1c base plus v2 facts.
Apply `schemas/sec_foreign_equity_sizing_v1.sql` explicitly as the database
owner. Workers and loaders do not apply migrations implicitly. The migration
and its rollback use `lock_timeout = '5s'` and the W1c transaction advisory lock
`pg_advisory_xact_lock(79311, 173)`. Functions are owned by `worker_writer`,
PUBLIC execution is revoked, and `app_runtime`, `app_analytics_ro` and `mcp_ro`
receive execution privileges when those roles exist.

## Public contracts

`public.sec_foreign_listing_context_at(cik, symbol, effective_on, known_on)`
returns exactly one row. Its first seven columns are the legacy W1c contract:
`status`, `listed_type`, `ratio_numerator`, `ratio_denominator`,
`listing_status`, `ratio_status`, `evidence_ids`. It adds `listing_class`,
`ratio_class`, `program_key`, `ratio_effective_from`, `ratio_effective_to`
from the elected listing and ratio facts. Audit IDs can also contain controls
and settlement evidence; their classes must not be used to infer the elected
class. Economic eligibility uses `effective_on`; public availability and
retirement use `known_on`. Pending and conflicting plans, corroboration and
v2 parser-correction invisibility retain their existing semantics.

`public.sec_foreign_listing_at(cik, symbol, D)` is the seven-column projection
of that core at `(D,D)`. Its legacy ABI and answers remain unchanged. Neither
function has a function-level `SET`.

Both projections use the single internal
`public.sec_foreign_listing_election_at(cik, symbol, effective_on, known_on)`.
It adds an elected `program_ambiguous` diagnostic for sizing. Distinct programs
with the same numerical entitlement retain the legacy v2 answer but refuse
sizing class binding; zero program tokens remains distinguishable from multiple
tokens. No election is copied into a second resolver.

`public.sec_cover_ticker_size_basis_at(ticker, cik, line_members, D,
max_age_days DEFAULT 400)` returns exactly one row with:

| Columns | Meaning |
|---|---|
| `status`, `refusal` | Admission result and readable `code: TICKER ...` reason |
| `ordinary_shares`, `shares_as_of`, `adsh`, `count_class_key` | Usable ordinary count and its elected date, accession and raw W1 member |
| `canonical_underlying_class_id`, `share_unit`, `basis` | Economic ordinary-class identity and units; phase 1 admits only `class` scope. `sole_class_total` remains a refused audit classification |
| `class_binding` | `explicit` on phase 1 success; `sole_ordinary_class_proven` is reserved for future positive census evidence |
| `listed_type`, `ratio_numerator`, `ratio_denominator` | Listed units and ordinary shares per ADS in force at D |
| `count_ratio_numerator`, `count_ratio_denominator` | W1c at economic count date S with knowledge cutoff D; both statuses must resolve, with valid historical class binding and an unambiguous program |
| `listing_status`, `ratio_status`, `program_key` | W1c diagnostics and elected program token |
| `exchange_name` | The latest visible W1 line observation's `dei:SecurityExchangeName` at D; conflicting or missing values return NULL |
| `evidence` | Audit diagnostics; not an alternate source of usable counts |

The SQL functions are non-STRICT, STABLE, PARALLEL SAFE, SECURITY INVOKER, single
statements without function-level settings, so PostgreSQL can inline them. Pass the
admitted W1 line's raw members in `line_members`; the caller remains responsible
for issuer/line admission and price ownership. A missing or refused input never
receives an invented 1/1 ratio. Only a resolved `ordinary_direct` W1c contract
establishes 1/1.

## Count election and explicit same-filing class scope

The shared internal count election retains W1's latest stated date, availability,
acceptance timestamp and accession ordering, together with its exact competing
count rules. W1's existing public share functions retain their previous answers.
`sec_cover_share_election_at` owns that election. Only the new sizing API invokes
`sec_cover_sizing_share_detail_at` to evaluate the elected filing's labels, units,
class labels and audit evidence. The legacy class and ticker APIs do not execute
or plan this sizing-only work, including for domestic equities.
A conflicting freshest count cannot be replaced with an older convenient count.
The selected count's own accession must bind an explicit ordinary-class member
to the listed line's underlying class. A later filing cannot validate an older
count's scope.

Every foreign issuer total without that explicit same-filing class dimension
returns `share_total_class_scope_unverified`, including 20-F, 40-F, 6-K and
20-FR totals. A Class A title does not bind an undimensioned total to Class A.
Neither `filing_equity_classes = 1` nor one observed count class proves that
an unlisted class is absent. Phase 1 admits only explicit class-dimensioned
ordinary counts that bind to the line's underlying class.

[EFM v68, section 6.5.26, printed page 6-18](https://www.sec.gov/files/edgar/filermanual/archive/efmvol2-v68.pdf)
requires separate `dei:EntityCommonStockSharesOutstanding` facts in dimensioned
contexts when multiple common classes are outstanding; its cover-fact table
marks the fact required for 20-F and optional for 40-F and 20FR12B/20FR12G.
This tagging requirement is not evidence that an accepted filing complied.
DLO's 2024 20-F, accession `0000950170-25-058197`, tags an undimensioned total
of 285,475,136 while the same filing reports 151,420,944 Class A plus
134,054,192 Class B shares. The Nasdaq Class A title cannot turn that A+B total
into Class A supply. See the [20-F](https://www.sec.gov/Archives/edgar/data/1846832/000095017025058197/dlo-20241231.htm)
and [share-capital exhibit](https://www.sec.gov/Archives/edgar/data/1846832/000095017025058197/dlo-ex2_1.htm).

The total-scope refusal is one CASE branch reserved for a later migration to
extend with positive same-filing census evidence from the 20-F cover's statement
of outstanding shares for each issuer class. No census source, parser or new
evidence relation is added here. `sole_class_proven` remains false in phase 1.

Positive unit and class proof belong to each elected count's own accession, CIK,
member and `dimh`. Observation titles from another context supply neither units
nor class identity. Every elected count must have its own unique identity and
agree on ordinary units; equal numerical values cannot lend one count's proof
to another. Audit `count_unit_contexts` retains each count's context, proof labels,
unit and veto flags.

Veto evidence has a separate, filing-wide rule. An observation linked by raw
member or canonical class key vetoes non-ordinary units regardless of its `dimh`.
Every non-equity kind and explicit depositary, preferred, preference, deferred,
founder, unknown or conflicting wording blocks admission. Count-side canonical
keys include its member identities and strict-valid own-context title identities.
Observation-side negative keys identify the security that the observation is
about: a subject Class/Series identity at the start of its member or caption
clause. In mixed captions, a title-derived unit contradiction applies to that
clause's class; Class B preferred wording cannot veto Class A common shares.
References to underlying stock in an ADS caption or stock purchasable through a
warrant do not make those instruments the ordinary stock class. These keys only
establish veto associations; they never supply positive count proof. Evidence
about another class cannot veto this count. A linked observation with several
class names cannot broaden a count's scope merely by its title; a linked
non-ordinary kind still vetoes units.
`count_unit_veto_evidence` records the exact facts, keys and link flags.

An ADS classification requires depositary evidence on the count's own member or
exact context; that evidence returns `ordinary_class_shares_unavailable`.
An ADS veto from another context denies ordinary admission but cannot assign ADS
units to the count. It returns `share_count_unit_unverified`. Conflicting
ordinary/depositary evidence and every other linked unit veto also return
`share_count_unit_unverified`. A Common/Ordinary member hint can support units
only in a filing without depositary evidence and without a linked veto. It must
be a whole word in the raw member or its member-only W1 normalization; the
canonical lexer cannot create proof from substrings such as Commonly. The 75
Round 4 unit refusals remain; safe recovery needs positive source attestation
attached to the elected count in a separate migration. A separately evidenced
ordinary count beneath an ADS line remains admissible when it has no veto.

Count titles use a whole-title whitelist: Class or Series, one identifier,
optional ordinary/common/voting descriptors, then share, shares or stock.
Tokenization lowercases raw titles and normalizes whitespace; it does not use
the legacy camel-case normalizer. Quotes may enclose the identifier. The grammar
accepts compact ClassA spelling and a closed par/nominal/no-par value suffix,
including a numeric value, currency marker, per-share wording and footnote stars.
The finite suffix also accepts `par value of $0.01 per share`,
`without par value`, `no-par value`, and equivalent nominal-value or
currency-before-value forms. A second class identity or other residue still
rejects the entire title.
These SEC cover qualifiers do not name another class. A generic common/ordinary
title is allowed only when the count's own member supplies one class identity.
Missing own-context titles do not invalidate that explicit member identity.
Every nonempty own-context title must match the whitelist. Bare identities,
coordinators between identities, unmatched prefixes and other residue refuse.

Member identities use a separate case-insensitive lexer. Class and Series remain
distinct, and Roman numerals retain the existing canonical equivalence. The union
of member and valid own-title identities must contain exactly one class per count.
Coordination in the count's own title or member refuses scope. A different
observation's ordinary title, whether it names another class or several classes,
does not veto the count's own single identity. The filing-wide veto is solely
explicit non-ordinary unit evidence linked by the same member or canonical
class; it is not a filing-wide class census.
`count_labels` contains proof labels only; rejected raw titles remain in
`count_title_evidence`. `count_class_scope_veto_evidence` remains an empty audit
array for compatibility. `count_scope_unverified` and `count_labels_ambiguous`
expose contradictory or absent count-owned scope proof, which
refuses `foreign_listing_class_ambiguous`. Frozen legacy W1 bodies are unchanged.

Explicit binding also requires a unique canonical stock line for the elected
normalized label. The sizing detail helper uses W1's canonical issuer-line
engine at the count filing's source date, retaining the latest complete cover
and subsequent incomplete filings. Two distinct stock lines both named Series A
refuse even when only one supplies the latest count. Dated raw-member aliases
of one canonical line, count tags accompanying one registration, and a separately
evidenced underlying ordinary count beneath an ADS wrapper remain admissible.
The audit records the source horizon, cohort, canonical lines and collision
evidence. This check does not change count-owned positive unit proof or the
same-filing member/class unit veto.

Observation subject extraction accepts an explicit coupon prefix such as
`8.250% Series B`, `8.25 percent Series B`, or a named fixed-to-floating rate
prefix. A preferred caption can therefore link a veto to Series B even when its
raw member is opaque. Unmarked years or numbers cannot supply that prefix, and
evidence about Series C cannot veto Series B.

The small public SEC fixtures in `tests/fixtures/sec_foreign_equity_sizing/`
preserve twelve issuer configurations and quoted Round 4 outcomes. The real-data
acceptance floor is checked separately from the synthetic differential oracle:
over the original 194 row-dates, every loss of a Round 4 resolved row needs a
quoted, linked non-ordinary contradiction from that filing. The 75 earlier
unit-proof refusals remain outside this recovery.

## Refusals and Light's remaining gates

The elected ADS ratio may omit its class while the elected listing names the
underlying class. Sizing can use that listing label in both count selection and
class binding only within the same resolved, unambiguous program. Existing
listing facts have no program key under the evidence CHECK, so this fallback
requires an unkeyed ratio (`program_key IS NULL`) and no competing elected
program. A keyed ratio cannot borrow the listing label. A conflicting non-NULL
ratio class remains a mismatch. This rule never binds an undimensioned total:
the separate explicit-count scope gate still refuses it.

Both count-date listing and ratio statuses must be `resolved` before usable
`count_ratio_*` values are exposed. These predicates are cumulative with
historical class binding and program checks. `evidence.count_ratio_refusal`
names a historical refusal while preserving the current ordinary-count basis
contract. `count_ratio_evidence_facts` retains raw source values and roles for
audit only; it does not elect a class or entitlement. Light must reject a size
when these historical factors are unusable.

Source/count validity precedes scope and unit proof; W1c ambiguity, missing
listing, missing or invalid ratio, class mismatch/ambiguity and staleness follow.
Missing proof uses explicit NULL-safe predicates. Independent diagnostics remain
in `evidence` even when an earlier refusal takes precedence.

| Code | Condition |
|---|---|
| `share_total_class_scope_unverified` | The elected foreign total has no explicit same-filing class dimension bound to the listed underlying class |
| `ordinary_class_shares_unavailable` | The selected count explicitly represents depositary units |
| `share_count_unit_unverified` | The selected member does not establish ordinary units |
| `foreign_listing_ambiguous` | W1c listing or ratio is ambiguous, including pending/conflicting plans |
| `foreign_issuer_listing_unverified` | No resolved listed-security contract by D |
| `depositary_ratio_unsourced` | An ADS line has no resolved ordinary-shares-per-ADS ratio |
| `depositary_ratio_invalid` | A resolved ratio is missing, nonpositive or nonfinite |
| `foreign_listing_class_mismatch` | A known W1c class differs from the selected count's class |
| `foreign_listing_class_ambiguous` | Class/program binding has no unique proof; a NULL ratio class can use a resolved listing label only under the same-program conditions above; a NULL underlying class needs positive scope proof, unavailable for totals in phase 1 |
| `stale` | The elected count exceeds `max_age_days`; 400 days is admitted, 401 is stale |

Existing missing, ambiguous, nonpositive, line and source refusals remain
authoritative. Refused results NULL the usable count and factors while retaining
the selected accession/date and audit diagnostics.

The `(S,D)` historical W1c lookup repeats the current class-binding checks.
A known historical listing or ratio class mismatch makes `count_ratio_*` NULL;
a NULL historical ratio class can use the elected listing class only under the
same resolved, unkeyed-program conditions. If both labels are NULL, positive
scope proof is required and remains unavailable for totals in phase 1. Phase 1 does not
infer that proof from an incomplete tagged census. Raw historical classes,
ratios, statuses, program and binding diagnostics remain in `evidence`; audit
facts must never substitute for NULL usable factors.

Light must additionally check the facts returned here. For ADS sizing, usable
ratio factors at S must be present and equal the ratio at D, and every ADS split factor over `(S,D]`
must be 1; otherwise it refuses the size. Light also requires the line's
exchange to be a US national exchange. Workers does not decide price currency,
event adjustments, USD prices or final market size in this phase. Phase 1
creates no publication registry, archived replay, events table, currency/share
basis relation, adapter or `publication_id` parameter.

## Owner-applied production procedure

The commands below are a procedure for a later authorized rollout. Local PR
validation does not apply them in production.

1. Verify the deployed W1 v1-v3 and W1c base/v2 dependencies, the candidate
   migration/rollback hashes, roles/grants and final7 pins, and that Light has
   not started calling the new contract. Stage the matching loader/operator
   guard before the DDL, without activating it. Apply the additive DDL first:

   ```powershell
   psql -X -v ON_ERROR_STOP=1 --dbname=$env:DATABASE_URL --file=schemas/sec_foreign_equity_sizing_v1.sql
   ```

2. Activate the matching loader code and any operator-side schema guard after
   the DDL and before subsequent ingestion. The
   updated `scripts/load_sec_foreign_listing_evidence.py::require_schema`
   accepts exact v2 alone or the exact internal election, context and legacy
   projections, including their ABI, non-STRICT behavior, SQL/STABLE/PARALLEL
   SAFE/SECURITY INVOKER flags, settings, owner and execution grants. The original
   v2 resolver retains its exact `search_path` setting; all sizing projections
   and the election require no function-level settings.
   The old v2-only body guard rejects the new projection. Keep schema and loader
   steps ordered under the same reconciliation-lock contract. No artifact
   reparse or reload is required to install this migration.

3. Run the runbook W1c readback with the updated composite guard, then inspect
   function owner, ABI, settings and grants. Replay all 12,155 final7 semantic
   queries and the named sizing probes with read-only credentials and the
   executable repeatable-read read-only snapshot, JIT off, 30-second statement,
   5-second lock and 60-second idle timeouts. Record separate W1c, count, non-stale, class-proof, binding
   and sizing-resolution stages. A resolved basis alone does not prove a final
   Light size.

4. Activate Light's foreign-sizing calls only after Workers readback passes,
   retaining its price admission, exchange, usable historical/current ratio
   equality, split and duplicate-class gates.

5. For rollback, remove dependent Light calls and drain requests first, then apply
   `schemas/sec_foreign_equity_sizing_v1.rollback.sql` with `ON_ERROR_STOP=1`.
   It restores the exact W1 v3 share resolvers and W1c v2 resolver, settings,
   comments and privileges, and removes the sizing helpers. The updated loader
   continues to accept the restored v2 resolver. Every W1/W1c evidence row
   remains intact.
