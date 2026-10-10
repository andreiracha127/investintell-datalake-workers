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
| `canonical_underlying_class_id`, `share_unit`, `basis` | Economic ordinary-class identity, units and `class` / `sole_class_total` scope |
| `class_binding` | `explicit` / `sole_ordinary_class_proven` on success |
| `listed_type`, `ratio_numerator`, `ratio_denominator` | Listed units and ordinary shares per ADS in force at D |
| `count_ratio_numerator`, `count_ratio_denominator` | W1c at economic count date S with knowledge cutoff D |
| `listing_status`, `ratio_status`, `program_key` | W1c diagnostics and elected program token |
| `exchange_name` | The latest visible W1 line observation's `dei:SecurityExchangeName` at D; conflicting or missing values return NULL |
| `evidence` | Audit diagnostics; not an alternate source of usable counts |

The SQL functions are STABLE, PARALLEL SAFE, SECURITY INVOKER, single statements
without function-level settings, so PostgreSQL can inline them. Pass the
admitted W1 line's raw members in `line_members`; the caller remains responsible
for issuer/line admission and price ownership. A missing or refused input never
receives an invented 1/1 ratio. Only a resolved `ordinary_direct` W1c contract
establishes 1/1.

## Count election and sole-class proof

The shared internal count election retains W1's latest stated date, availability,
acceptance timestamp and accession ordering, together with its exact competing
count rules. W1's existing public share functions retain their previous answers.
`sec_cover_share_election_at` owns that election. Only the new sizing API invokes
`sec_cover_sizing_share_detail_at` to evaluate the elected filing's labels, units,
class census and audit evidence. The legacy class and ticker APIs do not execute
or plan this sizing-only work, including for domestic equities.
A conflicting freshest count cannot be replaced with an older convenient count.
The selected count's own accession supplies its class census; a later filing
cannot validate an older total.

For a foreign undimensioned issuer total, phase 1 requires both the selected
filing's `filing_equity_classes = 1` and exactly one share-count class in that
filing. W1's census includes equity symbol classes, dimensioned count classes
and titled equity classes without a symbol. A single listed ticker alone is
insufficient.

The tagging justification is EFM 6.5.26: separate facts and dimensioned contexts
are required for multiple outstanding common classes, whereas a sole class uses
the undimensioned context. The [EFM v68 rule](https://www.sec.gov/files/edgar/filermanual/archive/efmvol2-v68.pdf)
marks the fact required for 20-F, optional for 40-F and optional for
20FR12B/20FR12G. The [EDGAR XBRL Guide, August 2026, section 3.2.3, printed page 65](https://www.sec.gov/files/edgar/filer-information/specifications/xbrl-guide-2026-08-14.pdf)
retains the mutually exclusive one-class / multiple-class cases and excludes
Canadian annual instances from the mandatory fact-presence validation.
Phase 1 therefore admits the sole-total inference only for 20-F and its
amendments. A 40-F, 6-K or registration-form total has weaker evidence and
returns `share_total_class_scope_unverified`; an explicit ordinary class count
can still be eligible. This is a restriction of existing evidence, with no
new parser or evidence relation.

Explicit depositary-member counts count ADS units and always return
`ordinary_class_shares_unavailable`. They do not establish the full outstanding
ordinary class even when the ADS ratio is known. Preferred and unknown count
units also fail closed. Class and Series namespaces remain distinct, and Roman
numerals are preserved by the shared class-token normalizer.

## Refusals and Light's remaining gates

Source/count validity precedes scope and unit proof; W1c ambiguity, missing
listing, missing or invalid ratio, class mismatch/ambiguity and staleness follow.
Missing proof uses explicit NULL-safe predicates. Independent diagnostics remain
in `evidence` even when an earlier refusal takes precedence.

| Code | Condition |
|---|---|
| `share_total_class_scope_unverified` | The elected total's own filing does not prove a sole ordinary class |
| `ordinary_class_shares_unavailable` | The selected count explicitly represents depositary units |
| `share_count_unit_unverified` | The selected member does not establish ordinary units |
| `foreign_listing_ambiguous` | W1c listing or ratio is ambiguous, including pending/conflicting plans |
| `foreign_issuer_listing_unverified` | No resolved listed-security contract by D |
| `depositary_ratio_unsourced` | An ADS line has no resolved ordinary-shares-per-ADS ratio |
| `depositary_ratio_invalid` | A resolved ratio is missing, nonpositive or nonfinite |
| `foreign_listing_class_mismatch` | A known W1c class differs from the selected count's class |
| `foreign_listing_class_ambiguous` | Class/program binding has no unique proof; NULL class requires sole-class proof |
| `stale` | The elected count exceeds `max_age_days`; 400 days is admitted, 401 is stale |

Existing missing, ambiguous, nonpositive, line and source refusals remain
authoritative. Refused results NULL the usable count and factors while retaining
the selected accession/date and audit diagnostics.

Light must additionally check the facts returned here. For ADS sizing, the
ratio at S must equal the ratio at D and every ADS split factor over `(S,D]`
must be 1; otherwise it refuses the size. Light also requires the line's
exchange to be a US national exchange. Workers does not decide price currency,
event adjustments, USD prices or final market size in this phase. Phase 1
creates no publication registry, archived replay, events table, currency/share
basis relation, adapter or `publication_id` parameter.

## Owner-applied production procedure

The commands below are a procedure for a later authorized rollout. Local PR
validation does not apply them in production.

1. Verify the deployed W1 v1-v3 and W1c base/v2 dependencies, the candidate
   migration/rollback hashes, and that Light has not started calling the new
   contract. Apply the additive DDL first:

   ```powershell
   psql -X -v ON_ERROR_STOP=1 --dbname=$env:DATABASE_URL --file=schemas/sec_foreign_equity_sizing_v1.sql
   ```

2. Install the matching loader code and any operator-side schema guard. The
   updated `scripts/load_sec_foreign_listing_evidence.py::require_schema`
   accepts exact v2 alone or the exact internal election, context and legacy
   projections, including their ABI, settings, owner and execution grants.
   The old v2-only body guard rejects the new projection. Keep schema and loader
   steps ordered under the same reconciliation-lock contract. No artifact
   reparse or reload is required to install this migration.

3. Run the runbook W1c readback with the updated composite guard, then inspect
   function owner, ABI, settings and grants. Replay all 12,155 final7 semantic
   queries and the named sizing probes with read-only credentials, JIT off and
   bounded timeouts. Record separate W1c, count, non-stale, class-proof, binding
   and sizing-resolution stages. A resolved basis alone does not prove a final
   Light size.

4. For rollback, remove dependent Light calls first, then apply
   `schemas/sec_foreign_equity_sizing_v1.rollback.sql` with `ON_ERROR_STOP=1`.
   It restores the exact W1 v3 share resolvers and W1c v2 resolver, settings,
   comments and privileges, and removes the sizing helpers. The updated loader
   continues to accept the restored v2 resolver. Every W1/W1c evidence row
   remains intact.
