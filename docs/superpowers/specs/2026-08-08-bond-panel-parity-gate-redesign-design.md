# Bond panel T3 parity gate redesign

**Date:** 2026-08-08
**Status:** approved direction; implementation pending
**Scope:** `bond_panel_parity` and its operational contract before the first Stage 6 run

## Problem

The current T3 gate treats three different questions as if they were one:

1. whether an approved bond is accounted for;
2. whether that bond is eligible for the monthly model at `t`;
3. whether the numerical implementation is unchanged.

The frozen publication and the DB-only rebuild do not construct membership from
the same temporal identity surface. The frozen backfill imports historical
artifact membership and records historical issuer identity as absent. The live
rebuild starts from the current `bond_curated_universe`, resolves current/PIT DB
inputs, and then applies monthly eligibility. Equality of their included sets is
therefore not a valid formula-parity invariant.

The existing RV comparison has a second mismatch. `rv_signal` is the
cross-sectional z-score of residuals from an OLS fit performed separately for
each monthly cohort. Changing cohort membership changes the fit, residual mean,
and residual standard deviation even when every formula is unchanged. Absolute
RV equality across different cohorts must not be a publication gate.

## Confirmed product contract

- The approved, identified CUSIPs form the **reference universe**.
- Every reference bond must be accounted for exactly once in a monthly snapshot.
- A reference bond may be excluded from the monthly RV cohort for a typed reason,
  including temporal identity, maturity, liquidity, price, terms, or other PIT
  input requirements.
- A typed monthly exclusion does not remove the bond from the reference universe.
- Bonds without the required identification must never enter the RV cohort.
- Legitimate membership changes across months are expected and are not, by
  themselves, evidence of a formula regression.

## Considered approaches

### 1. Increase the current tolerances

Raise the `0.5%` universe bound and RV percentile limits until the production
sample passes.

Rejected. This preserves the category error, chooses thresholds after seeing the
result, and can neither prove formula stability nor detect an unaccounted bond.

### 2. Compare only the intersection

Remove the universe-size gates and compare all numerical fields over common
included keys.

Incomplete. This is appropriate for pointwise YTM, duration, and spread, but an
arbitrarily small intersection could pass, and absolute RV remains invalid when
the two regressions were fit on different cohorts.

### 3. Split accounting, formula parity, and live readiness

Recommended and selected. Each gate answers one question with a denominator and
failure condition appropriate to that question.

## Redesigned gate

### A. Reference-universe accounting

The DB rebuild starts from distinct normalized CUSIP9 keys in
`bond_curated_universe`. For each parity month it must emit exactly one snapshot
row per reference key, either `included` or `excluded`.

Blocking invariants:

- reference keys are non-empty and unique;
- rebuilt snapshot keys are unique;
- `rebuilt_snapshot_keys == reference_keys`;
- every row has a recognized `eligibility_state`;
- every excluded row has a non-empty typed `eligibility_reason`;
- every included row satisfies the identity requirement enforced by eligibility;
- no future-dated input is admitted.

The JSON records reference size, included size, excluded size, and exclusion
counts by reason. These counts are evidence, not tolerance-based approximations.

Frozen-versus-rebuilt included-universe size and membership overlap remain in the
report as diagnostics. They are not blocking gates because the historical frozen
artifact and current DB resolver do not share a temporal identity source.

### B. Formula parity on comparable rows

Pointwise parity is evaluated only on keys included in both frozen and rebuilt
snapshots for the same month.

Blocking invariants:

- at least one declared parity month has a comparable cohort;
- each comparable cohort has at least the existing `MIN_MONTH_ROWS` (`300`) rows;
- existing YTM, duration, and spread percentile thresholds pass;
- spread definition and numerical semantics pass exactly as currently declared;
- config hash, frozen publication identity, fingerprint, status, and lineage
  remain exact.

A month with fewer than `MIN_MONTH_ROWS` common rows is reported as
`parity_not_comparable`, `comparable=false`, and `aborted=false`, with full
accounting evidence. Its overlap percentages remain diagnostic. It is not
independently sufficient to pass or fail formula parity. The overall worker
fails if no declared month is comparable.

### C. RV structural validation

Absolute frozen-versus-rebuilt `rv_signal` delta is diagnostic only. The report
continues to emit its median, p90, and p99 to make cohort sensitivity visible.

The rebuilt RV surface instead has blocking structural invariants:

- it is non-empty for every comparable month;
- keys are unique and are a subset of rebuilt `included` snapshot keys;
- row count equals the model-eligible fitted cohort reported by diagnostics;
- `rv_signal` and `residual_bps` are finite;
- the z-score has mean approximately zero and population standard deviation
  approximately one, using tight numerical tolerances;
- the fit uses only data available at or before `t`.

This validates the implemented RV contract without pretending that separately
fit cohorts must have equal standardized scores.

### D. Overall verdict

The worker returns `state=parity_passed` and `aborted=false` only when:

1. reference accounting passes for every declared month;
2. all comparable months pass pointwise formula parity and RV structure;
3. at least one declared month is comparable;
4. all existing identity, config, lineage, typing, semantic, and walk-forward
   hard gates pass.

Historical included-universe deltas and absolute RV deltas do not affect the
verdict. Their JSON keys remain available as diagnostics to avoid hiding the
observed drift.

## Implementation shape

### `src/workers/bond_panel_parity.py`

- Extend the rebuild result with the normalized reference key set or equivalent
  accounting evidence from `resolved_issuer_sector`.
- Add a pure reference-accounting evaluator and exclusion-reason counts.
- Refactor `_compare_month` so membership drift is diagnostic and pointwise
  metrics operate on a common cohort of at least `MIN_MONTH_ROWS` rows.
- Add pure RV structural checks and retain absolute RV deltas as diagnostics.
- Aggregate monthly results with explicit `comparable` status and require at
  least one comparable month.

### Tests

Update `tests/test_bond_panel_parity_worker.py` to cover:

- large, fully typed historical membership drift does not fail accounting;
- a missing or duplicate reference key fails;
- an untyped exclusion fails;
- one zero-overlap month plus one valid comparable month can pass overall;
- zero comparable months fails overall;
- formula deltas on a comparable cohort still fail at the declared thresholds;
- absolute RV drift alone is diagnostic;
- empty, non-finite, off-center, or non-unit rebuilt RV fails;
- config, parent identity, lineage, spread semantics, and walk-forward gates
  remain fail-closed.

No threshold is changed merely to fit the measured production result.

### Documentation and evidence

- Revise `docs/runbooks/bond-live-daily.md` to describe the three contracts and
  the new overall verdict.
- Revise the calibration evidence with a new gate declaration and a newly run
  literal JSON. Preserve the original no-go evidence as historical evidence;
  do not rewrite it as though the old gate had passed.

## Production boundary

This change updates code, tests, and the declared gate contract. It does not by
itself authorize a Stage 6 production execution. After the PR revision is
verified, the corrected read-only parity worker must run in Railway production.
Only a fresh `parity_passed` JSON permits the separate Stage 6 restart, with
`CODE_REVISION` explicitly set.

## Success criteria

- The gate cannot pass by silently dropping a reference CUSIP.
- An identified reference bond may be monthly-excluded only with a typed reason.
- Formula parity is measured on a sufficiently large like-for-like cohort.
- RV validation is invariant to legitimate cohort membership changes.
- The existing frozen publication and production pointer remain unchanged during
  parity validation.
- The updated focused test suite, lint, compile, and diff checks pass.
