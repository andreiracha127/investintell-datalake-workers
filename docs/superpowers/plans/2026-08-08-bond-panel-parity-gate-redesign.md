# Bond Panel T3 Parity Gate Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate reference-universe accounting, like-for-like formula parity, and rebuilt-RV structural validation so legitimate historical membership drift cannot falsely block Stage 6 readiness.

**Architecture:** The parity rebuild will expose its raw reference CUSIPs and monthly fit diagnostics. Pure evaluators will validate exact reference accounting and rebuilt RV structure; `_compare_month` will apply formula thresholds only when at least the existing `MIN_MONTH_ROWS` bonds are common to both included cohorts. A pure overall aggregator will accept noncomparable months only when another month is comparable and every hard gate passes.

**Tech Stack:** Python 3.13, pandas, NumPy, statsmodels outputs, pytest, Ruff 0.15.9, `compileall`, PostgreSQL read-only worker seams.

## Global Constraints

- Work only in `E:\investintell-datalake-workers-live-daily` on `feat/bond-panel-pack-live`.
- Implementation starts after this plan commit; record that commit in the
  task-specific `INVESTINTELL_PARITY_BASE` environment variable for final diff
  checks.
- Preserve unrelated dirty state and stage only task-owned paths.
- Do not deploy, invoke Railway, change service variables, connect to production, write a database, move a publication pointer, or create a Stage 6 run JSON.
- A future read-only production parity run and any Stage 6 action require separate authorization.
- Do not change `PANEL_CONFIG_HASH`, `BASE_PUBLICATION_ID`, `BASE_INPUT_FINGERPRINT`, `PARITY_MONTHS`, or `SPREAD_DEFINITION`.
- Import and reuse `MIN_MONTH_ROWS = 300`; do not create a new sample-size threshold.
- Keep the existing YTM, duration-absolute, duration-relative, and spread limits unchanged.
- Keep configuration, publication identity, fingerprint, status, frozen lineage, typing, spread semantics, and walk-forward checks fail-closed.
- Normalize CUSIPs with pandas string dtype, trim, and uppercase. Null, blank, or duplicate raw reference keys remain blocking evidence.
- Every valid reference key must appear exactly once in the rebuilt snapshot as `included` or `excluded`.
- Every excluded row requires a nonblank typed reason. Every included row requires a nonblank `issuer_id`.
- Historical membership size/overlap and absolute cross-cohort RV differences remain visible diagnostics and never enter the verdict conjunction.
- A month is comparable only when common included keys are at least `MIN_MONTH_ROWS`; no coverage-percentage gate is allowed.
- Monthly states are exact: failed = `parity_failed`, `comparable=false|true`, `aborted=true`; comparable pass = `parity_passed`, `comparable=true`, `aborted=false`; insufficient common cohort = `parity_not_comparable`, `comparable=false`, `aborted=false`.
- Overall pass requires zero failed months and at least one comparable passed month.
- RV tolerances are fixed independently of production results: `abs(mean) <= 1e-10` and `abs(std(ddof=0) - 1) <= 1e-10`.
- Use `python -m pytest`, `ruff check`, and `python -m compileall`; this repository has no `pyproject.toml` or `uv.lock`.
- Ruff only the changed Python scope. The 116 pre-existing global violations are out of scope.

## File Responsibility Map

| File | Responsibility |
| --- | --- |
| `src/workers/bond_panel_parity.py` | Reference evidence, pure accounting/RV evaluators, monthly comparison, overall verdict, read-only orchestration. |
| `tests/test_bond_panel_parity_worker.py` | Deterministic 300-row fixtures and contract/orchestration tests. |
| `docs/runbooks/bond-live-daily.md` | Operator-facing three-contract semantics and authorization boundary. |
| `docs/calibration/bond_panel_pack_live_evidence_001.md` | Immutable historical no-go plus append-only revised-contract status. |

---

### Task 1: Build deterministic fixtures and exact reference accounting

**Files:**
- Modify: `tests/test_bond_panel_parity_worker.py:1-35`
- Modify: `src/workers/bond_panel_parity.py`, pure-helper area near `_typed_exclusions`

**Interfaces:**
- Produces `parity.MIN_MONTH_ROWS`-sized `_snapshot`, `_rv`, and `_fit_diagnostics` test frames.
- Produces `_reference_accounting(reference_keys: pd.Series, rebuilt_snapshot: pd.DataFrame) -> dict[str, Any]`.

- [ ] **Step 1: Record the implementation baseline and workspace state**

```powershell
Set-Location E:\investintell-datalake-workers-live-daily
git branch --show-current
$env:INVESTINTELL_PARITY_BASE = git rev-parse HEAD
$env:INVESTINTELL_PARITY_BASE
git status --short
```

Expected: branch `feat/bond-panel-pack-live`. The printed baseline is the plan
commit; preserve any reported dirty paths.

- [ ] **Step 2: Replace one-row fixtures with deterministic cohort fixtures**

Add `numpy` and `pytest`, retain pandas, and replace the helpers with:

```python
import numpy as np
import pandas as pd
import pytest


def _cusips(n: int, *, offset: int = 0) -> list[str]:
    return [f"{offset + index:09d}" for index in range(n)]


def _snapshot(
    month: pd.Timestamp,
    *,
    n: int | None = None,
    offset: int = 0,
    ytm: float = 0.05,
    mod_dur: float = 4.0,
    eligibility_state: str = "included",
    eligibility_reason: object = "eligible",
) -> pd.DataFrame:
    size = parity.MIN_MONTH_ROWS if n is None else n
    cusips = _cusips(size, offset=offset)
    return pd.DataFrame({
        "cusip_id": cusips,
        "month": month,
        "issuer_id": [f"ISSUER-{cusip}" for cusip in cusips],
        "eligibility_state": eligibility_state,
        "eligibility_reason": eligibility_reason,
        "ytm": ytm,
        "mod_dur": mod_dur,
        "maturity_years": 4.0,
        "bond_maturity": 4.0,
        "spread_final": 0.01,
        "spread_final_bps": 100.0,
        "spread_definition": parity.SPREAD_DEFINITION,
        "source_lineage": [
            {"daily_observations": "bond_observation_daily"}
            for _ in cusips
        ],
    })


def _rv(
    month: pd.Timestamp,
    *,
    n: int | None = None,
    offset: int = 0,
    signal_shift: float = 0.0,
) -> pd.DataFrame:
    size = parity.MIN_MONTH_ROWS if n is None else n
    raw = np.arange(size, dtype=float)
    signal = (raw - raw.mean()) / raw.std(ddof=0)
    return pd.DataFrame({
        "cusip_id": _cusips(size, offset=offset),
        "month": month,
        "spread_bps": 100.0,
        "fitted_bps": 100.0 - signal,
        "residual_bps": signal,
        "rv_signal": signal + signal_shift,
        "spread_definition": parity.SPREAD_DEFINITION,
        "source_lineage": [
            {"daily_observations": "bond_observation_daily"}
            for _ in range(size)
        ],
    })


def _fit_diagnostics(
    month: pd.Timestamp,
    *,
    n: int | None = None,
    skipped: bool = False,
) -> pd.DataFrame:
    size = parity.MIN_MONTH_ROWS if n is None else n
    return pd.DataFrame({
        "month": [month],
        "n": [size],
        "r2": [0.5],
        "max_vif_continuous": [1.0],
        "skipped": [skipped],
    })
```

Tests intentionally using a small cohort must pass `n=10`; never use `_rv(..., n=1)` because a one-row z-score is undefined.

- [ ] **Step 3: Add failing exact-accounting tests**

Add tests that assert:

```python
def test_reference_accounting_accepts_exact_snapshot_with_typed_exclusion() -> None:
    month = pd.Timestamp("2025-01-01")
    included = _snapshot(month, n=2)
    excluded = _snapshot(
        month,
        n=1,
        offset=2,
        eligibility_state="excluded",
        eligibility_reason="illiquid",
    )
    rebuilt = pd.concat([included, excluded], ignore_index=True)

    result = parity._reference_accounting(
        pd.Series([" 000000000 ", "000000001", "000000002"]),
        rebuilt,
    )

    assert result["passed"] is True
    assert result["reference_size"] == 3
    assert result["included_size"] == 2
    assert result["excluded_size"] == 1
    assert result["exclusion_counts"] == {"illiquid": 1}


@pytest.mark.parametrize(
    ("keys", "gate"),
    [
        (pd.Series(["000000000", pd.NA]), "reference_keys_valid"),
        (pd.Series(["000000000", "   "]), "reference_keys_valid"),
        (pd.Series(["000000000", " 000000000 "]), "reference_keys_unique"),
    ],
)
def test_reference_accounting_rejects_invalid_source(
    keys: pd.Series,
    gate: str,
) -> None:
    result = parity._reference_accounting(
        keys,
        _snapshot(pd.Timestamp("2025-01-01"), n=1),
    )
    assert result["passed"] is False
    assert result["gates"][gate] is False
```

Add separate assertions for missing/unexpected rebuilt keys, duplicate/null/blank rebuilt keys, state outside `{included, excluded}`, excluded blank reason, missing `issuer_id` column, and null/blank included `issuer_id`.

- [ ] **Step 4: Run the new tests red**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k reference_accounting
```

Expected: FAIL because `_reference_accounting` is absent.

- [ ] **Step 5: Implement normalized keys and reference accounting**

Import `MIN_MONTH_ROWS` from `src.bonds.panel_resolvers` so tests can use `parity.MIN_MONTH_ROWS`. Add:

```python
RECOGNIZED_ELIGIBILITY_STATES = frozenset({"included", "excluded"})


def _normalized_keys(values: pd.Series) -> pd.Series:
    normalized = values.astype("string").str.strip().str.upper()
    return normalized.mask(normalized.eq(""))


def _reference_accounting(
    reference_keys: pd.Series,
    rebuilt_snapshot: pd.DataFrame,
) -> dict[str, Any]:
    reference = _normalized_keys(reference_keys)
    rebuilt = (
        _normalized_keys(rebuilt_snapshot["cusip_id"])
        if "cusip_id" in rebuilt_snapshot
        else pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string")
    )
    states = rebuilt_snapshot.get(
        "eligibility_state",
        pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string"),
    ).astype("string")
    reasons = rebuilt_snapshot.get(
        "eligibility_reason",
        pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string"),
    ).astype("string").str.strip()
    identities = rebuilt_snapshot.get(
        "issuer_id",
        pd.Series(pd.NA, index=rebuilt_snapshot.index, dtype="string"),
    ).astype("string").str.strip()

    valid_reference = reference.dropna()
    valid_rebuilt = rebuilt.dropna()
    reference_set = set(valid_reference.tolist())
    rebuilt_set = set(valid_rebuilt.tolist())
    included = states.eq("included")
    excluded = states.eq("excluded")
    typed_exclusions = (~excluded) | (reasons.notna() & reasons.ne(""))
    identified_included = (~included) | (identities.notna() & identities.ne(""))
    gates = {
        "reference_nonempty": bool(len(valid_reference)),
        "reference_keys_valid": bool(reference.notna().all()),
        "reference_keys_unique": bool(not valid_reference.duplicated().any()),
        "rebuilt_keys_valid": bool(rebuilt.notna().all()),
        "rebuilt_keys_unique": bool(not valid_rebuilt.duplicated().any()),
        "exact_reference_key_set": reference_set == rebuilt_set,
        "eligibility_states_recognized": bool(
            states.notna().all()
            and states.isin(RECOGNIZED_ELIGIBILITY_STATES).all()
        ),
        "excluded_reasons_typed": bool(typed_exclusions.all()),
        "included_identity_present": bool(identified_included.all()),
    }
    exclusion_counts = {
        str(reason): int(count)
        for reason, count in reasons.loc[excluded & reasons.notna() & reasons.ne("")]
        .value_counts()
        .sort_index()
        .items()
    }
    return {
        "passed": bool(all(gates.values())),
        "gates": gates,
        "reference_source_rows": int(len(reference)),
        "reference_size": int(len(reference_set)),
        "rebuilt_size": int(len(rebuilt)),
        "included_size": int(included.sum()),
        "excluded_size": int(excluded.sum()),
        "exclusion_counts": exclusion_counts,
        "invalid_reference_key_rows": int(reference.isna().sum()),
        "invalid_rebuilt_key_rows": int(rebuilt.isna().sum()),
        "duplicate_reference_key_rows": int(valid_reference.duplicated(keep=False).sum()),
        "duplicate_rebuilt_key_rows": int(valid_rebuilt.duplicated(keep=False).sum()),
        "missing_reference_key_count": int(len(reference_set - rebuilt_set)),
        "unexpected_rebuilt_key_count": int(len(rebuilt_set - reference_set)),
        "missing_reference_keys": sorted(reference_set - rebuilt_set)[:50],
        "unexpected_rebuilt_keys": sorted(rebuilt_set - reference_set)[:50],
    }
```

- [ ] **Step 6: Run accounting tests and full focused file green**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k reference_accounting
python -m pytest tests/test_bond_panel_parity_worker.py -q
```

Expected: accounting tests pass; update only stale fixture assumptions until the current suite passes without weakening production assertions.

- [ ] **Step 7: Commit the accounting unit**

```powershell
git add src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
git commit -m "feat: enforce bond reference accounting"
```

---

### Task 2: Expose reference and fit evidence from the rebuild

**Files:**
- Modify: `src/workers/bond_panel_parity.py:127-182`
- Modify: `tests/test_bond_panel_parity_worker.py`, exact-clock orchestration section

**Interfaces:**
- `_rebuild_month` retains its first six return elements and appends `pd.Series`
  reference keys and `pd.DataFrame` fit diagnostics without changing the Stage 6
  construction inputs.

- [ ] **Step 1: Add a failing rebuild-interface test**

Monkeypatch `_load_inputs` with `resolved_issuer_sector` containing representative
`cusip9` keys, and patch `fit_all_months` to return structurally valid signals plus
`_fit_diagnostics`. Assert eight returned values, reference keys preserved, the
original `resolved_issuer_sector` frame passed unchanged to the builder,
`residual_bps` retained, and diagnostic `n == len(rebuilt_rv)`.

- [ ] **Step 2: Run the interface test red**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k rebuild_exposes_reference_and_fit_evidence
```

Expected: FAIL because `_rebuild_month` returns six values and discards diagnostics.

- [ ] **Step 3: Preserve reference evidence without changing construction**

Immediately after `_load_inputs`:

```python
reference_frame = inputs["resolved_issuer_sector"].copy()
reference_column = next(
    (name for name in ("cusip9", "cusip_id") if name in reference_frame),
    None,
)
if reference_column is None:
    raise ValueError("reference_cusip_column_missing")
reference_keys = reference_frame[reference_column].copy()
```

`_reference_accounting` performs normalization only for validation and reporting.
Nulls, blanks, duplicates, or unexpected keys force a failed month; the parity
worker must never repair them silently or feed altered inputs to the formula path.

- [ ] **Step 4: Retain signals and diagnostics**

Replace the discarded diagnostics with:

```python
signals, fit_diagnostics = fit_all_months(included, as_of=month)
if not signals.empty:
    signals = signals.merge(
        included,
        on=["cusip_id", "month"],
        how="inner",
        validate="one_to_one",
        suffixes=("", "_snapshot"),
    )
```

Return the existing six elements followed by `reference_keys, fit_diagnostics`. Keep `max_day` typed as `date | None` and `input_exclusions` as `dict[str, int]`.

- [ ] **Step 5: Update `run` unpacking without changing verdict logic yet**

Temporarily unpack the two new values as `_reference_keys` and `_fit_diagnostics`
until Task 5 connects the redesigned comparison. The underscore names keep the
intermediate commit runnable and lint-clean.

- [ ] **Step 6: Run exact-clock and full tests green**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k "rebuild_exposes or exact_clock"
python -m pytest tests/test_bond_panel_parity_worker.py -q
```

Expected: pass; existing read-only SQL, `2025-01`/`2026-06` month clock, and future-rating evidence remain unchanged.

- [ ] **Step 7: Commit the rebuild interface**

```powershell
git add src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
git commit -m "feat: expose bond parity rebuild evidence"
```

---

### Task 3: Validate rebuilt RV structure independently of frozen RV

**Files:**
- Modify: `src/workers/bond_panel_parity.py`, pure-helper area
- Modify: `tests/test_bond_panel_parity_worker.py`, RV helper tests

**Interfaces:**
- Produces `_rv_structure(rebuilt_rv, rebuilt_included, fit_diagnostics, month) -> dict[str, Any]`.

- [ ] **Step 1: Write failing RV-structure tests**

Cover valid 300-row output and failures for empty output, absent required column,
nonfinite `rv_signal` or `residual_bps`, null/blank/duplicate key, key outside
rebuilt included cohort, wrong/null RV month, diagnostics absent/multiple/wrong
month, `skipped=true`, missing or nonintegral `n`, count mismatch, off-center
signal, and nonunit population standard deviation.

Representative assertions:

```python
def test_rv_structure_accepts_finite_standardized_fit() -> None:
    month = pd.Timestamp("2025-01-01")
    result = parity._rv_structure(
        _rv(month),
        _snapshot(month),
        _fit_diagnostics(month),
        month,
    )
    assert result["passed"] is True
    assert result["fit_row_count"] == parity.MIN_MONTH_ROWS
    assert abs(result["rv_mean"]) <= parity.RV_MEAN_TOLERANCE
    assert abs(result["rv_population_std"] - 1) <= parity.RV_STD_TOLERANCE
```

- [ ] **Step 2: Run RV tests red**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k rv_structure
```

Expected: FAIL because `_rv_structure` is absent.

- [ ] **Step 3: Implement structural validation**

Add `import numpy as np` and:

```python
RV_MEAN_TOLERANCE = 1e-10
RV_STD_TOLERANCE = 1e-10


def _rv_structure(
    rebuilt_rv: pd.DataFrame,
    rebuilt_included: pd.DataFrame,
    fit_diagnostics: pd.DataFrame,
    month: pd.Timestamp,
) -> dict[str, Any]:
    required = {"cusip_id", "month", "rv_signal", "residual_bps"}
    required_present = required.issubset(rebuilt_rv.columns)
    rv_keys = (
        _normalized_keys(rebuilt_rv["cusip_id"])
        if "cusip_id" in rebuilt_rv
        else pd.Series(pd.NA, index=rebuilt_rv.index, dtype="string")
    )
    included_keys = set(
        _normalized_keys(rebuilt_included["cusip_id"]).dropna().tolist()
    )
    diagnostic_rows = (
        fit_diagnostics.loc[pd.to_datetime(fit_diagnostics["month"]).eq(month)]
        if "month" in fit_diagnostics
        else fit_diagnostics.iloc[0:0]
    )
    raw_fit_count = (
        pd.to_numeric(
            pd.Series([diagnostic_rows.iloc[0]["n"]]),
            errors="coerce",
        ).iloc[0]
        if len(diagnostic_rows) == 1 and "n" in diagnostic_rows
        else np.nan
    )
    fit_count_valid = bool(
        pd.notna(raw_fit_count)
        and np.isfinite(float(raw_fit_count))
        and float(raw_fit_count).is_integer()
        and float(raw_fit_count) >= 0
    )
    skipped_value = (
        diagnostic_rows.iloc[0]["skipped"]
        if len(diagnostic_rows) == 1 and "skipped" in diagnostic_rows
        else None
    )
    diagnostic_valid = (
        len(diagnostic_rows) == 1
        and fit_count_valid
        and isinstance(skipped_value, (bool, np.bool_))
        and not bool(skipped_value)
    )
    fit_count = int(raw_fit_count) if diagnostic_valid else None
    numeric = (
        rebuilt_rv[["rv_signal", "residual_bps"]]
        .apply(pd.to_numeric, errors="coerce")
        if required_present
        else pd.DataFrame()
    )
    finite = bool(
        required_present
        and len(numeric) == len(rebuilt_rv)
        and np.isfinite(numeric.to_numpy(dtype=float)).all()
    )
    rv_mean = float(numeric["rv_signal"].mean()) if finite and len(numeric) else None
    rv_std = float(numeric["rv_signal"].std(ddof=0)) if finite and len(numeric) else None
    gates = {
        "rebuilt_rv_nonempty": bool(len(rebuilt_rv)),
        "required_columns_present": required_present,
        "rv_keys_valid": bool(rv_keys.notna().all()),
        "rv_keys_unique": bool(not rv_keys.dropna().duplicated().any()),
        "rv_keys_subset_of_included": set(rv_keys.dropna()).issubset(included_keys),
        "rv_month_exact": bool(
            required_present
            and pd.to_datetime(rebuilt_rv["month"], errors="coerce")
            .eq(month)
            .all()
        ),
        "fit_diagnostics_valid": diagnostic_valid,
        "row_count_matches_fit": fit_count == len(rebuilt_rv),
        "rv_values_finite": finite,
        "rv_mean_centered": rv_mean is not None and abs(rv_mean) <= RV_MEAN_TOLERANCE,
        "rv_population_std_unit": rv_std is not None and abs(rv_std - 1) <= RV_STD_TOLERANCE,
    }
    return {
        "passed": bool(all(gates.values())),
        "gates": gates,
        "row_count": int(len(rebuilt_rv)),
        "fit_row_count": fit_count,
        "included_row_count": int(len(rebuilt_included)),
        "rv_mean": rv_mean,
        "rv_population_std": rv_std,
    }
```

- [ ] **Step 4: Run RV and full tests green**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k rv_structure
python -m pytest tests/test_bond_panel_parity_worker.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit RV validation**

```powershell
git add src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
git commit -m "feat: validate rebuilt bond RV structure"
```

---

### Task 4: Split monthly accounting, comparability, formula, and diagnostics

**Files:**
- Modify: `src/workers/bond_panel_parity.py:241-381`
- Modify: `tests/test_bond_panel_parity_worker.py`, comparison tests

**Interfaces:**
- Extend `_compare_month` with keyword-only `reference_keys: pd.Series` and `fit_diagnostics: pd.DataFrame`; preserve all existing parameter names and pointwise metric keys.

- [ ] **Step 1: Add a comparison wrapper in tests**

```python
def _compare_fixture(
    month: pd.Timestamp,
    *,
    frozen_snapshot: pd.DataFrame | None = None,
    frozen_rv: pd.DataFrame | None = None,
    rebuilt_snapshot: pd.DataFrame | None = None,
    rebuilt_rv: pd.DataFrame | None = None,
    reference_keys: pd.Series | None = None,
    fit_diagnostics: pd.DataFrame | None = None,
) -> dict[str, object]:
    rebuilt = _snapshot(month) if rebuilt_snapshot is None else rebuilt_snapshot
    return parity._compare_month(
        month,
        _snapshot(month) if frozen_snapshot is None else frozen_snapshot,
        _rv(month) if frozen_rv is None else frozen_rv,
        rebuilt,
        _rv(month) if rebuilt_rv is None else rebuilt_rv,
        input_max_day=parity._month_end(month),
        fit_as_of=month,
        monthly_curve=_curve(month),
        reference_keys=(
            rebuilt["cusip_id"] if reference_keys is None else reference_keys
        ),
        fit_diagnostics=(
            _fit_diagnostics(month)
            if fit_diagnostics is None
            else fit_diagnostics
        ),
    )
```

- [ ] **Step 2: Write failing state and diagnostic tests**

Add tests proving:

- exact 300-row rebuild passes all three contracts;
- frozen 350 versus rebuilt 300 with 300 common passes and reports 50 symmetric-difference keys;
- zero or fewer than 300 common keys yields `parity_not_comparable`, `aborted=false`, and no formula evaluation;
- missing reference key or hard gate failure still yields `parity_failed`, `aborted=true` even when noncomparable;
- old `universe_delta`, RV size, and overlap values remain diagnostics only;
- an empty frozen RV surface remains diagnostic and does not block when rebuilt
  RV structure and every other contract pass;
- each unchanged formula gate (`ytm_abs_bps`, `duration_abs_years`, `duration_relative`, `spread_abs_bps`) still blocks a comparable month;
- spread semantics and month-end walk-forward still block;
- a large shift only in frozen `rv_signal` passes and reports its median/p90/p99 under RV diagnostics;
- each rebuilt RV structural failure blocks a comparable month.

- [ ] **Step 3: Run comparison tests red**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k "compare or comparable or membership_drift or formula_gate or rv_drift"
```

Expected: FAIL on the old universe/RV blockers and absent new arguments/state.

- [ ] **Step 4: Evaluate accounting and comparability before formula metrics**

Inside `_compare_month`, remove the zero-overlap and frozen-RV-empty immediate
returns. Build `frozen_included`, `rebuilt_included`, and a validated one-to-one
common merge. Set:

```python
reference_accounting = _reference_accounting(reference_keys, rebuilt_snapshot)
matched_bonds = int(len(common_snapshot))
comparable = matched_bonds >= MIN_MONTH_ROWS
```

Reference accounting is always blocking.

- [ ] **Step 5: Preserve all existing hard and pointwise gates**

Keep the current exact checks and specifically retain:

```python
walk_forward_ok = (
    input_max_day is not None
    and input_max_day <= _month_end(month)
    and fit_as_of == month
)
```

On the common cohort and only when comparable, run the unchanged limits for:

```python
ytm_abs_bps
duration_abs_years
duration_relative
spread_abs_bps
```

Do not compare the input date with the first day of the month.

- [ ] **Step 6: Make membership and absolute RV deltas diagnostic**

Return frozen/rebuilt included sizes, common size, both zero-safe overlap ratios,
symmetric-difference size, and universe delta under `diagnostics`. Return
frozen/rebuilt RV sizes, common size/ratios, and the existing `_quantile_gate`
median/p90/p99 for absolute RV under `diagnostics["rv_abs"]`; ignore its boolean.
If frozen RV is empty, emit null quantiles plus an explicit unavailable reason.
Validate frozen RV columns and lineage when rows exist, but do not make the
absence of a historical comparison surface a blocker.

- [ ] **Step 7: Apply exact monthly-state precedence**

```python
blocking_failure = (
    not reference_accounting["passed"]
    or not all(hard_gates.values())
    or (
        comparable
        and (
            formula_parity["passed"] is not True
            or not rv_structure["passed"]
        )
    )
)
if blocking_failure:
    state, aborted = "parity_failed", True
elif comparable:
    state, aborted = "parity_passed", False
else:
    state, aborted = "parity_not_comparable", False
```

For a noncomparable month, `formula_parity.evaluated=false` and RV structure is evidence only. Populate `failed_gates` only with blocking false gates; never list historical membership or RV-absolute diagnostics there.

- [ ] **Step 8: Run comparison and full tests green**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k "compare or comparable or membership_drift or formula_gate or rv_drift"
python -m pytest tests/test_bond_panel_parity_worker.py -q
```

Expected: pass with all original hard-gate tests retained.

- [ ] **Step 9: Commit monthly comparison**

```powershell
git add src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
git commit -m "feat: split monthly bond parity contracts"
```

---

### Task 5: Aggregate monthly states and connect `run`

**Files:**
- Modify: `src/workers/bond_panel_parity.py:384-430`
- Modify: `tests/test_bond_panel_parity_worker.py`, orchestration tests

**Interfaces:**
- Produces `_overall_verdict(month_results: list[dict[str, Any]]) -> dict[str, Any]`.

- [ ] **Step 1: Add failing overall-verdict tests**

Cover:

```python
def test_overall_passes_one_noncomparable_and_one_comparable() -> None:
    result = parity._overall_verdict([
        {"state": "parity_not_comparable", "comparable": False},
        {"state": "parity_passed", "comparable": True},
    ])
    assert result["state"] == "parity_passed"
    assert result["aborted"] is False


def test_overall_fails_without_comparable_month() -> None:
    result = parity._overall_verdict([
        {"state": "parity_not_comparable", "comparable": False},
        {"state": "parity_not_comparable", "comparable": False},
    ])
    assert result["state"] == "parity_failed"
    assert result["reason"] == "no_comparable_month"
    assert result["aborted"] is True
```

Add a third test proving any `parity_failed` month blocks even when another comparable month passed.

- [ ] **Step 2: Run overall tests red**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k overall_
```

Expected: FAIL because `_overall_verdict` is absent.

- [ ] **Step 3: Implement the pure aggregator**

Count failed, comparable-passed, and noncomparable months. Gates are `all_months_nonblocking`, `at_least_one_comparable_month`, and `all_comparable_months_passed`. Return top-level `state`, `reason`, `aborted`, counts, gate booleans, and failure reasons. Precedence: any failed month gives `monthly_parity_failure`; otherwise no comparable month gives `no_comparable_month`.

- [ ] **Step 4: Connect the new rebuild and comparison arguments in `run`**

Unpack all eight rebuild values and call `_compare_month` with `reference_keys` and `fit_diagnostics`. After both months:

```python
overall = _overall_verdict(results)
return {**overall, "months": results}
```

Preserve `_failure(...)` for config/publication/fingerprint/status/rebuild fail-fast paths and its exact `aborted=true` behavior.

- [ ] **Step 5: Update exact-clock orchestration**

Make the fake rebuilds produce one noncomparable and one comparable pass in one test, and two comparable passes in the read-only happy path. Assert `SET TRANSACTION READ ONLY`, no write statement, exact frozen publication queries, future static-rating evidence, top-level `aborted=false`, and `overall` counts.

- [ ] **Step 6: Run hard-gate, overall, and full tests green**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q -k "overall_ or config or publication or fingerprint or status or lineage or exact_clock"
python -m pytest tests/test_bond_panel_parity_worker.py -q
```

Expected: pass; config mismatch still refuses before connecting.

- [ ] **Step 7: Commit aggregation**

```powershell
git add src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
git commit -m "feat: aggregate comparable bond parity months"
```

---

### Task 6: Document the revised contract without rewriting history

**Files:**
- Modify: `docs/runbooks/bond-live-daily.md:223-241`
- Modify: `docs/calibration/bond_panel_pack_live_evidence_001.md`, append after current production evidence

**Interfaces:**
- Operator contract for a separately authorized future run.

- [ ] **Step 1: Revise the runbook's T3 section**

Document:

```markdown
### T3 parity gate contracts

1. Reference accounting requires every normalized CUSIP9 from
   `bond_curated_universe` exactly once as included or typed excluded.
2. Formula parity compares YTM, duration, duration-relative, and spread only on
   at least 300 common included bonds. Historical membership drift is diagnostic.
3. Rebuilt RV is validated structurally. Absolute cross-cohort RV deltas are
   diagnostic because each monthly cohort is fit and standardized separately.
```

Define all three monthly states, the at-least-one-comparable overall rule, and the separate production authorization boundary.

- [ ] **Step 2: Append, never replace, calibration evidence**

Append a dated section titled `Gate redesign adopted on 2026-08-08; production rerun pending`. State that the earlier no-go and literal JSON remain immutable evidence from the original gate; no new production JSON, deploy, DB write, pointer move, or Stage 6 run occurred.

- [ ] **Step 3: Inspect historical/current language**

```powershell
git diff -- docs/runbooks/bond-live-daily.md docs/calibration/bond_panel_pack_live_evidence_001.md
rg -n "99%|0.5%|RV-signal absolute|parity_not_comparable|production rerun pending" docs/runbooks/bond-live-daily.md docs/calibration/bond_panel_pack_live_evidence_001.md
```

Expected: old thresholds appear only as historical evidence; revised operational language contains no membership or absolute-RV blocker.

- [ ] **Step 4: Commit documentation**

```powershell
git add docs/runbooks/bond-live-daily.md docs/calibration/bond_panel_pack_live_evidence_001.md
git commit -m "docs: define redesigned bond parity gate"
```

---

### Task 7: Final verification and scope audit

**Files:**
- Verify: all four implementation paths above

**Interfaces:**
- Produces local, non-production acceptance evidence.

- [ ] **Step 1: Run the full focused suite**

```powershell
python -m pytest tests/test_bond_panel_parity_worker.py -q
```

Expected: all pass, covering accounting, noncomparability, all formula metrics, RV structure, hard gates, and aggregation.

- [ ] **Step 2: Lint only changed Python files**

```powershell
ruff check src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
```

Expected: pass. Do not run or claim global Ruff cleanliness.

- [ ] **Step 3: Compile changed Python files**

```powershell
python -m compileall -q src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
```

Expected: exit 0.

- [ ] **Step 4: Verify diff integrity and exact scope**

```powershell
git diff --check $env:INVESTINTELL_PARITY_BASE..HEAD
git diff --name-only $env:INVESTINTELL_PARITY_BASE..HEAD
git status --short
```

Expected changed implementation paths:

```text
docs/calibration/bond_panel_pack_live_evidence_001.md
docs/runbooks/bond-live-daily.md
src/workers/bond_panel_parity.py
tests/test_bond_panel_parity_worker.py
```

The plan document itself predates `INVESTINTELL_PARITY_BASE` and therefore does
not pollute this audit.

- [ ] **Step 5: Review the final behavior diff**

```powershell
git diff $env:INVESTINTELL_PARITY_BASE..HEAD -- src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
```

Confirm no reference bond disappears silently; included identity is required; typed exclusions are exact; fewer than 300 common keys is noncomparable; at least one comparable month is required; all four formula metrics remain blocking; absolute RV is diagnostic; malformed rebuilt RV blocks; all original hard gates remain fail-closed.

- [ ] **Step 6: Commit only if verification required a correction**

```powershell
git add src/workers/bond_panel_parity.py tests/test_bond_panel_parity_worker.py
git commit -m "fix: complete bond parity gate verification"
```

Do not create an empty commit. If a correction was committed, repeat pytest, Ruff, compileall, diff-check, and scope audit.
