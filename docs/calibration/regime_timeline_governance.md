# Regime timeline governance (open_macro_v03)

Status: **ratified + active in code** — the timeline gate policy
(`artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.json`) was **ratified
by the quant_owner (Andrei Rachadel) on 2026-07-11** with the bounds exactly as
proposed, and `carry_decay_v1` ships **ACTIVE** (`CARRY_DECAY_V1_ACTIVE = True`) in the
Stage B worker publish path. The remaining switches are **ops steps, not approvals**:
the orchestrator applies the additive migration DDL to the production DB and redeploys
the worker (Railway). No frozen model parameter was changed and nothing here was
self-ratified (the ratification was ordered by the owner and recorded per the
phase0q_003 convention).

This note records the Tranche W hardening of the open_macro_v03 regime system: what the
regime audit found, the `carry_decay_v1` carry policy, the ratified blocking timeline
gates, the ratification record, and the momentum-semantics disclosure that must
accompany every investor-facing regime label.

**Expectation set by the ratified gates:** the frozen v1 model FAILS them on the
certified 2021-2026 timeline, so official phase0q runs will report a **timeline
`no_go`** in `gates_overall_base_cost` until a recalibrated candidate passes review.
That no_go is the intended honest outcome — not a defect, and never a crash.

## 1. The gap the audit found

The model works exactly as coded and its materialization is faithful; the defect is in
what the acceptance machinery rewarded.

- **Abstention became non-gating.** `artifacts/quant/open_macro_v03_phase0q_003/stress_gate_semantics_amendment.json`
  (ratified 2026-07-02) correctly stopped penalizing *intended* per-window abstention by
  making `consumable_position_coverage` the blocking stress metric — carry-forward of the
  last valid decision counts as covered. But it reclassified `fresh_decision_rate` /
  `abstention_rate` (and, implicitly, carry age and single-quadrant occupancy) as
  `diagnostics_not_gating`.
- **The envelope is risk-only.** `BASE_ENVELOPE` in `harness/phase0q/runner.py` judges
  turnover / MDD / vol / worst-5d / fold stability; `no_return_target: true`. A defensive
  book carried for years has low vol, low MDD, ~0 turnover and perfect fold stability.
- **Joint effect: the pathology is the optimum of the envelope.** A chain that abstained
  43 of 66 months (2021-2026) and carried a single **2023-02 contraction** book forward
  indefinitely reported 100% consumable coverage and passed every risk gate. The regime
  the strategy was actually positioned in was invisible to every reported number.

### Measured evidence (this tranche)

Replaying the certified input pack v2 through the frozen decision engine
(`tests/test_regime_timeline_golden.py`, 2021-01..2026-06) and the recalibration smoke
cell (`scripts/regime_recalibration_experiment.py`, default cell 10y / 0.70):

| metric | value |
|---|---|
| valid / low_confidence months (66 total) | 23 / 43 |
| fresh-valid rate, global | 0.348 |
| fresh-valid rate, trailing 36m | **0.194** |
| max abstention streak | **18 months** (2023-03..2024-08) |
| max carry age | **18 months** |
| max same-quadrant run (consumable) | **38 months** (the 2023→2026 contraction anchor) |
| quadrant mix (valid) | recovery 4, expansion 12, slowdown 1, contraction 6 |

## 2. `carry_decay_v1` — bounded carry (ACTIVE)

`harness/direct_activation/carry_decay.py` (pure, non-pinned, **`CARRY_DECAY_V1_ACTIVE
= True`** since the 2026-07-11 ratification).

- A seed book is consumable for at most **`MAX_CARRY_MONTHS = 3`** monthly decision
  points. `carry_age_months` is the **calendar-month** distance from `carry_seed_as_of`
  to the as-of (not a row count), so a chain gap ages the carry naturally.
- Ages 1..3 carry the seed `compressed_50` book. Age **> 3** degrades to the mandate-
  tilted **CENTER book** — the cross-quadrant mean of the four `compressed_50` books, run
  through the sleeve risk-cap / defensive-floor machinery — with `carry_expired = true`.
  Degraded months keep being re-evaluated monthly; a **fresh valid decision resets the age
  to 0**.
- **Publish path (Stage B worker):** an expired carry publishes
  `decision_validity = 'carried_expired'` (seed quadrant preserved as reference), the
  allocation `book = 'center_50'`, and provenance columns `carry_age_months` /
  `carry_expired` (+ `carry_seed_as_of` on allocations) on both rows. The DB surface is
  the **additive** migration `schemas/open_macro_v03_carry_decay_v1_migration.sql`
  (widened `decision_validity` / `book` CHECKs + nullable provenance columns; old rows
  untouched; the three byte-pinned base DDL files are not edited).
- This mirrors the Light-repo backtest (`carry_decay_v1`) **exactly** — cross-repo
  fidelity is a hard requirement, the two repos must consume the same policy. The new
  DB vocabulary the Light repo needs: validity `'carried_expired'`, book `'center_50'`,
  columns `carry_age_months` / `carry_seed_as_of` / `carry_expired`.

## 3. Ratified blocking gates (phase0q_005)

`artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.json`
(**`status: ratified`, `ratified_by: quant_owner` (Andrei Rachadel),
`decision_date: 2026-07-11`** — bounds exactly as proposed).

| gate | bound | measured (default cell) |
|---|---|---|
| `min_fresh_valid_rate_36m` | ≥ 0.40 | 0.194 → **no_go** |
| `max_abstention_streak_months` | ≤ 6 | 18 → **no_go** |
| `max_carry_age_months` | ≤ 3 | 18 → **no_go** |
| `max_same_quadrant_run_months` | ≤ 12 | 38 → **no_go** |
| `min_upside_capture_bull_year` | ≥ 0.35 (SPY > +15% FULL calendar years) | reported per year |

`runner.judge_timeline_gates()` computes these on every run. With the ratified policy
the judgment is **gating** (`mode: gating`, `gates_enforced: true`) and lands as a
distinct blocking **`timeline`** entry in `gates_overall_base_cost` — a real go/no_go,
never a crash. It re-elevates `fresh_decision_rate` / `abstention_rate` / carry-age /
occupancy from `diagnostics_not_gating` back to blocking at the run level (an amendment
of phase0q_003; the per-window `consumable_position_coverage` gate stays). An
unratified policy remains advisory and never enters the overall gates (behaviour kept
under test). **Official phase0q runs will report timeline `no_go` until recalibration
lands** — by the table above, that is the honest state of the frozen v1 model.

## 4. Ratification record + remaining ops switches

1. **Ratification — DONE.** The quant_owner (Andrei Rachadel) ratified the policy on
   2026-07-11, bounds unchanged from the proposal; recorded in the artifact with the
   phase0q_003 field convention (`ratified_by` / `ratified_by_name` / `decision_date`).
   The self-ratification ban stands — the ratification was ordered by the owner, not
   initiated by the engineering side. The bounds govern abstention / flip / duration
   behaviour, **never** CAGR/Sharpe (the freeze rule; see the confidence docstring in
   `src/quadrant_confidence.py`).
2. **DB schema evolution — DDL COMMITTED, application is an ops step.** The additive
   migration `schemas/open_macro_v03_carry_decay_v1_migration.sql` is committed and
   idempotent; the **orchestrator applies it to the production DB in a controlled
   step** (it is deliberately NOT in the worker's `ensure_schema`). `EXPECTED_SCHEMA` /
   `verify_schema` expect the post-migration catalog, so an unmigrated DB fails loud
   (no writes) rather than accepting disguised rows.
3. **Deploy — ops step.** The worker (Railway service `open-macro-v03-worker`) must be
   redeployed with this code for the active carry policy to publish. Order-independent
   with (2): whichever lands first, the fail-loud gates prevent any inconsistent write.
4. **Pinned modules untouched.** The frozen decision chain (`consumable_today`,
   `decision.py`, `sleeve.py`, …) was not edited; the non-pinned worker composes
   `carry_decay` on top of the pinned seed selection, so no `module_pins.json`
   regeneration was needed.

There is **no remaining approval-shaped dependency**: everything still pending is an
operational switch (DDL application + redeploy).

## 5. Momentum-semantics disclosure (labels)

The investor-facing labels stay (Recovery / Expansion / Slowdown / Contraction), but
every surface that exposes a regime must disclose:

- **The axes are momentum, not level.** Growth = annualized log 3m/3m impulse; inflation
  = 3m−YoY impulse; each a robust z-score vs a 10-year baseline. "Contraction" means
  growth *and* inflation are **decelerating**, which is economically a disinflationary
  soft-landing — historically *bullish* for equities, not a recession call. The model
  mapped the most bullish disinflationary stretch of 2023-26 to its most defensive book.
- **Confidence is an abstention proxy, not a probability.** `candidate_confidence` is
  `Φ(|score| / u_adj)` — the statistical separation of the axis scores from zero, not
  "P(regime)". Calibrate it against abstention/flip/vintage-stability, never CAGR.
- No market feature (trend, credit, earnings) enters the classifier; it is a pure
  macro-momentum abstention engine.

## 6. What this tranche delivered

| item | where |
|---|---|
| W1 timeline metrics (always reported) | `harness/phase0q/metrics.py`, `harness/phase0q/runner.py` (`timeline` block) |
| W2 gate policy (ratified 2026-07-11) + gating judge | `artifacts/quant/open_macro_v03_phase0q_005/timeline_gate_policy.json`, `runner.judge_timeline_gates` + the blocking `timeline` overall gate |
| W3 `carry_decay_v1` (ACTIVE) + publish-path degradation | `harness/direct_activation/carry_decay.py`, `src/workers/open_macro_v03.py`, `schemas/open_macro_v03_carry_decay_v1_migration.sql` |
| W4 golden timeline replay | `tests/test_regime_timeline_golden.py` |
| W5 baseline-window regression + recalibration experiment | `tests/test_baseline_window_regression.py`, `scripts/regime_recalibration_experiment.py` |
| W6 this note | `docs/calibration/regime_timeline_governance.md` |

Frozen model parameters (confidence floor 0.70, axis weights, hysteresis, 10y robust
baseline) are unchanged. Recalibration is a future, separately-ratified experiment —
until it lands, official phase0q runs report the timeline `no_go` documented above.
