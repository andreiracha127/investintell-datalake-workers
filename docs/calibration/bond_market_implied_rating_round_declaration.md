# Declared research round — market-implied bond rating + exposure EL (v1)

Written **before the round is run**. The declaration fixes the policy, the data
split, the arms, the provenance and the acceptance gates **before any comparison
number exists**; every result cell below is deliberately empty and is filled
only by the run itself. The producer is the sibling datalake worker
(`E:\investintell-datalake-workers`): `src/bonds/implied_rating.py` (pure state
machine) plus `schemas/bond_market_implied_rating_v1.sql` and
`src/workers/bond_market_implied_rating.py` (publication).

Status: **declared, not run.** Phase 3 of the plan (the Light app consumer:
`bond_el_hazard_v1`, gate `implied_rating_current`, `/§8` rewrite, registry
v1.6) is **forbidden** until this predeclared round is executed, its result is
recorded append-only, and the owner records the **DG-4** decision in §8.

---

## 1. Why this round exists

The app's expected-loss gate currently estimates default frequency from
**static OSBAP ratings by adjacent rating pairs** (v1.5,
`expected_loss_by_bucket_adjacent`). That estimator cannot be validated against
the market: it is a restatement of the same static labels the cap already uses,
it has no exposure denominator, and its §8 "default capacity" is not a
statistical zero bound. The approved redesign replaces only the `expected_loss`
estimator — `rating_bucket` stays the source for the HY cap, the rating history
coverage and the feed-current gate — with:

1. a **market-implied, point-in-time monthly rating** (`AAA..CCC`, confirmed
   `D`, `WITHDRAWN`, `NOT_RATED`) built from the served bond-panel snapshot
   (price, executed spread, duration, traded prints) with a chained market
   level, hysteresis, carry-forward and default-episode confirmation; and
2. an **exposure estimator** (Jeffreys `(D+0.5)/(E+1)`, weighted PAVA across
   rated buckets, rule-of-three zero upper bound `3/E · LGD`, LGD = 0.60,
   `E_b · h_b ≥ 20` floor) fed by that rating, with censoring categories
   `source_exit`, `withdrawal`, `pending`, `window_end`.

The round exists to prove — on data frozen **before** the numbers are read —
that the implied state machine reproduces the static ordinal information it is
meant to substitute (τ, ±1 concordance, IG/HY AUC), that it discriminates
forward defaults (Gini, false-D), that it is usable by the solve universe at the
base date (gate G-U), and that its default-capacity evidence is real (positive
control). Nothing in this round selects or tunes a threshold.

## 2. The product and the frozen policy

- Product: `bond_market_implied_rating_v1` — grain `(publication_id, month,
  cusip_id)`, one full rebuild per publication over every candidate in
  `bond_panel_current_snapshot_v1_mat` with `month <=` the panel's last closed
  month. No delta publication; the pointer flip is the only "delta".
- Policy version: `bond_market_implied_rating_policy_v1`.
- **`POLICY_DIGEST` at declaration:**
  `28f70b9bd8f617fedf6104deb86cd518d43307e88b3bbd1fcf53aa1fae869a3b`
  (canonical sha256 over the version + the parameter mapping; every published
  row and every build carries it; it participates in the publication identity
  `uuid5(product | policy_version | policy_digest | code_revision |
  input_fingerprint)`). Superseded digest
  `a23fd5115cd66abf54986b5189724d1f9497c3f8594c7ecf35855c5ca256983b`
  (hard-price confirmations before the owner decision below) is dead: any
  publication carrying it is pre-decision evidence and must not be consumed.

| Block | Frozen parameters |
| --- | --- |
| `witness` | `n_min=3` prints, `v_min_usd=250000`, `price ∈ [1, 200]`, `mod_dur ≥ 0.5`; a witnessed month must also carry computable `spread_final_bps` and `mod_dur` |
| `spread` | winsor `[5, 5000]` bps, `d_ref=5`, duration slope `b=0.2`: `s = log(winsor(spread)) − b·(log(mod_dur) − log d_ref)` |
| `market_level` | chained `L_t = L_{t−1} + median_{W_t ∩ W_{t−1}}(s_t − s_{t−1})`; neutralization `x = s − β·(L_t − L_anchor)` with `β=0.6`; anchor = **median of `L` over the calibration window** `[2023-09, 2026-08]` (`window_end_month=2026-08-01`, `window_months=36`). Both layers are published: `spread_norm_log = s`, `neutralized_score = x`; the state machine consumes `x`, and an audit can recompute either from `market_level_l`, `β` and the pinned anchor |
| `cuts_bps` | `60, 85, 125, 220, 380, 700` applied in log space (AAA..CCC) |
| `hysteresis` | `δ_log=0.10` **or** 2 observations of the target side within `h=3` months; carried months are not observations |
| `carry_forward_k` | `3` carried months after the last witness, then `WITHDRAWN` (absorbing within the spell) |
| `source_exit` | spell closes at the last witness when `maturity_date − month ≤ 1` or `price ≥ 97` |
| `default` | candidate: `price ≤ 50 ∧ spread ≥ 2000` bps at a witnessed month; confirmation: second observation within `h_D=3`; **`hard_price_confirmation="standalone_immediate"` (owner decision, 2026-09-18): `price ≤ 35` confirms `D` in that same month, standalone — no active/recent candidate and no spread condition required, `d_event_month` = that month**; absorbing `D`; cure: `price ≥ 80` for `n_cure=3` consecutive witnessed months → new spell |
| `calibration` | `t_c=36`, `holdout=24`, `lambda_floor=20`, `m_max=0.05`, `phi_max=0.25` |
| buckets | `AAA, AA, A, BBB, BB, B, CCC, D, WITHDRAWN, NOT_RATED` |

Mechanics also frozen (already implemented in `src/bonds/implied_rating.py`,
covered by its unit tests):

- **Strict PIT (plan D2).** `implied_bucket` of a month never changes when the
  series is truncated after that month; only `d_confirmed`/`d_event_month` (and
  the `recovery_observed` of the event row) may be revised when a later month
  confirms the episode. The default event is **dated at the candidate month**
  with the **origin bucket = the prior month's implied bucket**.
- **`L_anchor` pinning.** The committed policy carries `l_anchor: null`. The
  worker resolves the frozen-window median once per build, pins it on the
  publication (`..._builds.l_anchor`) and refuses a build whose re-resolution
  differs from the current pin (`anchor_drift`). This round **pins the measured
  value into the policy**; that is a deliberate policy event that mints a new
  `POLICY_DIGEST` and requires a re-publication, never a silent retune.
- **Publication protocol.** Shared derived-publication ledger
  (`sec_derived_publications` / `sec_derived_current_pointers`), build pin
  `bond_market_implied_rating_v1_builds`, insert-only rows for `prepared`
  publications, compare-and-set pointer, immutable snapshot — the real sibling
  `bond_metrics`/`bond_serving` pattern; the plan's `*_publications` /
  `*_app_pointer` read contract is preserved as read-only views.
- **Revision ladder (verbatim from the fleet):** `CODE_REVISION`, `GIT_SHA`,
  `SOURCE_COMMIT`, `RAILWAY_GIT_COMMIT_SHA`, then git; an unresolvable revision
  refuses to publish instead of stamping `unknown`.
- **`d_candidate` is per-month.** It is published exactly on the witnessed months
  whose `price ≤ 50 ∧ spread ≥ 2000`, regardless of spell state; it is NOT
  propagated through recovered intermediate months, and the app's pending
  censoring reads the month it needs directly.
- **Typed refusals and pre-Phase-3 observability.** A snapshot whose frozen
  anchor window has no witnessed spread refuses with
  `no_market_level_observation` (never a fabricated anchor); a re-resolution
  that no longer reproduces the pinned/frozen anchor refuses with
  `anchor_not_reproduced` / `anchor_drift`. Every typed refusal
  (`anchor_drift`, `publish_failed`, …) is emitted at WARNING level by the
  worker, so before Phase 3 the alerting surface is the daily JSON
  (`implied_rating.state`) plus those `bond_market_implied_rating_v1` warnings;
  the daily verdict stays neutral by plan.

## 3. Data split (frozen)

Let `A = 2026-08` be the frozen window end (`CALIBRATION_WINDOW_END_MONTH`, the
last closed bond-panel month at declaration).

| Block | Months | Role |
| --- | --- | --- |
| Calibration | `2023-09 .. 2026-08` (36 months) | Frozen `L_anchor` window; mechanism diagnostics |
| Holdout | `2021-09 .. 2023-08` (24 months) | **Every acceptance gate is evaluated here** |

- The two blocks share no month. The holdout is the 24 months immediately
  **preceding** the calibration block: that is the only assignment consistent
  with the frozen anchor constant (the anchor window must end at `A`), and it
  keeps every statistic fully observable inside the closed history — a default
  candidate as late as 2023-08 has its 12-month outcome by 2024-08 ≤ `A`.
- The notebook must verify the served panel's `last_closed_month`; if it is not
  `2026-08`, the round **stops** and a new declaration is required (re-anchoring
  after seeing data is exactly what the freeze exists to prevent).
- No month, no cusip and no outcome may be used to choose a parameter: all
  parameters in §2 are typed here, before the numbers. A parameter change means
  a new `POLICY_DIGEST` and a **new round**.

## 4. Arms

- **Control (benchmark):** the currently adopted static OSBAP rating
  (`bond_panel_current_rating_pit_v1_mat`, `rating_state='historical_pit'`) and
  the v1.5 pair-adjacent EL arithmetic. Used as the ordinal benchmark for τ,
  ±1 concordance and IG/HY AUC; the OSBAP post-exit reconciliation stays
  **descriptive** (plan D11), never a numerator.
- **Treatment:** the implied bucket + exposure estimator of §2 (Jeffreys,
  weighted PAVA with weights `E_b+1`, rule-of-three upper bound, LGD 0.60,
  `E_b·h_b ≥ 20` capacity floor).

## 5. Pipeline and provenance

- Producer: `src/bonds/implied_rating.py` at the committed revision of branch
  `feat/bond-market-implied-rating`; `POLICY_DIGEST` above.
- Inputs (read-only export; no production writes, no publication is created by
  the round):
  - `bond_panel_current_snapshot_v1_mat` — all candidates, `month <= A`
    (`cusip_id, month, price, spread_final_bps, mod_dur, trade_count,
    dollar_volume, maturity_date`; `eligibility_state` for gate G-U);
  - `bond_panel_current_rating_pit_v1_mat` (`rating_state='historical_pit'`) and
    `bond_rating_static` for the OSBAP benchmark and the false-D/D-capacity
    reading.
- Transport: read-only private-network export (`railway ssh --service livefeed`
  pattern) or an approved read-only replica. No secret is written to the
  notebook or to this declaration.
- Notebook: `docs/calibration/bond_market_implied_rating_v1/calibration_001.ipynb`.
  Cell 0 records, before any number: `sys.version`, numpy/pandas versions,
  `git rev-parse HEAD` of the producer, `POLICY_DIGEST`, and the sha256 of every
  exported CSV.
- Result: `docs/calibration/bond_market_implied_rating_round_result.md`,
  **append-only**; it never edits this declaration. It must carry the gate
  table filled, the resolved `L_anchor`, the new `POLICY_DIGEST` after pinning,
  and the executed notebook digests.

| Input | Identity at declaration | SHA-256 |
| --- | --- | --- |
| Snapshot export (`snapshot.csv`) | to be produced by the round, `month <= 2026-08` | filled by the round |
| Rating PIT export (`rating_pit.csv`) | to be produced by the round | filled by the round |
| Static rating export (`rating_static.csv`) | to be produced by the round | filled by the round |
| `src/bonds/implied_rating.py` | `POLICY_DIGEST` above | filled by the round |

## 6. Acceptance gates — typed before the run; results intentionally empty

Every gate is computed on the **holdout** block unless stated otherwise.

| # | Gate | Threshold / reading | Result |
| --- | --- | --- | --- |
| G1 | Determinism | two builds from the same export produce identical `rows_digest`; a repeat run is bit-exact | |
| G2 | Ordinal agreement with OSBAP static | Kendall τ ≥ **0.60** over holdout `(cusip, month)` pairs where both sides are `AAA..CCC` | |
| G3 | ±1 bucket concordance | ≥ **80 %** of the same pairs within one bucket | |
| G4 | IG/HY frontier discrimination | AUC ≥ **0.85**: static `AAA..BBB` as IG vs static `BB..CCC` as HY, ranked by the implied bucket ordinal | |
| G5 | Forward default discrimination | Gini ≥ **0.70** for a static default event observed within 12 months after the row | |
| G6 | False-D | ≤ **25 %** of implied `D` confirmations have no static `D` and no rating-feed `D` within 12 months of the candidate month | |
| G7 | Monthly migration | mean month-over-month implied bucket change (RATED→RATED) ≤ **5 %** | |
| G8 | **G-U** implied unmapped rate | fraction of the solve universe at the base date with implied state outside `AAA..CCC` = **0** on the holdout; any non-zero value **fails the round** and goes to the owner as a DG-3 decision (the blocking behavior itself is already decided) | |
| G9 | Positive control / default capacity (§8) | the declared 60-month window (calibration + holdout) contains ≥ 1 confirmed `D` **and** ≥ 1 `d_candidate`; otherwise the round fails with `default capacity unverified` and §7 is reported | |
| G10 | Anchor reproducibility | the frozen `[2023-09, 2026-08]` median of `L` reproduces the pinned anchor exactly | |

Rules that are part of the declaration:

- No gate may be re-read on a different window after a failure; no threshold,
  cut, δ, β, window or mechanism may be adjusted after seeing holdout output.
- A `PASS` here only licenses presenting the result to the owner for **DG-4**;
  it does not move the pointer, does not publish, and does not change the app.
- A `FAIL` records the measured value in the result file and **stops the
  front** — it is an owner decision, not a retuning license.

## 7. Positive control and default capacity (§8 contract)

The round also freezes the estimator contract the app's §8 will assert, so the
numbers below are the ones the app registry will pin:

- Exposure: adjacent observable `(m → m+1)` pairs of the same cusip/spell;
  `E_b` counts the pair when `implied_bucket(m) = b ∈ RATED` and
  `implied_bucket(m+1) ∈ RATED ∪ {D}`; carry-forward months (`witnessed=false`,
  `carry_months ≤ 3`) count as exposure in the carried bucket.
- Defaults: `D_b` counts the pair as an event when the successor is `D`, dated
  at `d_event_month` with the origin bucket from `d_event_month − 1`; months
  between the event date and the confirmation month are excluded from exposure.
- Censoring — never exposure and never a denominator: `source_exit`,
  `withdrawal`, `pending` (`d_candidate ∧ ¬d_confirmed` inside `h_D` of the
  window end), `window_end`.
- Point hazard: Jeffreys `(D_b + 0.5)/(E_b + 1)`; isotonic via weighted PAVA
  (non-decreasing AAA→CCC, weights `E_b + 1`); `EL_b = λ̂_b · 0.60`.
- Zero-default HY bucket: `available` with the informative
  `el_default_capacity_verified_zero_bounded` **only when** the positive control
  passes and `E_b · h_b ≥ 20`, with `upper_bound_95 = 3/E_b · 0.60`; otherwise
  `el_bucket_exposure_insufficient` (blocked). IG with `D_b = 0` reports
  `observed` (Jeffreys + PAVA).
- `non-RATED` state in the solve universe at `T` → `el_bucket_unmapped`
  (blocked, fail-loud; no fallback and no stale witness) — **DG-3, owner
  decision, already taken.**

## 8. Decision gates before Phase 3

- **DG-3 (owner, decided).** Solve-universe bonds with implied state outside
  `AAA..CCC` at `T` block with `el_bucket_unmapped`. Gate G8 measures the rate;
  it can fail the round (owner then decides on a new policy, e.g. typed extended
  carry-forward), but it never changes this blocking behavior silently.
- **DG-4 (owner).** The owner records, in the result file, acceptance of the
  executed round (arm, gate results, resolved anchor, new `POLICY_DIGEST`).
  **Until G1–G10 have been executed and DG-4 is recorded, Phase 3 must not
  start**: no app consumer module, no `implied_rating_current` gate, no
  registry v1.6, no `make types`/fixture change in `investintell-light`.
- **No-retune rule.** Any change to §2 (including the anchor pin) mints a new
  `POLICY_DIGEST` and requires a new declaration + round; the holdout of this
  round is consumed and may not be reused for a retune of the same policy.
- Rollout after DG-4 (plan Phase 4) is a separate authorization: backfill in
  the datalake, verify `bond_market_implied_rating_v1_current`, then deploy the
  app consumer.
