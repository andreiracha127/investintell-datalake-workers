# Bond panel unblock — findings, 2026-08-11

Standalone summary. The full evidence, with every gate and the verbatim run
JSON, is appended to `docs/calibration/bond_panel_pack_live_evidence_001.md`.

Branches: `feat/bond-panel-issuer-name-eligibility` (workers, 6 commits) and
`fix/bond-spread-model-144a-control` (investintell-light, 1 commit). Both pushed.

---

## 1. The failing gate: `rv_rank_correlation`. Not walk-forward, not absolute deltas.

The parity run under the redesigned contract emitted `parity_failed`,
`aborted=true`, for both declared months. **Exactly one gate failed, the same
one in both: the RV structural check, Spearman rank correlation.**

| Gate | 2025-12 | 2026-06 | Verdict |
| --- | ---: | ---: | --- |
| Walk-forward boundary | `max_input_day 2025-12-31`, `fit_as_of 2025-12-01` | `max_input_day 2026-06-30`, `fit_as_of 2026-06-01` | **pass** |
| Rebuilt universe size (>= 90%) | `9,201 / 9,304` = **`98.89%`** | `8,709 / 8,603` = **`101.23%`** | **pass** |
| Formula parity, ytm (median <= 1bp, p99 <= 25bp) | median `0`, p99 `1.39e-13` bp | same order | **pass** |
| Formula parity, duration (median <= 0.10y, p99 <= 1.0y) | median `0`, p99 `5.33e-15` y | same order | **pass** |
| Formula parity, spread (median <= 5bp, p99 <= 75bp) | median `0`, p99 `1.42e-13` bp | same order | **pass** |
| Exact reference accounting | `10,208 / 10,208` | `10,208 / 10,208` | **pass** |
| Typed exclusions | `100%` | `100%` | **pass** |
| RV structural validation (15 sub-gates) | all true | all true | **pass** |
| **RV Spearman >= 0.80** | **`0.4194`** | **`0.7840`** | **FAIL** |
| RV absolute z-delta (diagnostic only) | median `0.1349`, p99 `1.7296` | median `0.0886`, p99 `0.7429` | recorded, not blocking |

Common bonds: `7,507` (2025-12) and `7,008` (2026-06) — both far above the
300-bond comparability minimum.

The failing gate is **not** the absolute z-delta (which the contract makes a
diagnostic and which was treated as one) and **not** walk-forward (which passed
and was never relaxed).

### Is it a real defect or a threshold artifact?

**Neither, precisely: it is a real signal-level disagreement with a fully
measured cause, and the cause is a correction this same task asked for.**

Reproduced by refitting the frozen `2026-06` snapshot under each changed
condition in turn and ranking against the published `rv_signal`:

| Fit | Spearman vs published | n | Marginal cost |
| --- | ---: | ---: | ---: |
| A — frozen inputs, frozen specification | `0.9963` | `8,603` | control |
| B — cohort restricted to `7,013` bonds | `0.9929` | `7,013` | `-0.0034` |
| C — full cohort **+ the 144A control** | `0.8738` | `8,603` | **`-0.1225`** |
| D — full cohort **+ walk-forward static ratings** | `0.9219` | `8,603` | `-0.0744` |
| E — all three (the rebuild's conditions) | `0.8020` | `7,013` | — |

The production run measured `0.7840`; fit E predicts `0.8020` with a random
cohort proxy instead of the real eligibility-driven one. The decomposition
holds.

- **The cohort costs nothing** (`-0.0034`). The universe-size gate's premise is
  sound and it passed at `98.9%` / `101.2%`.
- **The 144A control dominates** (`-0.1225`). This is the measurable footprint
  of the premium the residual absorbed for 24 years. The gate compares a
  *corrected* signal against an *uncorrected* reference, so `>= 0.80` stopped
  being the right expectation the moment the 144A fix landed.
- **The non-PIT rating input is the rest** (`-0.0744`). `bond_rating_static` is
  a final-row mapping, not a point-in-time series, so walk-forward correctly
  discards rows dated after the month: `1,128` stripped for `2026-06`, `9,588`
  for `2025-12`. That difference is the entire distance between `0.78` and
  `0.42`.

**Walk-forward was not bent to recover either number.**

---

## 2. Issuer resolution

| Measure | Before | After (measured in the rebuild) |
| --- | ---: | ---: |
| `resolved` | `1,330` | — |
| `unresolved` | `6,360` | — |
| `missing_cik` | `2,518` | — |
| Bonds with a resolved display identity | `1,330` | **`8,350`** |
| Bonds INCLUDED in the closed month | `1,132` (2026-07) | **`7,013`** (2026-06) / `6,899` (2026-07) |
| Excluded as `unnamed_issuer` | — | `1,696` |

Resolution source: the serving chain's normalized reported-name consensus
(`src/bonds/issuer_consensus.py`) — `8,350` of the `10,073` curated securities
that have a security-master row (`8,349` `resolved` + `1` `ambiguous`); `133`
curated CUSIPs have no security at all.

**The acceptance floor is met on resolution and cannot be met on inclusion.**
`8,350` is the count of bonds that resolve. Applying the remaining *frozen*
eligibility tests to those same rows leaves `7,013` in `2026-06`, and the
residual exclusions are typed market and data facts, not identity:
`matured_or_short` `676`, `illiquid` `322`, `missing_asset_class` `190`,
`missing_amount` `175`, `missing_currency` `135`, `invalid_ytm` `1`. The target
"8,350 resolved **and included**" assumed named implies eligible; it does not.

Measured in the rebuild, not inferred: the parity run's reference accounting
reports `included_size: 7,013`, `excluded_size: 3,195` over `10,208` reference
CUSIPs, every exclusion typed.

**Not yet in the published table.** Stage 6 is blocked by the failed gate, so
`bond_panel_snapshot` still carries the old `1,330 / 6,360 / 2,518` split for
the live months. The `7,013` is measured from the production rebuild inside a
read-only transaction.

### The single highest-value follow-up

The `1,723` securities without a consensus name are **not unnamed** — all
`1,723` carry reported names. All abstained at the CUSIP6 layer:

| Abstain reason | Count | Mean top share | Mean distinct names |
| --- | ---: | ---: | ---: |
| `no_consensus` | `1,290` | `0.512` | `7.2` |
| `multiple_lei` | `433` | — | `7.9` |

The `1,290` `no_consensus` cases carry **exactly one distinct reported LEI** —
the legal entity agrees, only the spelling splits, just under the `0.60`
threshold. Prefix containment folds truncations but not abbreviations, so
`DELL INT EMC` (`711` votes) loses to `DELL INTERNATIONAL EMC...` (`797`) and
neither reaches consensus. The `433` `multiple_lei` cases are genuine co-issuer
bonds whose slash-joined name is a perfectly good display string.

Recovering these lifts the solve universe from `~6,900` toward `~8,500`. Not
done here: it touches a pre-registered module and deserves its own round.

---

## 3. RV IC before and after the 144A fix

`db_type` is never `2` anywhere in 24 years — the values are `1` (`58,881`
CUSIPs from 2002-07), `3` (`9,407` from 2010-03) and `NULL` (only from 2025-04).
Measured over the frozen panel: **`0` of `273` months had a non-zero `is_144a`
value.** The declared control was an identically-zero column since 2002, so the
144A premium — `213,854` of `1,547,178` panel rows, `24.4%` of the live universe
— went into the residual, which IS the RV signal.

Dev window `2013-01 -> 2023-03`, `123` months, same inputs, same clock, same
pre-registered gates. The `before` arm reproduces the published P1/P2 report
exactly, which is what makes the delta credible.

| Metric | before | after | P1/P2 report | Frozen kill gate |
| --- | ---: | ---: | ---: | --- |
| mean monthly IC | `0.0633` | `0.0633` | `0.063` | `>= 0.02` → **PASS** |
| Newey-West t (3 lags) | `5.55` | `5.65` | `5.55` | `>= 2` → **PASS**, wider |
| IC hit rate | `74.8%` | `74.8%` | `74.8%` | — |
| Q5−Q1 gross annualized | `+2.19%` | `+2.27%` | `+2.2%` | — |
| Q5−Q1 **net** annualized | `-6.98%` | `-6.90%` | not published | `> 0` → see below |
| mean monthly R² | `0.4174` | `0.4197` | — | — |
| months with the control applied | `0` | `129 / 273` | — | — |
| IC decay h=1 → h=12 | `0.0957 → 0.0769` | `0.0951 → 0.0763` | `0.096 → 0.077` | — |

**Two of the three gates pass, and by a wider margin after the fix.**

**The third gate is not reproducible from this repository, in either arm.** The
P1/P2 report publishes only the gross spread and records `PASS`; the cost
convention behind that verdict exists nowhere in either repo — there is no
runner, no notebook, no recorded parameter. Under the convention declared here
(per-month median `one_way_costs_asof`, net = gross − `4x` cost, i.e. full
monthly rotation of both legs) the net series is `-6.98%` before and `-6.90%`
after: **negative in both arms, essentially unchanged, therefore not a
regression introduced by the correction.** Median one-way cost `23.63 bps` over
all months, `18.24 bps` over the dev window; breakeven cost multiplier `0.956`
before and `0.991` after — the gross spread is worth roughly one round of
one-way cost, so the sign of the net gate is entirely a function of the assumed
turnover.

Said plainly: **the published `PASS` on `Q5−Q1 net > 0` cannot be reproduced,
and that is a finding about the original run's provenance, not about this fix.**

A third defect surfaced while fixing the second: `sm.add_constant` skips the
intercept when the design already carries a constant non-zero column, and
pre-2010 the all-`NR` rating dummy is exactly that. Dropping zero-variance
columns would have removed the model's only intercept and turned the fit into a
regression through the origin. `has_constant="add"` fixes it, verified
residual-neutral (fitted values identical to machine precision on months with
no 144A paper).

Rank stability before → after on identical frozen inputs: Spearman `>= 0.8862`
in all `273` months, median `1.0000`.

---

## 4. What is blocked, and on whom

**Stage 6 was not executed and the pointer was not moved.** Verified read-only
after the run: `3` publications, `max(computed_at)` and pointer `changed_at`
both unchanged at `2026-08-11T00:38:11.906749Z`, snapshot row counts unchanged.

The rank gate cannot pass while the frozen reference and the rebuild carry
different model specifications. **This is a product call, not an engineering
one:**

1. **Re-baseline** — republish the historical base under the corrected
   specification, then re-run parity. Fit A shows the harness reproduces the
   published signal at `0.9963`, so the rebase is mechanical.
2. **Sequence the changes** — land T1 alone (parity would clear `0.80`
   comfortably in `2026-06`: fit B `0.9929`, degraded only by the rating input
   to about `0.92`), then land the 144A correction as its own declared research
   round with its own re-baselined history.

Either way, **T4 — a genuinely point-in-time rating source — is now on the
critical path for any historical parity month**, not merely for the HY cap and
the expected-loss term. `2025-12` fails on the rating input alone.

## 5. Operational findings

- **`CODE_REVISION` is still pinned** on `bond-live-daily` to
  `7139388f0f65aab9e0232495822e07ab29e2d613`, left over from the 2026-08-07
  republication. `railway.toml` states in terms why this must never be a
  permanent variable: a fixed value shadows the per-deploy
  `RAILWAY_GIT_COMMIT_SHA`, so a code-only change re-serves the previous payload
  under the same `publication_id` while the run reports success. It must come
  off before the next Stage 6. Removing it is an owner-authorised production
  config change and was not done here.
- **`railway up` builds and starts without executing.** Both attempts required
  `railway service restart` to run the job, matching the 2026-08-08 record.
- **The parity gate had retired itself.** `run()` refused before opening a
  connection under the active `1863d3d5fa3a0edf` identity, which is why the
  publication serving production since 2026-08-11 00:38Z had passed no gate at
  all.
- **A single-month rebuild was not expressible** until this run: the mapping was
  emitted once per resolution window, and a single-month rebuild resolves the
  same month in both, so every row duplicated and the candidate join fanned out.
- **The recorded cause of the 2025-01 `zero_overlap` was wrong.**
  `sec_current_bond_security_alias_v1.valid_from` starts `2025-04-30`, so under
  walk-forward no alias resolves before then, `currency` is NULL for every row
  and the month falls out at `missing_currency`. Any month before `2025-05`
  rebuilds to zero by construction, whatever the rating input does.
