"""Generate the open_macro v4.0-rev FORMULATION FREEZE artifact.

Usage (from the repo root):

    python -m harness.direct_activation.build_v4_formulation_freeze

Writes ``artifacts/quant/open_macro_v4_formulation_freeze_001/formulation_freeze.json``.

WHAT THE ARTIFACT IS
--------------------
The complete, unambiguous statement of the v4.0-rev formula — every explicit FRED
series id, every transform, threshold, window and PIT lag, the per-arm freshness
clocks with their two measured calibration points, the full book table at exact
float precision, the binding gates, and the pins that tie all of it to a specific
tree and a specific input snapshot.

It is committed UNRATIFIED (``status: awaiting_ratification``, ``approved: false``).
The ratification decision belongs to the quant_owner; self-ratification is
prohibited, and an engineering agent generating this file is not a step toward
approval. Nothing here activates anything: the artifact writes no DB row, flips no
flag and publishes no allocation.

Serialization mirrors the phase0q mould: ``sort_keys``, ``indent=1``,
``ensure_ascii=False``, trailing LF.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_DIR = ROOT / "artifacts" / "quant" / "open_macro_v4_formulation_freeze_001"
ARTIFACT = ARTIFACT_DIR / "formulation_freeze.json"
FIXTURES = ROOT / "tests" / "fixtures" / "open_macro_v4"

# The four modules this freeze describes. A5-style pins, computed over the tree.
FORMULA_MODULES = (
    "src/fiscal_state.py",
    "harness/direct_activation/credit_guard.py",
    "harness/phase0q/book_router.py",
    "harness/phase0q/v4_replay.py",
)

INPUT_SNAPSHOT_SHA256 = "42253855212736a948c2ae865676c3511aa271e3be698b91ba7fa9537b79da28"
M4_CORE_SHA256 = "384b02177cd8b17cb5403e5b140bf69d8f64df0ea05815936fcab7e8d7d0f424"
GOLDEN_LEDGER_SHA256 = "02a3b5ee791a8712eab0c40122d4491e513aa5886a6bd8248628999c6abd6cd3"

# The owner required this paragraph verbatim. It exists because "degrades to ALERT
# (never off)" is TRUE of the state and FALSE of the portfolio, and a reader who
# takes only the comfortable half will believe the book is defended when it is not.
A8_NORMATIVE_TEXT = (
    "Sob amplitude 0, ALERT em contained devolve o center: inerte. A leitura "
    "correta: após 3 meses não confirmados a guarda DESARMA A CARTEIRA, e só o "
    "preço (SPY ≤ −8% da máxima móvel de 12m) a re-arma. 'Degrada a ALERT "
    "(nunca off)' é verdadeiro do estado — a re-escalada segue vigiando — e "
    "falso da carteira.")

INPUT_VINTAGE_CAVEAT = (
    "a margem do arm_b ao limiar é 7× só dentro da janela avaliada; o mês mais "
    "apertado da série (1976-10) tem folga 0,0323pp contra revisão máxima "
    "observada de 0,0404pp — estender a janela para trás herda um ledger de arm_b "
    "instável a safra; o espelho M2SL é safra corrente (re-sync 2026-08-03), não "
    "PIT por vintage")


def _sha256_norm(path: Path) -> str:
    """sha256 of file bytes with CRLF→LF normalization (git-checkout agnostic)."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _canonical_block_sha256(block: dict) -> str:
    """sha256 over the canonicalized block (stable across writers)."""
    return hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_formulation() -> dict:
    """L1 / L2 / L3, stated so the engine could be rebuilt from this block alone."""
    from harness.direct_activation import credit_guard as guard
    from harness.phase0q import book_router as books
    from src import fiscal_state as fiscal

    return {
        "engine": "open_macro v4.0-rev (M-COMP4), owner-ratified 2026-08-02",
        "decision_timing": (
            "Every sensor is PIT-lagged to what a decision maker knew at the CLOSE "
            "of month-end t. The state machine at t produces book w_t; the "
            "portfolio is rebalanced to w_t at that close and earns month t+1's "
            "total returns."),
        "universe": {
            "core": list(books.CORE_TICKERS),
            "credit": list(books.CREDIT_TICKERS),
            "credit_base_weight": 0.0,
            "note": ("LQD is the barbell's credit destination; HYG is the "
                     "high-yield sensitivity column and is inert under the signed "
                     "configuration."),
        },
        "L1_fiscal": {
            "observable": "deficit_gdp",
            "series": {
                "deficit": {
                    "fred_series_id": fiscal.MTS_SERIES_ID,
                    "frequency": "monthly",
                    "units": "USD millions",
                    "pit_lag_months": fiscal.MTS_PIT_LAG_MONTHS,
                    "note": ("Monthly Treasury Statement federal surplus/deficit; "
                             "negative in a deficit month."),
                },
                "gdp": {
                    "fred_series_id": fiscal.GDP_SERIES_ID,
                    "frequency": "quarterly",
                    "units": "USD billions, NOMINAL level",
                    "pit_lag_months": fiscal.GDP_PIT_LAG_MONTHS,
                    "note": ("The nominal LEVEL series. NOT A191RL1Q225SBEA, which "
                             "is real growth and would silently change the ratio."),
                },
            },
            "transform": ("deficit_gdp(t) = -( rolling_12m_sum(MTSDS133FMS)(t) / "
                          "1000 ) / GDP(t) * 100"),
            "rolling_window_months": fiscal.DEFICIT_WINDOW_MONTHS,
            "pit_convention": ("stamp each observation at month_end(obs_date + lag), "
                               "keep the LAST duplicate stamp, reindex to the monthly "
                               "decision index and forward-fill; a publication-lag "
                               "shift, NOT vintage reconstruction"),
            "hysteresis": {
                "enter_dominance_above": fiscal.FISCAL_ENTER,
                "return_contained_below": fiscal.FISCAL_EXIT,
                "band_behaviour": ("[4.0, 5.0] HOLDS the state in force and raises "
                                   "fiscal_boundary"),
                "cold_start": ("the first finite reading picks dominance iff it is "
                               "strictly above the entry threshold, contained "
                               "otherwise"),
                "abstention": ("a non-finite reading emits no state, resets the age "
                               "to 0, and the month gets NO book"),
            },
            "state_age": ("fiscal_state_age_m counts consecutive months the current "
                          "state has held; 1 on the month of entry"),
        },
        "L2_books": {
            "compression": ("compressed_f(q) = f * baseline[q] + (1 - f) * CENTER — a "
                            "PURE convex combination over the fixed universe: no "
                            "`w > 0` drop, no renormalization, zero columns "
                            "preserved"),
            "center": ("the cross-quadrant MEAN of the four per-quadrant baselines "
                       "in the hash-pinned harness/phase0q/sleeve.py, imported "
                       "read-only"),
            "contained_amplitude": books.CONTAINED_AMPLITUDE,
            "contained_book": ("compressed_0(q) == CENTER exactly; the quadrant is a "
                               "PUBLISHED DIAGNOSTIC and NEVER determines the book"),
            "dominance_book": (f"compressed_{int(books.DOMINANCE_COMPRESSION * 100)}"
                               f"({books.DOMINANCE_QUADRANT}) + B60-LQD barbell"),
            "severe_book": (f"compressed_{int(books.SEVERE_COMPRESSION * 100)}"
                            f"({books.SEVERE_QUADRANT})"),
            "alert_blend": (f"{books.ALERT_BLEND_WEIGHT} * book_L2 + "
                            f"{1 - books.ALERT_BLEND_WEIGHT} * CENTER"),
            "barbell": {
                "id": "B60-LQD",
                "sleeve_replaced": list(books.FI_LEGS),
                "share_short": books.BARBELL_SHARE_SHORT,
                "short_ticker": books.BARBELL_SHORT_TICKER,
                "credit_ticker": books.BARBELL_CREDIT_TICKER,
                "applications": 1,
                "idempotent": False,
                "trap": ("bb_transform is NOT idempotent: pre-barbelling the "
                         "dominance book and then applying the hook again re-splits "
                         "the SHY leg the first pass created. Measured delta 0.093 "
                         "on SHY/LQD, and the double-pass book BREAKS the defensive "
                         "floor (TLT+SHY+TIP = 0.1395 against 0.20). Applied EXACTLY "
                         "ONCE, at construction of the dominance book."),
            },
            "invariants": {
                "weight_sum": 1.0,
                "weight_sum_tolerance": books.WEIGHT_SUM_TOLERANCE,
                "risk_assets": list(books.RISK_ASSETS),
                "max_risk_weight": books.RISK_CAP,
                "defensive_assets": list(books.DEFENSIVE_ASSETS),
                "min_defensive_weight": books.DEFENSIVE_FLOOR,
                "enforcement": ("REFUSE, never repair. Every emitted book satisfies "
                                "these natively, so none of the pinned sleeve's "
                                "_enforce_risk_cap / _enforce_defensive_floor / "
                                "_renormalize machinery is used."),
            },
            "renormalization_asymmetry_dissolved": (
                "The pinned sleeve keeps only `w > 0` and renormalizes the "
                "survivors. On the v4 universe that deletes LQD — the barbell's own "
                "destination — and HYG before the barbell could fill them. Under the "
                "signed dial only λ=0.5 and amplitude 0 are reachable, so "
                "compressed_25, the grid cell where the asymmetry actually bites, is "
                "never computed."),
            "quadrant_sources": {
                "post_chain": ("open_macro_v03_decision_chain; status='valid' reads "
                               "FRESH, otherwise the last valid reading CARRIES for "
                               "up to 3 months, then no_signal"),
                "pre_chain": ("REPLAY-ONLY proxy: CFNAI ma3 lag 1 (growth) × CPI YoY "
                              "acceleration lag 1 (inflation). Historical "
                              "reconstruction, never a live path."),
            },
        },
        "L3_guard": {
            "arm_a": {
                "fred_series_id": guard.SLOOS_SERIES_ID,
                "what_it_is": ("SLOOS net percentage of banks TIGHTENING standards on "
                               "C&I loans to large and middle-market firms — the bank "
                               "DECISION series, not DRTSCILM and not a spread"),
                "frequency": "quarterly",
                "pit_lag_months": guard.SLOOS_PIT_LAG_MONTHS,
                "transform": (f"rolling z-score, {guard.SLOOS_Z_WINDOW_MONTHS}-month "
                              f"trailing window INCLUDING the current month, "
                              f"min_periods {guard.SLOOS_Z_MIN_PERIODS}"),
                "threshold": guard.SLOOS_Z_THRESHOLD,
                "fires_when": f"z > {guard.SLOOS_Z_THRESHOLD}",
            },
            "arm_b": {
                "fred_series_id": guard.M2_SERIES_ID,
                "frequency": "monthly",
                "pit_lag_months": guard.M2_PIT_LAG_MONTHS,
                "transform": ("month-end resample (last), NaN months dropped before "
                              "the reindex, year-over-year in percent, shifted 1 "
                              "month"),
                "lookback_months": guard.M2_YOY_LOOKBACK_MONTHS,
                "threshold": guard.M2_YOY_THRESHOLD,
                "fires_when": f"M2 YoY > {guard.M2_YOY_THRESHOLD}",
            },
            "alert": "arm_a OR arm_b OR guard_blind",
            "A3": {
                "rule": ("a SEVERE candidate additionally requires "
                         "fiscal_state == contained with fiscal_state_age_m >= "
                         f"{guard.SEVERE_MIN_CONTAINED_AGE_M}"),
                "min_contained_age_months": guard.SEVERE_MIN_CONTAINED_AGE_M,
                "consequence": "SEVERE is unreachable under fiscal dominance",
            },
            "A8": {
                "persist": guard.SEVERE_PERSIST_MODE,
                "K": guard.SEVERE_PERSIST_K,
                "rule": ("the ENTRY rule is unchanged; a month meeting it is a "
                         "CANDIDATE. severe_run_age counts the current maximal "
                         "consecutive candidate stretch. The candidate is EMITTED as "
                         "SEVERE iff run_age <= K OR stress_confirmed; otherwise it "
                         "DEGRADES to ALERT and re-escalates the moment stress "
                         "confirms."),
                "stress_confirmed": {
                    "ticker": guard.STRESS_TICKER,
                    "price": "month-end adjusted close (total return)",
                    "window_months": guard.STRESS_WINDOW_MONTHS,
                    "window_bounds": "t-11..t inclusive, 12 observations required",
                    "threshold": guard.STRESS_DRAWDOWN_X,
                    "rule": ("SPY month-end close at or below -8% of its trailing "
                             "12-month month-end maximum"),
                },
                "normative_text": A8_NORMATIVE_TEXT,
            },
            "tokens": {
                "guard_level": [guard.GUARD_OFF, guard.GUARD_ALERT, guard.GUARD_SEVERE],
                "guard_coverage": [guard.COVERAGE_FULL, guard.COVERAGE_PARTIAL_A,
                                   guard.COVERAGE_PARTIAL_B, guard.COVERAGE_BLIND],
                "severe_run_age": "length of the current consecutive candidate stretch",
                "severe_degraded": "a candidate that did NOT emit",
                "stress_confirmed": "A8's price confirmation",
                "emission_rule": ("the coverage token is emitted even when the book "
                                  "does not move. Under amplitude 0 an ALERT inside "
                                  "contained is inert, which is exactly the month "
                                  "where a suppressed token would hide a real loss "
                                  "of coverage."),
            },
        },
    }


def build_freshness() -> dict:
    """The per-arm publication clocks and the two measured calibration points."""
    from harness.direct_activation import credit_guard as guard

    return {
        "principle": ("freshness is measured in NATIVE PUBLICATION PERIODS, never in "
                      "months: the two arms publish on different clocks, so a single "
                      "month-count threshold gives opposite verdicts for the same "
                      "label age"),
        "deadline": "deadline(p) = month_end( start_of(p) + lag_months )",
        "rule": "missing(arm, t) = #{ p : p > last_observation ∧ deadline(p) <= t }",
        "stale_when": "missing >= 1",
        "arms": [
            {
                "arm": "a",
                "series_id": guard.SLOOS_SERIES_ID,
                "frequency": guard.SLOOS_CLOCK.frequency,
                "lag_months": guard.SLOOS_CLOCK.lag_months,
                "period_identified_by": "period start (quarter start)",
            },
            {
                "arm": "b",
                "series_id": guard.M2_SERIES_ID,
                "frequency": guard.M2_CLOCK.frequency,
                "lag_months": guard.M2_CLOCK.lag_months,
                "period_identified_by": "period start (month start)",
            },
        ],
        "consequences": {
            "one_arm_stale": ("contributes False to the ALERT (never its last known "
                              "reading) and raises guard_coverage partial_a | "
                              "partial_b"),
            "both_stale": ("guard_blind: the guard cannot see, so it compresses "
                           "conservatively (ALERT) and says so with coverage blind"),
        },
        "calibration_points": [
            {
                "series_id": guard.M2_SERIES_ID,
                "last_observation": "2026-04-01",
                "evaluated_at": "2026-08-03",
                "missing_periods": 2,
                "stale": True,
                "why": ("the 2026-05 print was due 2026-06-30 and the 2026-06 print "
                        "was due 2026-07-31; the 2026-07 print is not due until "
                        "2026-08-31, which is why the count is 2 and not 3"),
            },
            {
                "series_id": guard.SLOOS_SERIES_ID,
                "last_observation": "2026-04-01",
                "evaluated_at": "2026-08-03",
                "missing_periods": 0,
                "stale": False,
                "why": ("the next quarterly period (2026-07) is not due until "
                        "2026-09-30; nothing is late"),
            },
        ],
        "equivalence_with_the_signed_engine": (
            "m4_core carries a single month-count rule, guard_blind = sloos_age_m > 5 "
            "or NaN. Over the 2006-12..2026-05 decision window the SLOOS mirror is "
            "gapless and quarterly, so its PIT age never leaves {0,1,2} and BOTH "
            "rules say guard_blind = False in all 234 months — the golden end-to-end "
            "replay proves it byte for byte. The rules DIVERGE before 1990-04, where "
            "SLOOS has no data at all: the month-count rule calls that blind (hence "
            "ALERT, hence a SEVERE candidate), while the per-arm rule sees M2SL "
            "publishing normally, drops only arm A and emits partial_a. That "
            "divergence is the v4 amendment and it lies entirely outside the window "
            "the signed configuration was measured on."),
    }


def build_books_table() -> dict:
    """Every book at exact float precision, 8 columns, no rounding."""
    from harness.phase0q import book_router as books

    table = {
        "center": dict(books.CENTER),
        "dominance_expansion_c50_b60_lqd": dict(books.DOMINANCE_BOOK),
        "severe_contraction_c50": dict(books.SEVERE_BOOK),
        "dominance_double_barbell_TRAP_not_the_signed_book": books.bb_transform(
            books.DOMINANCE_BOOK),
    }
    for quadrant in books.QUADRANTS:
        table[f"contained_compressed_0_{quadrant}"] = dict(books.CONTAINED_BOOKS[quadrant])
    for book_id, book in books.emitted_books().items():
        table[f"emitted::{book_id}"] = dict(book)
    return table


def build_gates() -> dict:
    return {
        "G1": {
            "id": "max_unconfirmed_severe_run_months",
            "bound": 3,
            "comparator": "<=",
            "binding": True,
            "what_it_bounds": ("the longest stretch of consecutive SEVERE candidate "
                               "months that A8 emits WITHOUT a price confirmation. "
                               "Past K the guard must disarm, not keep holding the "
                               "defensive book on an unconfirmed thesis."),
        },
        "G2": {
            "id": "min_rolling_12m_guard_cost",
            "bound": -0.10,
            "comparator": ">=",
            "binding": True,
            "definition": ("over every rolling 12-observation window with SPY total "
                           "return > 0.15: guard_cost = (port_TR - "
                           "port_TR_guard_OFF) / SPY_TR. Menu-neutral: both legs "
                           "carry the SAME L1/L2 book and the SAME barbell; only "
                           "guard_level differs."),
            "measured_min_at_signature": -0.086557991737,
        },
        "A4_prime": {
            "id": "min_upside_capture_bull_year",
            "bound": 0.35,
            "comparator": ">=",
            "watches": ("the ALERT blend target — the book the guard compresses "
                        "TOWARD, i.e. the CENTER"),
            "binding": True,
            "exception": {
                "year": 2013,
                "capture_2013": 0.1912,
                "status": "dated exception, granted at signature",
                "expires_with": "v4.1",
                "backstop_months": 12,
                "why": ("2013 is the single bull year the signed configuration fails "
                        "A4′ on. The exception is DATED and carries a 12-month "
                        "backstop so it cannot quietly become the rule; it expires "
                        "with v4.1 and must be re-measured, not renewed by default."),
            },
        },
        "F1_amplitude_reentry": {
            "what_it_governs": ("the only path by which contained amplitude may rise "
                                "above 0 again"),
            "conditions_all_required": [
                "the NEXT contained episode, not the current one",
                "a certified instrument only — no uncertified proxy may carry it",
                ("the contracting − expanding forward-12m spread must be <= 0 on the "
                 "certified instrument"),
                ">= 18 months of fresh-valid decisions inside the episode",
                "once per episode",
            ],
            "partial_credit": False,
            "note": ("No partial credit: an episode that meets four of the five "
                     "conditions re-enters at amplitude 0, unchanged. The dial does "
                     "not move on a near miss."),
        },
    }


def build_measurement_ledger() -> dict:
    return {
        "binding": {
            "masked_months": ["2020-02", "2020-03", "2020-04", "2020-05", "2020-06"],
            "why": ("the COVID crash and its rebound dominate every metric they touch "
                    "and would let a gate pass or fail on one episode"),
            "bull_year_2020": ("excluded from upside capture; a 7-month year is not a "
                               "calendar year"),
        },
        "advisory": {
            "always_printed": True,
            "why": ("the unmasked ledger is printed on EVERY run alongside the "
                    "binding one. Masking a window for judgement is defensible; "
                    "hiding it is not."),
        },
        "fold_dispersion": {
            "excluded_fold": 5,
            "fold_window": ["2022-03-01", "2023-02-28"],
            "why": ("the 2022 inflation shock; the exclusion is DECLARED here rather "
                    "than applied silently, and the all-folds statistic is reported "
                    "next to the excl-fold5 one"),
        },
    }


def build_freeze() -> dict:
    formulation = build_formulation()
    freshness = build_freshness()
    books_table = build_books_table()
    gates = build_gates()
    block = {"books": books_table, "formulation": formulation, "gates": gates,
             "freshness": freshness}
    return {
        "artifact_type": "open_macro_v4_formulation_freeze",
        "schema_version": 1,
        "freeze_id": "open_macro_v4_formulation_freeze_001",
        "engine_version": "v4.0-rev",
        "status": "awaiting_ratification",
        "approved": False,
        "approval_required_from": "quant_owner",
        "title": ("open_macro v4.0-rev — frozen formulation of the three-layer regime "
                  "engine (M-COMP4)"),
        "summary": ("The complete formula: explicit FRED ids, transforms, thresholds, "
                    "windows and PIT lags; per-arm freshness in native publication "
                    "periods with its two measured calibration points; the full book "
                    "table at exact float precision; the binding gates; and pins "
                    "tying all of it to a specific tree and input snapshot. Committed "
                    "UNRATIFIED."),
        "formulation": formulation,
        "freshness": freshness,
        "books": books_table,
        "gates": gates,
        "measurement_ledger": build_measurement_ledger(),
        "caveats": {
            "input_vintage": INPUT_VINTAGE_CAVEAT,
            "pre_chain_quadrant": ("the CFNAI × CPI-acceleration proxy is a REPLAY "
                                   "reconstruction for months before the decision "
                                   "chain exists. It is never a live decision path "
                                   "and never publishes."),
            "pin_scope": ("the module pins cover the FORMULA. They do not cover the "
                          "ingestion that fills macro_data, which is governed "
                          "separately."),
        },
        "pins": {
            "modules": {relative: _sha256_norm(ROOT / relative)
                        for relative in FORMULA_MODULES},
            "module_sha256_method": ("sha256 of file bytes with CRLF→LF "
                                     "normalization, the same _sha256_norm the Stage "
                                     "B module pins use"),
            "input_snapshot_sha256": INPUT_SNAPSHOT_SHA256,
            "m4_core_sha256": M4_CORE_SHA256,
            "golden_ledger_sha256": GOLDEN_LEDGER_SHA256,
            "golden_ledger_path": "tests/fixtures/open_macro_v4/golden_ledger.csv",
            "golden_ledger_rows": 234,
            "golden_ledger_window": ["2006-12-31", "2026-05-31"],
            "formulation_sha256": _canonical_block_sha256(block),
            "formulation_sha256_scope": ["books", "formulation", "freshness", "gates"],
        },
        "governance": {
            "A5": "blocked",
            "activation_allowed": False,
            "allocator_publish": False,
            "db_write_mode": "none",
            "official_result": False,
            "runtime_activation": False,
            "self_ratification": "prohibited",
            "note": ("These pins describe THIS ARTIFACT's own side effects — a "
                     "formulation record writes nothing and activates nothing by "
                     "itself. Runtime activation stays governed by the Stage B "
                     "activation envelope and its approval matrix. The ratification "
                     "decision is the quant_owner's; an engineering agent generating "
                     "this file is not a step toward approval."),
        },
    }


def main() -> int:
    payload = build_freeze()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    ARTIFACT.write_text(
        json.dumps(payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n",
        encoding="utf-8", newline="\n")
    print(f"wrote {ARTIFACT.relative_to(ROOT)}")
    print(f"  modules pinned      {len(FORMULA_MODULES)}")
    print(f"  formulation_sha256  {payload['pins']['formulation_sha256']}")
    print(f"  status              {payload['status']} (approved={payload['approved']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
