"""Golden acceptance for the open_macro v4.0-rev engine (M-COMP4).

The signed configuration was ratified by the quant_owner on 2026-08-02 and its
ledger over the 2006-12..2026-05 decision window is pinned under
``tests/fixtures/open_macro_v4/``. These tests prove the repo's three formula
layers reproduce that ledger from the pinned inputs alone — states, tokens, book
ids and weights, at zero tolerance — and that the artifact freezing the
formulation still describes the tree it was cut from.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from harness.phase0q import book_router as books
from harness.phase0q import v4_replay as replay
from src import fiscal_state as fiscal

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "open_macro_v4"
INPUTS = FIXTURES / "inputs"
FREEZE = (ROOT / "artifacts" / "quant" / "open_macro_v4_formulation_freeze_001"
          / "formulation_freeze.json")

GOLDEN_LEDGER_SHA256 = "02a3b5ee791a8712eab0c40122d4491e513aa5886a6bd8248628999c6abd6cd3"
INPUT_SNAPSHOT_SHA256 = "42253855212736a948c2ae865676c3511aa271e3be698b91ba7fa9537b79da28"
M4_CORE_SHA256 = "384b02177cd8b17cb5403e5b140bf69d8f64df0ea05815936fcab7e8d7d0f424"

# The order the combined input digest is built in. Not alphabetical, not arbitrary:
# it is the order the canonical snapshot script emits and the digest is not
# reproducible under any other.
CANONICAL_INPUT_ORDER = ("GDP", "MTSDS133FMS", "SUBLPDCILSLGNQ", "M2SL", "CFNAI",
                         "CPIAUCSL", "chain", "prices")

EXPECTED_CROSSTAB = {
    ("contained", "off"): 58, ("contained", "alert"): 25, ("contained", "severe"): 23,
    ("dominance", "off"): 86, ("dominance", "alert"): 42, ("dominance", "severe"): 0,
}

# Measured, and deliberately NOT contiguous: 2015-09 is missing because A8
# re-escalates it to SEVERE on confirmed stress.
EXPECTED_DEGRADED_MONTHS = (
    "2007-12",
    "2015-06", "2015-07", "2015-08",
    "2015-10", "2015-11", "2015-12",
    "2016-01", "2016-02", "2016-03", "2016-04", "2016-05", "2016-06",
    "2016-07", "2016-08", "2016-09", "2016-10", "2016-11", "2016-12",
    "2017-01", "2017-02", "2017-03", "2017-04", "2017-05",
)

# The fiscal ledger over the full replay history (1959-01..2026-07). The first
# entry is the COLD START, not a flip: the panel is not finite before 1981-10.
EXPECTED_FISCAL_TRANSITIONS = (
    ("1981-10", None, "contained"),
    ("1983-05", "contained", "dominance"),
    ("1987-05", "dominance", "contained"),
    ("1992-05", "contained", "dominance"),
    ("1993-07", "dominance", "contained"),
    ("2009-02", "contained", "dominance"),
    ("2013-11", "dominance", "contained"),
    ("2020-05", "contained", "dominance"),
    ("2022-08", "dominance", "contained"),
    ("2022-10", "contained", "dominance"),
)


@pytest.fixture(scope="module")
def full_ledger() -> pd.DataFrame:
    return replay.ledger(extended=True)


@pytest.fixture(scope="module")
def window(full_ledger: pd.DataFrame) -> pd.DataFrame:
    return replay.golden_window(full_ledger)


# --------------------------------------------------------------------------- #
# 6. End to end — the replay reproduces the signed ledger                      #
# --------------------------------------------------------------------------- #

def _golden_text() -> str:
    """The golden ledger as text, CRLF-normalized so a Windows checkout with
    `core.autocrlf=true` cannot fake a mismatch (the fixtures also carry an
    explicit `text eol=lf` attribute)."""
    return FIXTURES.joinpath("golden_ledger.csv").read_bytes().replace(
        b"\r\n", b"\n").decode("utf-8")


def test_the_replay_reproduces_the_golden_ledger_byte_for_byte() -> None:
    """Zero tolerance. `%.17g` is round-trip exact for float64, so 'the same
    number' and 'the same bytes' are the same claim here — there is no rounding
    slack for a drifting book to hide in."""
    replayed = replay.replay_golden_csv()
    golden = _golden_text()
    if replayed != golden:
        mine, theirs = replayed.splitlines(), golden.splitlines()
        header = theirs[0].split(",")
        differing = [(i, a, b) for i, (a, b) in enumerate(zip(mine, theirs)) if a != b]
        detail = []
        for i, a, b in differing[:5]:
            fields = [f"{c}: {x!r} != {y!r}"
                      for c, x, y in zip(header, a.split(","), b.split(",")) if x != y]
            detail.append(f"row {i} ({a.split(',')[0]}): " + "; ".join(fields))
        pytest.fail(f"{len(differing)} of {len(theirs) - 1} ledger rows differ\n"
                    + "\n".join(detail))
    assert hashlib.sha256(replayed.encode("utf-8")).hexdigest() == GOLDEN_LEDGER_SHA256


def test_the_golden_ledger_file_still_carries_its_pinned_digest() -> None:
    """The fixture is the contract; a silent edit to it would silently move the
    contract."""
    digest = hashlib.sha256(_golden_text().encode("utf-8")).hexdigest()
    assert digest == GOLDEN_LEDGER_SHA256
    meta = json.loads(FIXTURES.joinpath("golden_meta.json").read_text(encoding="utf-8"))
    assert meta["sha256"] == GOLDEN_LEDGER_SHA256
    assert meta["rows"] == 234
    assert meta["window"] == ["2006-12-31", "2026-05-31"]
    assert meta["input_snapshot_sha256"] == INPUT_SNAPSHOT_SHA256
    assert meta["m4_core_sha256"] == M4_CORE_SHA256


def _canonical_part_digests() -> dict[str, str]:
    """Rebuild each input's canonical digest FROM THE CSV, not from the manifest.

    macro series  ``obs_date|value%.17g``
    chain         ``as_of|quadrant|status`` (a SQL NULL is the literal ``None``)
    prices        ``ticker|date|adj_close%.17g``
    """
    def digest(lines: list[str]) -> str:
        return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()

    out: dict[str, str] = {}
    for series_id in replay.MACRO_SERIES:
        with INPUTS.joinpath(f"{series_id}.csv").open(newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        out[series_id] = digest([f"{r['obs_date']}|{float(r['value']):.17g}"
                                 for r in rows])
    with INPUTS.joinpath("decision_chain.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    out["chain"] = digest([f"{r['as_of']}|{r['quadrant'] or 'None'}|"
                           f"{r['status'] or 'None'}" for r in rows])
    with INPUTS.joinpath("eod_prices_7.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    out["prices"] = digest([f"{r['ticker']}|{r['date']}|{float(r['adj_close']):.17g}"
                            for r in rows])
    return out


def test_the_pinned_input_digests_reproduce_from_the_files() -> None:
    """Every one of the eight inputs, and the combined snapshot digest.

    Recomputed from the CSVs rather than read off the manifest — a manifest that
    only agrees with itself proves nothing about the bytes the replay just ate."""
    manifest = json.loads(INPUTS.joinpath("manifest.json").read_text(encoding="utf-8"))
    parts = _canonical_part_digests()
    for series_id in replay.MACRO_SERIES:
        assert parts[series_id] == manifest["series"][series_id]["sha256"], series_id
    assert parts["chain"] == manifest["chain"]["sha256"]
    assert parts["prices"] == manifest["prices"]["sha256"]

    combined = hashlib.sha256(
        "\n".join(parts[name] for name in CANONICAL_INPUT_ORDER).encode("utf-8")
    ).hexdigest()
    assert combined == INPUT_SNAPSHOT_SHA256
    assert manifest["input_snapshot_sha256"] == INPUT_SNAPSHOT_SHA256
    assert manifest["verdict"] == "MATCH" and manifest["failures"] == []


# --------------------------------------------------------------------------- #
# 5. The crosstab                                                              #
# --------------------------------------------------------------------------- #

def test_the_golden_crosstab_of_state_by_guard_level(window: pd.DataFrame) -> None:
    """58/25/23 contained, 86/42/0 dominance, 234 months.

    The zero is the load-bearing cell: A3 forbids SEVERE under fiscal dominance, so
    the guard can compress a dominant-fiscal book but never replace it."""
    assert len(window) == 234
    table = pd.crosstab(window["fiscal_state"], window["guard_level"])
    observed = {
        (state, level): int(table.at[state, level]) if level in table.columns else 0
        for state in ("contained", "dominance")
        for level in ("off", "alert", "severe")}
    assert observed == EXPECTED_CROSSTAB
    assert sum(observed.values()) == 234


def test_severe_is_unreachable_under_fiscal_dominance(window: pd.DataFrame) -> None:
    dominance = window[window["fiscal_state"] == "dominance"]
    assert not dominance["severe_candidate"].astype(bool).any()
    assert not dominance["severe_emit"].astype(bool).any()


# --------------------------------------------------------------------------- #
# 4. A8 — degradation disarms the portfolio, price re-arms it                  #
# --------------------------------------------------------------------------- #

def test_a8_degraded_months_are_alert_and_hold_the_unguarded_book(
        window: pd.DataFrame) -> None:
    """Both halves of the claim, asserted TOGETHER.

    On every degraded month the STATE says `alert` — the guard is still watching —
    and the BOOK is bit-identical to the one composed with the guard OFF. "Degrades
    to ALERT (never off)" is true of the state and false of the portfolio, and
    splitting these into two tests would let each pass while the pair is the point.

    The list is 24 months and it is NOT contiguous: 2015-09 sits inside the
    2015-06..2017-05 stretch and is missing, because that month re-escalated."""
    degraded = window[window["severe_degraded"].astype(bool)]
    assert tuple(f"{d:%Y-%m}" for d in degraded.index) == EXPECTED_DEGRADED_MONTHS
    assert len(degraded) == 24
    assert set(degraded["guard_level"]) == {"alert"}
    assert set(degraded["fiscal_state"]) == {"contained"}

    for date, row in degraded.iterrows():
        unguarded, unguarded_id = books.compose(
            row["fiscal_state"], "off", row["quadrant"])
        assert unguarded_id is not None
        for ticker in books.UNIVERSE:
            assert row[ticker] - unguarded[ticker] == 0.0, f"{date:%Y-%m} {ticker}"
        # ... and that unguarded book is the CENTER, which is where amplitude 0 puts
        # every contained month regardless of the guard.
        assert unguarded == books.CENTER
        assert row["book_id"] == f"alert50:{unguarded_id}"


def test_2015_09_re_escalates_to_severe_on_confirmed_stress(
        window: pd.DataFrame) -> None:
    """The hole in the degraded range, and why it is there.

    2015-09 is a SEVERE candidate at run age 7 — well past K = 3 — so the cap alone
    would degrade it. SPY closed 8.48% below its trailing 12-month month-end
    maximum, past the 8% confirmation, and A8 re-escalates: only price re-arms the
    portfolio."""
    date = pd.Timestamp("2015-09-30")
    row = window.loc[date]
    assert bool(row["severe_candidate"]) is True
    assert int(row["severe_run_age"]) == 7
    assert int(row["severe_run_age"]) > 3
    assert bool(row["stress_confirmed"]) is True
    assert bool(row["severe_emit"]) is True
    assert bool(row["severe_degraded"]) is False
    assert row["guard_level"] == "severe"
    assert row["book_id"] == "severe:contraction_c50"

    from harness.direct_activation import credit_guard as guard
    monthly = replay.month_end_prices(replay.load_price_frame())
    drawdown = guard.stress_drawdown_series(monthly["SPY"], replay.decision_index())
    assert float(drawdown.loc[date]) == pytest.approx(-0.084802, abs=1e-6)
    assert float(drawdown.loc[date]) <= guard.STRESS_DRAWDOWN_X

    # its neighbours in the same candidate run were degraded, so the re-escalation is
    # the month's own price fact, not a property of the run.
    assert bool(window.at[pd.Timestamp("2015-08-31"), "severe_degraded"]) is True
    assert bool(window.at[pd.Timestamp("2015-10-31"), "severe_degraded"]) is True


def test_the_severe_candidate_accounting_closes(window: pd.DataFrame) -> None:
    """47 candidates = 23 emitted + 24 degraded; 25 contained ALERT months = the 24
    degraded plus 2022-09, an ALERT that never became a candidate at all (the
    contained run was only 2 months old)."""
    candidates = window["severe_candidate"].astype(bool)
    emitted = window["severe_emit"].astype(bool)
    degraded = window["severe_degraded"].astype(bool)
    assert int(candidates.sum()) == 47
    assert int(emitted.sum()) == 23
    assert int(degraded.sum()) == 24
    assert int(emitted.sum()) + int(degraded.sum()) == int(candidates.sum())

    contained_alert = ((window["fiscal_state"] == "contained")
                       & (window["guard_level"] == "alert"))
    extra = [f"{d:%Y-%m}" for d in window.index[contained_alert & ~degraded]]
    assert extra == ["2022-09"]
    assert int(window.at[pd.Timestamp("2022-09-30"), "fiscal_state_age_m"]) == 2


def test_contained_alert_months_hold_the_center_exactly(window: pd.DataFrame) -> None:
    """Group 2 again, this time on the measured ledger rather than on the composer."""
    mask = ((window["fiscal_state"] == "contained")
            & (window["guard_level"] == "alert"))
    rows = window[mask]
    assert len(rows) == 25
    deltas = [abs(rows.at[d, t] - books.CENTER[t]) for d in rows.index
              for t in books.UNIVERSE]
    assert max(deltas) == 0.0


# --------------------------------------------------------------------------- #
# 8. The fiscal ledger                                                         #
# --------------------------------------------------------------------------- #

def test_the_fiscal_ledger_entries_and_flips(full_ledger: pd.DataFrame) -> None:
    """Over the whole replay history: 10 transitions — 1 cold start + 9 FLIPS — and
    5 entries into dominance.

    The 2022 pair is the sharp one: dominance breaks at 2022-08 and is back at
    2022-10, a two-month contained interlude (2022-08 and 2022-09) driven by a
    deficit that dips to 3.71 and rebounds to 5.22. The hysteresis band did not
    absorb it because both readings cleared the band outright — which is the band
    working, not failing."""
    transitions = fiscal.state_transitions(full_ledger["fiscal_state"])
    observed = tuple((f"{t['date']:%Y-%m}", t["from"], t["to"]) for t in transitions)
    assert observed == EXPECTED_FISCAL_TRANSITIONS

    cold_starts = [t for t in transitions if t["from"] is None]
    assert len(cold_starts) == 1
    assert f"{cold_starts[0]['date']:%Y-%m}" == "1981-10"
    flips = [t for t in transitions if t["from"] is not None]
    assert len(flips) == 9
    assert sum(1 for t in transitions if t["to"] == "dominance") == 5

    interlude = full_ledger.loc["2022-08-31":"2022-09-30"]
    assert list(interlude["fiscal_state"]) == ["contained", "contained"]
    assert float(interlude.at[pd.Timestamp("2022-08-31"), "deficit_gdp"]) < 4.0
    assert float(full_ledger.at[pd.Timestamp("2022-10-31"), "deficit_gdp"]) > 5.0


def test_the_window_ledger_opens_already_contained(window: pd.DataFrame) -> None:
    """Inside the golden window there are 5 flips and 3 entries into dominance; the
    window opens mid-state, which the transition list reports honestly as a
    `None -> contained` entry rather than hiding."""
    transitions = fiscal.state_transitions(window["fiscal_state"])
    assert [(f"{t['date']:%Y-%m}", t["from"], t["to"]) for t in transitions] == [
        ("2006-12", None, "contained"),
        ("2009-02", "contained", "dominance"),
        ("2013-11", "dominance", "contained"),
        ("2020-05", "contained", "dominance"),
        ("2022-08", "dominance", "contained"),
        ("2022-10", "contained", "dominance"),
    ]
    assert sum(1 for t in transitions[1:] if t["to"] == "dominance") == 3


# --------------------------------------------------------------------------- #
# Freshness, measured on the replay rather than asserted in the abstract        #
# --------------------------------------------------------------------------- #

def test_the_per_arm_rule_agrees_with_the_signed_month_count_rule_in_window(
        window: pd.DataFrame, full_ledger: pd.DataFrame) -> None:
    """The equivalence the golden proves, stated explicitly.

    `m4_core` carries a single `guard_blind = sloos_age_m > 5 or NaN` month-count
    rule. Over the decision window the SLOOS mirror is gapless and quarterly, so its
    PIT age never leaves {0, 1, 2}: both rules say `guard_blind = False` in all 234
    months and the ledger comparison is byte-identical.

    They DIVERGE before 1990-04, where SLOOS has no data at all. The month-count
    rule calls that blind — and therefore ALERT, and therefore a SEVERE candidate —
    while the per-arm rule sees M2SL publishing normally, drops only arm A and says
    so with `partial_a`. That divergence is the v4 amendment, and it lives entirely
    outside the window the signed configuration was measured on."""
    assert not window["guard_blind"].astype(bool).any()
    assert set(window["sloos_age_m"]) <= {0.0, 1.0, 2.0}
    assert (window["sloos_age_m"] <= 5).all()
    assert set(window["guard_coverage"]) == {"full"}

    pre_1990 = full_ledger.loc[:"1990-03-31"]
    assert set(pre_1990["guard_coverage"]) == {"partial_a"}
    assert not pre_1990["guard_blind"].astype(bool).any()
    # arm A votes False while dark; it is never carried forward as its last reading.
    assert not pre_1990["arm_a"].astype(bool).any()
    assert full_ledger["guard_coverage"].value_counts().to_dict() == {
        "full": 436, "partial_a": 375}


def test_every_book_in_the_window_satisfies_the_mandate_invariants(
        window: pd.DataFrame) -> None:
    """The dissolution proof on real data: 234 emitted books, none of which needed
    the pinned `_enforce_*` machinery to become legal."""
    for date, row in window.iterrows():
        book = {t: float(row[t]) for t in books.UNIVERSE}
        books.check_invariants(book, f"{date:%Y-%m} {row['book_id']}")


def test_the_ledger_has_no_month_with_a_state_and_no_book(
        full_ledger: pd.DataFrame) -> None:
    """A routed month always has a book; a stateless month never does."""
    has_state = full_ledger["fiscal_state"].notna()
    has_book = full_ledger["book_id"].notna()
    assert (has_state == has_book).all()
    assert full_ledger.loc[has_state, list(books.UNIVERSE)].notna().all().all()
    assert full_ledger.loc[~has_state, list(books.UNIVERSE)].isna().all().all()


# --------------------------------------------------------------------------- #
# H. The formulation-freeze artifact                                           #
# --------------------------------------------------------------------------- #

def _sha256_norm(path: Path) -> str:
    """sha256 of file bytes with CRLF->LF normalization (git-checkout agnostic).

    The same helper `build_stage_b_artifacts` pins its modules with, restated here
    so the test does not verify a pin using the pin's own code path."""
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _canonical_block_sha256(block: dict) -> str:
    return hashlib.sha256(
        json.dumps(block, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@pytest.fixture(scope="module")
def freeze() -> dict:
    return json.loads(FREEZE.read_text(encoding="utf-8"))


def test_the_freeze_pins_exactly_the_four_v4_formula_modules(freeze: dict) -> None:
    assert set(freeze["pins"]["modules"]) == {
        "src/fiscal_state.py",
        "harness/direct_activation/credit_guard.py",
        "harness/phase0q/book_router.py",
        "harness/phase0q/v4_replay.py",
    }


def test_the_freeze_module_pins_match_the_recomputed_tree_hashes(freeze: dict) -> None:
    """Mirrors `test_module_pins_match_recomputed_tree_hashes`: the artifact must
    describe the tree it was cut from, or it is describing something else."""
    for relative, pinned in freeze["pins"]["modules"].items():
        assert pinned == _sha256_norm(ROOT / relative), relative


def test_the_freeze_carries_the_input_and_ledger_digests(freeze: dict) -> None:
    pins = freeze["pins"]
    assert pins["input_snapshot_sha256"] == INPUT_SNAPSHOT_SHA256
    assert pins["m4_core_sha256"] == M4_CORE_SHA256
    assert pins["golden_ledger_sha256"] == GOLDEN_LEDGER_SHA256
    assert _sha256_norm(FIXTURES / "golden_ledger.csv") == GOLDEN_LEDGER_SHA256


def test_the_formulation_sha256_recomputes(freeze: dict) -> None:
    """The digest is over the canonicalized formulation block, so any silent edit to
    a threshold, a lag or a book weight invalidates it."""
    recorded = freeze["pins"]["formulation_sha256"]
    block = {k: freeze[k] for k in ("books", "formulation", "gates", "freshness")}
    assert _canonical_block_sha256(block) == recorded


def test_the_freeze_is_unratified_and_forbids_self_ratification(freeze: dict) -> None:
    assert freeze["artifact_type"] == "open_macro_v4_formulation_freeze"
    assert freeze["schema_version"] == 1
    assert freeze["status"] == "awaiting_ratification"
    assert freeze["approved"] is False
    assert freeze["approval_required_from"] == "quant_owner"
    assert freeze["governance"]["self_ratification"] == "prohibited"
    assert freeze["governance"]["activation_allowed"] is False
    assert freeze["governance"]["allocator_publish"] is False
    assert freeze["governance"]["db_write_mode"] == "none"


def test_the_freeze_states_the_owner_normative_text_verbatim(freeze: dict) -> None:
    """The one paragraph the owner required word for word. It exists because
    "degrades to ALERT (never off)" is true of the STATE and false of the
    PORTFOLIO, and the artifact must not let a reader take the comfortable half."""
    assert freeze["formulation"]["L3_guard"]["A8"]["normative_text"] == (
        "Sob amplitude 0, ALERT em contained devolve o center: inerte. A leitura "
        "correta: após 3 meses não confirmados a guarda DESARMA A CARTEIRA, e só o "
        "preço (SPY ≤ −8% da máxima móvel de 12m) a re-arma. 'Degrada a ALERT "
        "(nunca off)' é verdadeiro do estado — a re-escalada segue vigiando — e "
        "falso da carteira.")


def test_the_freeze_books_match_the_router(freeze: dict) -> None:
    """The artifact's book table is the router's arithmetic, not a transcription."""
    table = freeze["books"]
    assert table["center"] == books.CENTER
    assert table["dominance_expansion_c50_b60_lqd"] == books.DOMINANCE_BOOK
    assert table["severe_contraction_c50"] == books.SEVERE_BOOK
    for quadrant in books.QUADRANTS:
        assert table[f"contained_compressed_0_{quadrant}"] == books.CENTER


def test_the_freeze_binds_g1_and_g2_and_dates_the_a4_prime_exception(
        freeze: dict) -> None:
    gates = freeze["gates"]
    assert gates["G1"]["bound"] == 3
    assert gates["G1"]["binding"] is True
    assert gates["G2"]["bound"] == -0.10
    assert gates["G2"]["binding"] is True
    assert gates["A4_prime"]["exception"]["capture_2013"] == 0.1912
    assert gates["A4_prime"]["exception"]["expires_with"] == "v4.1"
    assert gates["A4_prime"]["exception"]["backstop_months"] == 12


def test_the_freeze_records_the_freshness_calibration_points(freeze: dict) -> None:
    from harness.direct_activation import credit_guard as guard
    freshness = freeze["freshness"]
    assert freshness["rule"].startswith("missing")
    assert freshness["stale_when"] == "missing >= 1"
    arms = {a["arm"]: a for a in freshness["arms"]}
    assert arms["a"]["series_id"] == guard.SLOOS_SERIES_ID
    assert arms["a"]["frequency"] == "quarterly" and arms["a"]["lag_months"] == 2
    assert arms["b"]["series_id"] == guard.M2_SERIES_ID
    assert arms["b"]["frequency"] == "monthly" and arms["b"]["lag_months"] == 1
    points = {p["series_id"]: p for p in freshness["calibration_points"]}
    assert points[guard.M2_SERIES_ID]["missing_periods"] == 2
    assert points[guard.M2_SERIES_ID]["stale"] is True
    assert points[guard.SLOOS_SERIES_ID]["missing_periods"] == 0
    assert points[guard.SLOOS_SERIES_ID]["stale"] is False
    for point in points.values():
        assert point["evaluated_at"] == "2026-08-03"
        assert point["last_observation"] == "2026-04-01"
        verdict = guard.arm_freshness(
            [pd.Timestamp(point["last_observation"])],
            pd.Timestamp(point["evaluated_at"]),
            guard.SLOOS_CLOCK if point["series_id"] == guard.SLOOS_SERIES_ID
            else guard.M2_CLOCK)
        assert verdict.missing_periods == point["missing_periods"]
        assert verdict.stale is point["stale"]


def test_the_freeze_declares_the_covid_dual_ledger_and_the_vintage_caveat(
        freeze: dict) -> None:
    ledger = freeze["measurement_ledger"]
    assert ledger["binding"]["masked_months"] == ["2020-02", "2020-03", "2020-04",
                                                  "2020-05", "2020-06"]
    assert ledger["advisory"]["always_printed"] is True
    assert ledger["fold_dispersion"]["excluded_fold"] == 5
    assert "7×" in freeze["caveats"]["input_vintage"]
    assert "1976-10" in freeze["caveats"]["input_vintage"]


def test_the_freeze_is_serialized_in_the_phase0q_mould() -> None:
    raw = FREEZE.read_bytes()
    assert b"\r\n" not in raw
    assert raw.endswith(b"\n")
    payload = json.loads(raw.decode("utf-8"))
    assert raw.decode("utf-8") == json.dumps(
        payload, sort_keys=True, indent=1, ensure_ascii=False) + "\n"
