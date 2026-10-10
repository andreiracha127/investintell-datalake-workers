"""Mutation-kill test for ``validate_series``, ``stored_digest`` and the warmer.

Each mutant is an explicit text patch (old -> new, ``old`` occurring exactly
once) applied to a source file and executed as a throwaway module.

Validator mutants patch ``src/workers/eod_history_validation.py``. One is
killed when it disagrees with the independent reference
(``_eod_history_reference``) or breaks a digest property on

* its deterministic targeted counterexample, and
* the reduced seeded corpus: the fuzz module's deterministic sweep plus
  ``EOD_MUTANT_CASES`` random cases (default 1,500, seed ``EOD_MUTANT_SEED``).

Both are required for every validator mutant. M01-M26 are the review gate's
mutations (gate 4, head 57df03fe) re-targeted to the current source; X01-X13
are this suite's own (X13 re-creates a calendar defect an earlier sweep found);
N02, N15, N20, N21 and G5C01 are the five mutants gate 5 (head 56f7047a) wrote
that the corpus then missed (accept tuple bodies; the duplicate rule after the
outside-request rule; emit the wrong ticker; a digest blind to dates; Nixon's
funeral, 1994-04-27, as a session); R01-R05 are this round's (the conflict
ratio through a float, and the adjusted-price bound).

Warmer mutants (W01-W22 and W02a, 23) patch ``src/workers/eod_prices_warmer.py``. The fuzz
corpus judges ``validate_series`` only, so each is killed by the database-free
scenario (``_eod_warmer_scenarios``) that pins its rule, which the original
module passes; the real-lock proof of W01 runs against TimescaleDB in
``test_eod_foreign_listing_coverage_db``.

If a source changes so that a patch no longer applies, the test fails: update
the patch, never drop the mutant.
"""

from __future__ import annotations

import datetime as dt
import math
import os
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import _eod_warmer_scenarios as scenarios
import pytest
import test_eod_history_fuzz as fuzz

from src.workers import eod_history_validation as v
from src.workers import eod_prices_warmer as w

SOURCE = Path(v.__file__).read_text(encoding="utf-8")
WARMER_SOURCE = Path(w.__file__).read_text(encoding="utf-8")
CORPUS_CASES = int(os.environ.get("EOD_MUTANT_CASES", "1500"))
CORPUS_SEED = int(os.environ.get("EOD_MUTANT_SEED", "20261010"))

DAY = dt.date(2026, 10, 9)          # a Friday session
PREV = dt.date(2026, 10, 8)
SATURDAY = dt.date(2026, 10, 10)
NEXT = dt.date(2026, 10, 12)         # Monday session (Columbus Day trades)


def bar(day=DAY, **fields):
    b = {"date": day.isoformat(), "open": 100.0, "high": 110.0, "low": 90.0, "close": 100.0,
         "volume": 1000, "adjOpen": 100.0, "adjHigh": 110.0, "adjLow": 90.0, "adjClose": 100.0,
         "adjVolume": 1000.0, "divCash": 0.0, "splitFactor": 1.0}
    b.update(fields)
    return b


def flat(day=DAY, raw=100.0, adj=100.0, **fields):
    return bar(day, **{"open": raw, "high": raw, "low": raw, "close": raw, "adjOpen": adj,
                       "adjHigh": adj, "adjLow": adj, "adjClose": adj, **fields})


def row(b, **cols):
    r = fuzz.to_stored(b)
    r.update(cols)
    return r


@dataclass(frozen=True)
class Case:
    interval: tuple[dt.date, dt.date]
    provider: Any
    stored: dict
    gaps: frozenset = frozenset()


def one(b, stored=None, day=DAY):
    return Case((day, day), [b], stored or {})


DIGEST = "digest"   # targeted check is the digest property suite below

# (name, old, new, targeted counterexample)
MUTANTS = [
    ("M01_skip_session_holes", "if holes:", "if False and holes:",
     Case((dt.date(2026, 10, 7), DAY), [bar(dt.date(2026, 10, 7)), bar(DAY)], {})),
    ("M02_allow_duplicate_dates", "if len(bars) != len(provider_bars):",
     "if False and len(bars) != len(provider_bars):", Case((DAY, DAY), [bar(), bar()], {})),
    ("M03_allow_provider_off_session", "if off:", "if False and off:",
     Case((DAY, NEXT), [bar(DAY), bar(SATURDAY), bar(NEXT)], {})),
    ("M04_omit_stored_raw_open", "for col, key in STORED_RAW)",
     'for col, key in STORED_RAW if col != "open")', one(bar(), {DAY: row(bar(), open=100.01)})),
    ("M05_omit_stored_raw_low", "for col, key in STORED_RAW)",
     'for col, key in STORED_RAW if col != "low")', one(bar(), {DAY: row(bar(), low=90.009)})),
    ("M06_omit_stored_adj_open", "for col, key in STORED_ADJ)",
     'for col, key in STORED_ADJ if col != "adj_open")',
     one(bar(), {DAY: row(bar(), adj_open=100.01)})),
    ("M07_omit_stored_adj_low", "for col, key in STORED_ADJ)",
     'for col, key in STORED_ADJ if col != "adj_low")',
     one(bar(), {DAY: row(bar(), adj_low=90.009)})),
    ("M08_factor_tolerance_10pct", "FACTOR_REL_TOL = Fraction(1, 100)",
     "FACTOR_REL_TOL = Fraction(1, 10)", one(bar(adjOpen=102.0))),
    ("M09_omit_open_factor",
     '(("open", "adjOpen"), ("high", "adjHigh"), ("low", "adjLow"))',
     '(("high", "adjHigh"), ("low", "adjLow"))', one(bar(adjOpen=102.0))),
    ("M10_omit_low_factor",
     '(("open", "adjOpen"), ("high", "adjHigh"), ("low", "adjLow"))',
     '(("open", "adjOpen"), ("high", "adjHigh"))', one(bar(adjLow=91.8))),
    ("M11_allow_negative_adj_volume",
     'if any(values[k] < 0 for k in _VOLUMES) or values["divCash"] < 0:',
     'if values["volume"] < 0 or values["divCash"] < 0:', one(bar(adjVolume=-1.0))),
    ("M12_allow_zero_split", 'or values["splitFactor"] <= 0:', 'or values["splitFactor"] < 0:',
     one(bar(splitFactor=0.0))),
    ("M13_allow_boolean_numbers",
     "if isinstance(value, bool) or not isinstance(value, (int, float)):",
     "if not isinstance(value, (int, float)):", one(bar(volume=True))),
    ("M14_truncate_date_before_parsing", "m = _DATE.fullmatch(value)",
     "m = _DATE.fullmatch(value[:10])", one(bar(date="2026-10-09garbage"))),
    ("M15_allow_unicode_digits", "([0-9]{4})-([0-9]{2})-([0-9]{2})", r"(\d{4})-(\d{2})-(\d{2})",
     one(bar(date="２０２６-１０-０９"))),
    ("M16_omit_raw_ohlc_order", "for o, h, lo, c in (_RAW, _ADJ):", "for o, h, lo, c in (_ADJ,):",
     one(bar(high=99.5, adjHigh=100.0))),
    ("M17_omit_adjusted_ohlc_order", "for o, h, lo, c in (_RAW, _ADJ):",
     "for o, h, lo, c in (_RAW,):", one(bar(high=100.5, adjHigh=99.6))),
    ("M18_skip_stored_absent", "if absent:", "if False and absent:",
     Case((PREV, DAY), [bar(DAY)], {PREV: row(bar(PREV))})),
    ("M19_stored_tolerance_1e4", "STORED_REL_TOL = Fraction(1, 1_000_000)",
     "STORED_REL_TOL = Fraction(1, 10_000)", one(bar(), {DAY: row(bar(), close=100.005)})),
    ("M20_calendar_start_1990", "CALENDAR_SUPPORTED_FROM = _dt.date(1970, 1, 1)",
     "CALENDAR_SUPPORTED_FROM = _dt.date(1990, 1, 1)",
     one(bar(dt.date(1982, 1, 4)), day=dt.date(1982, 1, 4))),
    ("M21_constant_digest", "return h.hexdigest()", 'return "constant"', DIGEST),
    ("M22_swap_volume_columns", '("volume", "volume"), ("adj_open", "adjOpen")',
     '("volume", "adjVolume"), ("adj_open", "adjOpen")', one(bar(volume=1000, adjVolume=2000.0))),
    ("M23_allow_negative_dividend", ' or values["divCash"] < 0:', ":", one(bar(divCash=-0.5))),
    ("M24_allow_any_fractional_midnight", r"(?:\.000)?", r"(?:\.[0-9]{3})?",
     one(bar(date="2026-10-09T00:00:00.123Z"))),
    ("M25_allow_nonfinite_dividend", "if not _finite(values[key]):",
     'if key != "divCash" and not _finite(values[key]):', one(bar(divCash=math.nan))),
    ("M26_reject_factor_exact_boundary",
     "if abs(a * close - adj_close * r) > FACTOR_REL_TOL * adj_close * r:",
     "if abs(a * close - adj_close * r) >= FACTOR_REL_TOL * adj_close * r:",
     one(bar(adjOpen=10100.0, adjHigh=11000.0, adjLow=9000.0, adjClose=10000.0))),
    # ---- this suite's own mutants ----
    ("X01_skip_stored_bar_validation", "problem = stored_problem(stored[day])", "problem = None",
     one(flat(), {DAY: row(flat(), high=99.99995)})),
    ("X02_digest_omits_volume", 'for col in STORED_FIELDS:\n            h.update(b"|"',
     'for col in STORED_FIELDS[:4] + STORED_FIELDS[5:]:\n            h.update(b"|"', DIGEST),
    ("X03_float_factor_quotients",
     "if abs(a * close - adj_close * r) > FACTOR_REL_TOL * adj_close * r:",
     "if abs(float(a) / float(r) - float(adj_close) / float(close))"
     " > float(FACTOR_REL_TOL) * float(adj_close) / float(close):",
     one(flat(raw=1e-308, adj=1e7, adjOpen=1e6, adjLow=1e6))),
    ("X04_drop_price_bound", "any(values[k] > PRICE_MAX for k in _RAW)", "False",
     one(flat(raw=math.nextafter(1e7, math.inf), adj=1e7))),
    ("X05_drop_stored_outside_interval", "if stored_outside:", "if False and stored_outside:",
     Case((DAY, DAY), [bar(DAY)], {PREV: row(bar(PREV))})),
    ("X06_drop_bars_outside_request", "if outside:", "if False and outside:",
     Case((PREV, DAY), [bar(PREV), bar(DAY), bar(NEXT)], {})),
    ("X07_allow_stored_off_session", "if stored_off:", "if False and stored_off:",
     Case((DAY, NEXT), [bar(DAY), bar(NEXT)], {SATURDAY: row(bar(SATURDAY))})),
    ("X08_adjusted_check_only_when_loading", "if adj_bad:",
     "if adj_bad and set(bars) - set(stored):",
     one(bar(), {DAY: row(bar(), adj_open=50.0, adj_high=55.0, adj_low=45.0, adj_close=50.0)})),
    ("X09_drop_supported_from_guard", "if first < self.first or last > self.last:",
     "if last > self.last:",
     Case((dt.date(1965, 3, 1), dt.date(1965, 3, 5)), [bar(dt.date(1965, 3, 1))], {})),
    ("X10_ignore_accepted_gaps", "holes = sorted((expected - accepted_gaps)",
     "holes = sorted((expected)", Case((PREV, DAY), [bar(PREV)], {}, frozenset({DAY}))),
    ("X11_finite_without_overflow_guard", "except OverflowError:", "except ZeroDivisionError:",
     one(bar(volume=10**400))),
    ("X12_stored_tolerance_relative_to_min", "return abs(fa - fb) <= tol * max(abs(fa), abs(fb))",
     "return abs(fa - fb) <= tol * min(abs(fa), abs(fb))",
     one(flat(raw=1e6, adj=1e6), {DAY: row(flat(raw=999999.0, adj=1e6))})),
    # the calendar bug this round's sweep found: the library refuses a range
    # starting before its first session (1970-01-02), so a pass whose interval
    # was floored at 1970-01-01 was refused as outside the calendar
    ("X13_query_calendar_from_requested_day", "lo = max(first, self._first_session)",
     "lo = first", Case((dt.date(1970, 1, 1), dt.date(1970, 1, 2)), [bar(dt.date(1970, 1, 2))], {})),
    # ---- the five gate 5 mutants the corpus missed (now killed by it) ----
    ("N02_accept_tuple_bodies", "if not isinstance(provider_bars, list):",
     "if not isinstance(provider_bars, (list, tuple)):", Case((DAY, DAY), (bar(),), {})),
    ("N15_duplicate_rule_after_outside",
     'if len(bars) != len(provider_bars):\n        return verdict(VERDICT_INCOMPLETE,\n'
     '                       f"duplicate_dates={len(provider_bars) - len(bars)}")\n'
     '    outside = sorted(d for d in bars if d < start or d > end)\n'
     '    if outside:\n        return verdict(VERDICT_INCOMPLETE, f"bars_outside_request: first={outside[0]}")\n',
     'outside = sorted(d for d in bars if d < start or d > end)\n'
     '    if outside:\n        return verdict(VERDICT_INCOMPLETE, f"bars_outside_request: first={outside[0]}")\n'
     '    if len(bars) != len(provider_bars):\n        return verdict(VERDICT_INCOMPLETE,\n'
     '                       f"duplicate_dates={len(provider_bars) - len(bars)}")\n',
     Case((DAY, DAY), [bar(NEXT), bar(NEXT)], {})),
    ("N20_emit_the_wrong_ticker", "rows = tuple((ticker, d,", 'rows = tuple(("WRONG", d,', one(bar())),
    ("N21_digest_omits_dates", "h.update(day.isoformat().encode())", 'h.update(b"")', DIGEST),
    ("G5C01_nixon_funeral_is_a_session", "lo.isoformat(), last.isoformat()))",
     "lo.isoformat(), last.isoformat())) | (frozenset({_dt.date(1994, 4, 27)})"
     " if first <= _dt.date(1994, 4, 27) <= last else frozenset())",
     Case((dt.date(1994, 4, 26), dt.date(1994, 4, 28)),
          [bar(dt.date(1994, 4, 26)), bar(dt.date(1994, 4, 28))], {})),
    # ---- this round: conflict ratio without a float; the adjusted-price bound ----
    ("R01_ratio_through_a_float", 'return "n/a" if m is None else _format_ratio(m)',
     'return "n/a" if m is None else f"{float(m):.6f}"',
     Case((DAY, DAY), [flat(raw=1e7, adj=1e7)], {DAY: row(flat(raw=5e-324, adj=5e-324))})),
    ("R02_adjusted_bound_is_the_raw_bound", "ADJ_PRICE_MAX = 1e13", "ADJ_PRICE_MAX = PRICE_MAX",
     one(flat(raw=1.0, adj=11_760_000.0))),
    ("R03_drop_the_adjusted_bound", "or any(values[k] > ADJ_PRICE_MAX for k in _ADJ)", "or False",
     one(flat(raw=1e7, adj=math.nextafter(1e13, math.inf)))),
    ("R04_adjusted_bound_exclusive", "any(values[k] > ADJ_PRICE_MAX for k in _ADJ)",
     "any(values[k] >= ADJ_PRICE_MAX for k in _ADJ)", one(flat(raw=1e7, adj=1e13))),
    ("R05_raw_bound_is_the_adjusted_bound", "any(values[k] > PRICE_MAX for k in _RAW)",
     "any(values[k] > ADJ_PRICE_MAX for k in _RAW)",
     one(flat(raw=math.nextafter(1e7, math.inf), adj=1e7))),
]
GATE = [m for m in MUTANTS if m[0].startswith("M")]
OWN = [m for m in MUTANTS if m[0].startswith("X")]
GATE5 = [m for m in MUTANTS if m[0].startswith(("N", "G"))]
ROUND6 = [m for m in MUTANTS if m[0].startswith("R")]

# (name, old, new, scenario): scenario(module) is None for the original warmer
# and a problem for the mutant.
_EXC = "except Exception as exc:  # noqa: BLE001 \u2014 isolation is the point"
_BAD = "except ZeroDivisionError as exc:  # noqa: BLE001"
WARMER_MUTANTS = [
    ("W01_hold_the_snapshot_transaction",
     "            conn.commit()\n            tiingo_start = start",
     "            tiingo_start = start", scenarios.snapshot_released),
    # Either commit alone is covered by the other (the preflight's closes the
    # plan's reads; the explicit one is defence in depth), so W02 removes both.
    ("W02_hold_the_plan_transaction",
     ("    conn.commit()   # the plan's reads end here: no table lock is held across HTTP\n",
      "    chunks = chunk_footprint(conn, since, through)\n    conn.commit()\n"),
     ("", "    chunks = chunk_footprint(conn, since, through)\n"), scenarios.snapshot_released),
    ("W02a_hold_the_preflight_transaction",
     "    chunks = chunk_footprint(conn, since, through)\n    conn.commit()\n",
     "    chunks = chunk_footprint(conn, since, through)\n", scenarios.snapshot_released),
    ("W03_footprint_ceiling_exclusive", "if chunks > MAX_VERIFICATION_CHUNKS:",
     "if chunks >= MAX_VERIFICATION_CHUNKS:", scenarios.footprint_ceiling),
    ("W04_footprint_ceiling_raised", "MAX_VERIFICATION_CHUNKS = 800", "MAX_VERIFICATION_CHUNKS = 8000",
     scenarios.footprint_ceiling),
    ("W05_no_footprint_check_in_promote",
     "    require_chunk_footprint(conn, since, through)\n    inserted = 0", "    inserted = 0",
     scenarios.footprint_ceiling),
    ("W06_no_footprint_check_before_the_pass",
     "            require_chunk_footprint(conn, CALENDAR_SUPPORTED_FROM, as_of)\n", "",
     scenarios.footprint_ceiling),
    ("W07_footprint_counted_over_the_wrong_range",
     "require_chunk_footprint(conn, CALENDAR_SUPPORTED_FROM, as_of)",
     "require_chunk_footprint(conn, as_of, as_of)", scenarios.footprint_ceiling),
    ("W08_promotion_refusal_is_an_unexpected_failure", "except ChunkFootprintExceeded as exc:",
     "except ZeroDivisionError as exc:", scenarios.footprint_ceiling),
    ("W09_malformed_metadata_is_not_detected",
     'return value is not None and value != "" and parse_bar_date(value) is None', "return False",
     scenarios.metadata_dates),
    ("W10_metadata_date_is_truncated", "    return parse_bar_date(value)\n\n\ndef _meta_date_malformed",
     "    return parse_bar_date(str(value)[:10])\n\n\ndef _meta_date_malformed", scenarios.metadata_dates),
    ("W11_discovery_failure_escapes",
     "                " + _EXC + "\n                    _rollback_quietly(conn)",
     "                " + _BAD + "\n                    _rollback_quietly(conn)", scenarios.discovery_isolation),
    ("W12_discovery_failure_not_rolled_back",
     "                    _rollback_quietly(conn)\n"
     '                    history_error = {"source": "error", "reason": type(exc).__name__}\n',
     '                    history_error = {"source": "error", "reason": type(exc).__name__}\n',
     scenarios.discovery_isolation),
    ("W13_error_counter_not_reported", '        stats["foreign_history_errors"] = 1\n', "",
     scenarios.discovery_isolation),
    # ---- the status table and the history phase are isolated too ----
    ("W16_status_table_failure_escapes",
     "            " + _EXC + "\n                _rollback_quietly(conn)",
     "            " + _BAD + "\n                _rollback_quietly(conn)", scenarios.status_table_isolation),
    ("W17_history_runs_against_an_unverified_table",
     "            if history_error is None:\n                try:\n"
     "                    foreign = foreign_listing_tickers(conn, as_of)",
     "            if True:\n                try:\n"
     "                    foreign = foreign_listing_tickers(conn, as_of)", scenarios.status_table_isolation),
    ("W18_status_table_failure_not_staged", '"stage": "status_table"}', '"stage": "discovery"}',
     scenarios.status_table_isolation),
    ("W19_status_table_failure_not_rolled_back",
     "                _rollback_quietly(conn)\n"
     '                history_error = {"source": "error", "reason": type(exc).__name__,\n',
     '                history_error = {"source": "error", "reason": type(exc).__name__,\n',
     scenarios.status_table_isolation),
    ("W20_history_phase_failure_escapes",
     "                    " + _EXC + "\n                        _rollback_quietly(conn)",
     "                    " + _BAD + "\n                        _rollback_quietly(conn)", scenarios.history_phase_isolation),
    ("W21_status_table_without_timeouts",
     "        cur.execute(\n"
     '            "SELECT set_config(\'lock_timeout\', %s, true), set_config(\'statement_timeout\', %s, true)",\n'
     '            (f"{STATUS_TABLE_LOCK_TIMEOUT_MS}ms", f"{STATUS_TABLE_STATEMENT_TIMEOUT_MS}ms"))\n',
     "", scenarios.status_table_timeouts),
    ("W22_status_table_lock_wait_unbounded", "STATUS_TABLE_LOCK_TIMEOUT_MS = 5_000\n",
     "STATUS_TABLE_LOCK_TIMEOUT_MS = 5_000_000\n", scenarios.status_table_timeouts),
    ("W14_empty_source_is_omitted",
     "    else:\n        # The resolver exists and no line resolves", "    elif False:\n        # The resolver exists and no line resolves",
     scenarios.source_states),
    ("W15_empty_source_is_not_called_empty", '"source": "empty", "source_tickers": 0,',
     '"source": "none", "source_tickers": 0,', scenarios.source_states),
]
_MODULES: dict[str, types.ModuleType] = {}


def _build(name: str, source: str, old, new) -> types.ModuleType:
    """``old`` / ``new`` are one patch, or equal-length tuples of patches
    (a mutant that needs two removals to be observable)."""
    if name not in _MODULES:
        changed = source
        for o, n in zip(old if isinstance(old, tuple) else (old,),
                        new if isinstance(new, tuple) else (new,), strict=True):
            assert changed.count(o) == 1, f"{name}: patch target occurs {changed.count(o)} times"
            changed = changed.replace(o, n)
        mod_name = f"_eod_mutant_{name}"
        module = types.ModuleType(mod_name)
        sys.modules[mod_name] = module          # dataclasses resolve the module by name
        exec(compile(changed, f"<{mod_name}>", "exec"), module.__dict__)
        _MODULES[name] = module
    return _MODULES[name]


def mutant(name: str, old: str, new: str) -> types.ModuleType:
    return _build(name, SOURCE, old, new)


def warmer_mutant(name: str, old: str, new: str) -> types.ModuleType:
    return _build(name, WARMER_SOURCE, old, new)


def verdict_problem(module, case: Case) -> str | None:
    """Disagreement with the reference on ``case``, or None."""
    try:
        got = fuzz.signature(module.validate_series(
            "MT", case.interval, case.provider, case.stored, module.xnys_calendar(), case.gaps))
    except Exception as exc:                                            # noqa: BLE001
        return f"raised {type(exc).__name__}: {exc}"
    want = fuzz.expected(*case.interval, case.provider, case.stored, case.gaps, "MT")
    return None if got == want else f"{got[:2]} != reference {want[:2]}"


def digest_problem(module) -> str | None:
    """A digest property the snapshot contract needs, broken, or None."""
    snap = {PREV: row(bar(PREV)), DAY: row(bar(DAY))}
    base = module.stored_digest(snap)
    if module.stored_digest(dict(reversed(snap.items()))) != base:
        return "order dependent"
    for col in v.STORED_FIELDS:
        changed = {**snap, DAY: {**snap[DAY], col: snap[DAY][col] + 1}}
        if module.stored_digest(changed) == base:
            return f"blind to {col}"
    if module.stored_digest({PREV: snap[PREV]}) == base:
        return "blind to a deleted row"
    # the same unchanged row on another date is another snapshot
    if module.stored_digest({PREV: snap[PREV], NEXT: snap[DAY]}) == base:
        return "blind to the date of a row"
    if module.stored_digest({DAY: snap[PREV]}) == module.stored_digest({PREV: snap[PREV]}):
        return "blind to the date of the only row"
    return None


def targeted_problem(module, case) -> str | None:
    return digest_problem(module) if case == DIGEST else verdict_problem(module, case)


def corpus_kill(module) -> int | None:
    """Index of the first corpus case that kills ``module``, or None."""
    try:
        for n, _, _ in fuzz.run_corpus(module.validate_series, module.stored_digest,
                                       fuzz.corpus(CORPUS_CASES, CORPUS_SEED),
                                       calendar=module.xnys_calendar()):
            return n
    except Exception:                                                   # noqa: BLE001
        return -1
    return None


def test_mutant_list_is_complete():
    assert len(GATE) == 26 and len(OWN) == 13
    assert [m[0].split("_")[0] for m in GATE5] == ["N02", "N15", "N20", "N21", "G5C01"]
    assert len(ROUND6) == 5
    assert len(GATE) + len(OWN) + len(GATE5) + len(ROUND6) == len(MUTANTS)
    names = [m[0] for m in MUTANTS] + [m[0] for m in WARMER_MUTANTS]
    assert len(set(names)) == len(names)


@pytest.mark.parametrize("name,old,new,case", MUTANTS, ids=[m[0] for m in MUTANTS])
def test_targeted_counterexample_kills_the_mutant(name, old, new, case):
    assert targeted_problem(v, case) is None, "the original must agree with the reference"
    problem = targeted_problem(mutant(name, old, new), case)
    assert problem is not None, f"{name} survived its targeted counterexample"


@pytest.mark.parametrize("name,old,new,scenario", WARMER_MUTANTS, ids=[m[0] for m in WARMER_MUTANTS])
def test_warmer_scenario_kills_the_mutant(name, old, new, scenario):
    assert scenario(w) is None, "the original warmer must pass its scenario"
    problem = scenario(warmer_mutant(name, old, new))
    assert problem is not None, f"{name} survived its scenario"


def test_mutation_score_on_the_seeded_corpus():
    assert not list(fuzz.run_corpus(v.validate_series, v.stored_digest,
                                    fuzz.corpus(CORPUS_CASES, CORPUS_SEED)))
    lines, corpus_survivors, score = [], [], 0
    for name, old, new, case in MUTANTS:
        module = mutant(name, old, new)
        targeted = targeted_problem(module, case) is not None
        at = corpus_kill(module)
        killed = targeted or at is not None
        score += killed
        if at is None:
            corpus_survivors.append(name)
        lines.append(f"{name:42s} targeted={'killed' if targeted else 'SURVIVED'}"
                     f" corpus={'case ' + str(at) if at is not None else 'survived'}")
    warmer_killed = 0
    for name, old, new, scenario in WARMER_MUTANTS:
        problem = scenario(warmer_mutant(name, old, new))
        warmer_killed += problem is not None
        lines.append(f"{name:42s} scenario={'killed' if problem else 'SURVIVED'}")
    print(f"\nmutation score {score}/{len(MUTANTS)} validator mutants (corpus {CORPUS_CASES} "
          f"cases, seed {CORPUS_SEED}) + {warmer_killed}/{len(WARMER_MUTANTS)} warmer mutants\n"
          + "\n".join(lines))
    assert score == len(MUTANTS)
    assert warmer_killed == len(WARMER_MUTANTS)
    assert not corpus_survivors, f"mutants the corpus alone does not kill: {corpus_survivors}"
