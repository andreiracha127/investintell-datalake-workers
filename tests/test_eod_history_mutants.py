"""Mutation-kill test for ``validate_series`` and ``stored_digest``.

Each mutant is an explicit text patch (old -> new, ``old`` occurring exactly
once) applied to ``src/workers/eod_history_validation.py`` and executed as a
throwaway module. A mutant is killed when it disagrees with the independent
reference (``_eod_history_reference``) or breaks a digest property on

* its deterministic targeted counterexample, and
* the reduced seeded corpus: the fuzz module's deterministic sweep plus
  ``EOD_MUTANT_CASES`` random cases (default 1,500, seed ``EOD_MUTANT_SEED``).

Both are required for every mutant. M01-M26 are the review gate's mutations
(gate 4, head 57df03fe) re-targeted to the current source; X01-X13 are this
suite's own (X13 re-creates the calendar defect the sweep found in this round).
If the validator changes so
that a patch no longer applies, the test fails: update the patch, never drop
the mutant.
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

import _eod_history_reference as ref
import pytest
import test_eod_history_fuzz as fuzz

from src.workers import eod_history_validation as v

SOURCE = Path(v.__file__).read_text(encoding="utf-8")
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
    ("X04_drop_price_bound", "any(values[k] > PRICE_MAX for k in _PRICES)", "False",
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
]
GATE = [m for m in MUTANTS if m[0].startswith("M")]
OWN = [m for m in MUTANTS if m[0].startswith("X")]
_MODULES: dict[str, types.ModuleType] = {}


def mutant(name: str, old: str, new: str) -> types.ModuleType:
    if name not in _MODULES:
        assert SOURCE.count(old) == 1, f"{name}: patch target occurs {SOURCE.count(old)} times"
        mod_name = f"_eod_validation_mutant_{name}"
        module = types.ModuleType(mod_name)
        sys.modules[mod_name] = module          # dataclasses resolve the module by name
        exec(compile(SOURCE.replace(old, new), f"<{mod_name}>", "exec"), module.__dict__)
        _MODULES[name] = module
    return _MODULES[name]


def verdict_problem(module, case: Case) -> str | None:
    """Disagreement with the reference on ``case``, or None."""
    try:
        got = fuzz.signature(module.validate_series(
            "MT", case.interval, case.provider, case.stored, module.xnys_calendar(), case.gaps))
    except Exception as exc:                                            # noqa: BLE001
        return f"raised {type(exc).__name__}: {exc}"
    want = ref.reference_verdict(*case.interval, case.provider, case.stored, case.gaps)
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
    assert len(GATE) == 26 and len(OWN) >= 10
    assert len({m[0] for m in MUTANTS}) == len(MUTANTS)


@pytest.mark.parametrize("name,old,new,case", MUTANTS, ids=[m[0] for m in MUTANTS])
def test_targeted_counterexample_kills_the_mutant(name, old, new, case):
    assert targeted_problem(v, case) is None, "the original must agree with the reference"
    problem = targeted_problem(mutant(name, old, new), case)
    assert problem is not None, f"{name} survived its targeted counterexample"


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
    print(f"\nmutation score {score}/{len(MUTANTS)} (corpus {CORPUS_CASES} cases, seed "
          f"{CORPUS_SEED})\n" + "\n".join(lines))
    assert score == len(MUTANTS)
    assert not corpus_survivors, f"mutants the corpus alone does not kill: {corpus_survivors}"
