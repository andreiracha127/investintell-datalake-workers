"""Differential fuzz: ``validate_series`` vs an independent reference.

The corpus is a deterministic sweep (``sweep_cases``, ~1,000 cases; on its own
it kills every mutant in ``test_eod_history_mutants``) followed by
seeded random cases (``gen_case``). Intervals lie in years the reference's own
hard-coded NYSE calendar covers (1970, 1973, 1974, 1982, 1985, 2001, 2012,
2023-2026), straddle ``CALENDAR_SUPPORTED_FROM`` (1970-01-01), lie before it,
or are reversed.

Sweep (enumerated, every item is a case):
 S1. provider: each of the 12 price fields x 17 kinds (NaN, +/-inf, negative,
     zero, +/-5e-324, numeric string, "N/A", True, False, None, missing, 1e308,
     10**400, exactly at its bound, one ulp over its bound), with an empty and
     with a complete store;
 S2. provider dates: 14 syntax variants (accepted and refused), fullwidth,
     Arabic-Indic and Devanagari digits, impossible dates, non-strings;
 S3. stored: each of the 12 columns x 9 kinds (NaN, +/-inf, negative, zero,
     -5e-324, NULL, 1e308, over bound), and the gate's high-below-open row;
 S4. raw-only and adjusted-only OHLC order breaks with factors inside 1%;
     series and store shapes (unsorted, duplicate, hole with and without an
     accepted gap, prefix, suffix, endpoints, off-session and outside bars,
     stored partial / absent / outside / off-session / rebased, empty, non-list);
 S5. the 1% factor edge on adjOpen/adjHigh/adjLow, both sides, exactly at, one
     ulp inside and one ulp outside, at four scale pairs;
 S6. the 1e-6 stored/provider edge on each of the 8 price columns, stored side
     larger or smaller, at/inside/outside, three scales;
 S7. magnitude bounds at and over on coherent bars; extreme magnitudes (raw
     1e-308 with adjusted 1e7 or 1e307/1e308; float quotients overflow);
 S8. the calendar domain edge (1965, 1969-12-31, 1970-01-01/02) and a window
     around every reference holiday, with and without a bar on the holiday.

Random (``gen_case``):
 R1. series shapes: complete, prefix, suffix, middle slice, interior holes,
     duplicates, unsorted, off-session or outside bars, endpoints only, empty,
     non-list body; evidenced gaps;
 R2. one provider field corrupted (all kinds above plus order-only breaks), a
     date corrupted a quarter of the time;
 R3. stored states: empty, complete, partial, gappy, outside the interval,
     off-session, another adjusted basis, a raw difference, a corrupted column,
     a single price column drifting by +/-5e-7, 2e-6 or 1e-4;
 R4. boundary and extreme bars (factor edges, overflow, bounds), the 1e-6 edge
     on all raw or all adjusted fields, scales from 1e-4 to 1e4.

On every case the production verdict, reason code and rows must equal the
reference's; every certifying verdict must satisfy the invariant on its own
terms (test-local exact predicate and exchange_calendars sessions); the digest
must be order-independent and change when a stored value changes or a stored
row is deleted.

``EOD_FUZZ_CASES`` sets the random size (CI default 3,000); ``EOD_FUZZ_SEED``
the seed.
"""

from __future__ import annotations

import collections
import datetime as dt
import math
import os
import random
import re
from fractions import Fraction

import _eod_history_reference as ref
import exchange_calendars as xcals
import pytest

from src.workers import eod_history_validation as v

CASES = int(os.environ.get("EOD_FUZZ_CASES", "3000"))
SEED = int(os.environ.get("EOD_FUZZ_SEED", "20261010"))
_XNYS = xcals.get_calendar("XNYS", start="1970-01-01")
CAL = v.xnys_calendar()
D = dt.timedelta
KEYS = ref.KEYS
BOUND = {"open": 1e7, "high": 1e7, "low": 1e7, "close": 1e7, "adjOpen": 1e7,
         "adjHigh": 1e7, "adjLow": 1e7, "adjClose": 1e7, "volume": 1e13,
         "adjVolume": 1e13, "divCash": 1e7, "splitFactor": 1e4}
STORED_OF = dict(zip(KEYS, ref.COLS))


def own_sessions(a, b):
    a = max(a, _XNYS.first_session.date())   # the library refuses a start before it
    if a > b:
        return set()
    return {ts.date() for ts in _XNYS.sessions_in_range(a.isoformat(), b.isoformat())}


def _fmt_date(rng, d):
    return rng.choice([d.isoformat(), f"{d.isoformat()}T00:00:00.000Z",
                       f"{d.isoformat()}T00:00:00Z"])


def make_bar(rng, d, factor, scale=1.0):
    o = rng.uniform(5, 300) * scale
    c = o * rng.uniform(0.95, 1.05)
    h = max(o, c) * rng.uniform(1.0, 1.03)
    lo = min(o, c) * rng.uniform(0.97, 1.0)

    def adj(x):
        return round(x * factor, 3) if rng.random() < 0.5 else x * factor

    return {"date": _fmt_date(rng, d), "open": o, "high": h, "low": lo, "close": c,
            "volume": rng.randint(0, 10**7), "adjOpen": adj(o), "adjHigh": adj(h),
            "adjLow": adj(lo), "adjClose": adj(c), "adjVolume": float(rng.randint(0, 10**7)),
            "divCash": rng.choice([0.0, 0.0, 0.0, 0.25]), "splitFactor": rng.choice([1.0, 1.0, 2.0])}


def flat_bar(d, raw, adj):
    """All four raw fields equal ``raw``, all four adjusted equal ``adj``."""
    return {"date": d.isoformat(), "open": raw, "high": raw, "low": raw, "close": raw,
            "volume": 100, "adjOpen": adj, "adjHigh": adj, "adjLow": adj, "adjClose": adj,
            "adjVolume": 100.0, "divCash": 0.0, "splitFactor": 1.0}


def to_stored(bar):
    return {STORED_OF[k]: bar[k] for k in KEYS}


def _non_session(rng, start, end, exp):
    days = [start + D(days=i) for i in range((end - start).days + 1)]
    off = [d for d in days if d not in exp]
    return rng.choice(off) if off else None


def _digits(d, zero):
    """``d`` with its ASCII digits replaced by another script's digits."""
    return "".join(chr(zero + int(ch)) if ch.isdigit() else ch for ch in d)


def corrupt_date(rng, d):
    return rng.choice([
        d + "garbage", d + "T00:00:00+01:00", d + "T12:00:00Z", d + " ", " " + d,
        d.replace("-", "/"), "2026-13-01", "2026-02-30", 20260810, None,
        _digits(d, 0xFF10), _digits(d, 0x0660), _digits(d, 0x0966),      # fullwidth, Arabic, Devanagari
        d + "T00:00:00.000", d + "T00:00:00.123Z", d + "T00:00:00.999Z", d + "T00:00:00.5Z",
        d + "T00:00:00.000000Z", d + "T00:00:00"])


def corrupt_field(rng, bar):
    bar = dict(bar)
    key = "date" if rng.random() < 0.25 else rng.choice(KEYS)
    if key == "date":
        bar["date"] = corrupt_date(rng, bar["date"][:10])
        return bar
    kind = rng.choice(["nan", "inf", "-inf", "negative", "zero", "string", "na", "bool",
                       "none", "missing", "huge", "bigint", "over_bound", "high_below",
                       "order_only", "order_only"])
    if kind == "nan":
        bar[key] = float("nan")
    elif kind == "inf":
        bar[key] = float("inf")
    elif kind == "-inf":
        bar[key] = float("-inf")
    elif kind == "negative":
        bar[key] = -abs(bar[key]) - 1.0
    elif kind == "zero":
        bar[key] = 0.0
    elif kind == "string":
        bar[key] = str(bar[key])
    elif kind == "na":
        bar[key] = "N/A"
    elif kind == "bool":
        bar[key] = rng.choice([True, False])
    elif kind == "none":
        bar[key] = None
    elif kind == "missing":
        del bar[key]
    elif kind == "huge":
        bar[key] = 1e308
    elif kind == "bigint":
        bar[key] = 10**400
    elif kind == "over_bound":
        bar[key] = BOUND[key] * 1.5
    elif kind == "high_below":
        h = "high" if rng.random() < 0.5 else "adjHigh"
        o = "open" if h == "high" else "adjOpen"
        bar[h] = bar[o] * 0.999
    elif kind == "order_only":
        bar = break_order(bar, rng.choice(["high", "low", "adjHigh", "adjLow"]))
    return bar


def break_order(bar, field):
    """Break the OHLC order of ONE side by 0.5% while the other side stays
    ordered and every factor stays within 1%, so only that side's order rule
    can reject the bar: the broken extreme moves 0.5% inside max/min(open,
    close) and its counterpart on the other side moves onto its own
    max/min(open, close)."""
    bar = dict(bar)
    other = {"high": "adjHigh", "low": "adjLow", "adjHigh": "high", "adjLow": "low"}[field]
    side = ("adjOpen", "adjClose") if field.startswith("adj") else ("open", "close")
    rest = ("open", "close") if field.startswith("adj") else ("adjOpen", "adjClose")
    if field.lower().endswith("high"):
        bar[field] = max(bar[side[0]], bar[side[1]]) * 0.995
        bar[other] = max(bar[rest[0]], bar[rest[1]])
    else:
        bar[field] = min(bar[side[0]], bar[side[1]]) * 1.005
        bar[other] = min(bar[rest[0]], bar[rest[1]])
    return bar


def corrupt_stored(rng, row):
    row = dict(row)
    col = rng.choice(ref.COLS)
    kind = rng.choice(["nan", "inf", "-inf", "negative", "zero", "over_bound", "high_below"])
    if kind == "nan":
        row[col] = float("nan")
    elif kind == "inf":
        row[col] = float("inf")
    elif kind == "-inf":
        row[col] = float("-inf")
    elif kind == "negative":
        row[col] = -1.0
    elif kind == "zero":
        row[col] = 0.0
    elif kind == "over_bound":
        row[col] = BOUND[KEYS[ref.COLS.index(col)]] * 1.5
    else:
        row["high"] = row["open"] * 0.99999995
    return row


def gen_interval(rng):
    r = rng.random()
    if r < 0.01:
        return dt.date(1965, 3, 1), dt.date(1965, 3, 20)
    if r < 0.02:
        return dt.date(1969, 12, rng.randint(20, 31)), dt.date(1970, 1, rng.randint(1, 20))
    if r < 0.035:
        return ref.SUPPORTED_FROM, ref.SUPPORTED_FROM + D(days=rng.choice([0, 1, 5, 30]))
    year = rng.choice(ref.YEARS)
    start = dt.date(year, 1, 1) + D(days=rng.randrange(365))
    end = start + D(days=rng.choice([0, 2, 6, 13, 30, 61, 90]))
    if end.year != year and end.year not in ref.HOLIDAYS:
        end = dt.date(year, 12, 31)
    if start.year not in ref.HOLIDAYS:
        start = dt.date(year, 12, 31)
    if r < 0.05:
        start, end = end + D(days=1), start
    return start, end


def gen_case(rng):
    start, end = gen_interval(rng)
    valid = start <= end and start >= ref.SUPPORTED_FROM
    exp = sorted(own_sessions(start, end)) if valid else []
    split_at = rng.randrange(len(exp) + 1) if exp else 0
    f0, f1 = rng.choice([(1.0, 1.0), (0.5, 1.0), (0.8731, 0.9512), (1.0, 1.0)])
    scale = rng.choice([1.0, 1.0, 1.0, 1e-4, 1e4])
    base = {d: make_bar(rng, d, f0 if i < split_at else f1, scale) for i, d in enumerate(exp)}

    # Exact-boundary and extreme bars (domains 4 and 6) replace a base bar.
    special = None
    if exp and rng.random() < 0.15:
        d = rng.choice(exp)
        p, q = 100.0 * 2 ** rng.randint(-3, 3), 100.0 * 2 ** rng.randint(-3, 3)
        kind = rng.choice(["factor_hi", "factor_lo", "overflow", "tiny_coherent",
                           "price_max", "price_over", "volume_max", "volume_over",
                           "split_max", "split_over", "dividend_max"])
        if kind in ("factor_hi", "factor_lo"):
            # adjClose/close = q/p; the edge is |adj/raw - q/p| == (q/p)/100, so
            # with raw p the adjusted edge is q +/- q/100 = (100 +/- 1) * 2**j,
            # exactly representable. Draw the edge or its float neighbours.
            b = flat_bar(d, p, q)
            edge = q * (101 if kind == "factor_hi" else 99) / 100
            a = rng.choice([edge, math.nextafter(edge, 0.0), math.nextafter(edge, math.inf)])
            if kind == "factor_hi":
                b["adjOpen"] = b["adjHigh"] = a
            else:
                b["adjOpen"] = b["adjLow"] = a
        elif kind == "overflow":
            # float adj/raw quotients overflow to inf; only exact arithmetic sees
            # that the open factor is a tenth of the close factor.
            b = flat_bar(d, 1e-308, 1e7)
            b["adjOpen"] = b["adjLow"] = 1e6
        elif kind == "tiny_coherent":
            b = flat_bar(d, 1e-308, 1e7)
        elif kind == "price_max":
            b = flat_bar(d, 1e7, 1e7)
        elif kind == "price_over":
            b = flat_bar(d, math.nextafter(1e7, math.inf), 1e7)
        elif kind == "volume_max":
            b = {**flat_bar(d, p, q), "volume": 10**13, "adjVolume": 1e13}
        elif kind == "volume_over":
            b = {**flat_bar(d, p, q), rng.choice(["volume", "adjVolume"]): 10**13 + 1}
        elif kind == "split_max":
            b = {**flat_bar(d, p, q), "splitFactor": 1e4}
        elif kind == "split_over":
            b = {**flat_bar(d, p, q), "splitFactor": math.nextafter(1e4, math.inf)}
        else:
            b = {**flat_bar(d, p, q), "divCash": rng.choice([1e7, math.nextafter(1e7, math.inf)])}
        base[d] = b
        special = (kind, d)
    provider = [dict(base[d]) for d in exp]

    # stored state (domain 8)
    s_kind = rng.choice(["empty", "complete", "partial", "gaps", "complete", "partial"])
    if s_kind == "empty" or not exp:
        stored_dates = []
    elif s_kind == "complete":
        stored_dates = list(exp)
    elif s_kind == "partial":
        i = rng.randrange(len(exp))
        stored_dates = exp[i:] if rng.random() < 0.5 else exp[:i]
    else:
        stored_dates = [d for d in exp if rng.random() < 0.7]
    stored = {d: to_stored(base[d]) for d in stored_dates}
    extra = rng.random()
    if stored and extra < 0.05:
        d = rng.choice(list(stored))
        stored[d] = {k: (x * 2 if k == "close" else x) for k, x in stored[d].items()}
    elif stored and extra < 0.11:
        k = rng.choice([0.5, 0.98, 1 + 2e-6])
        for d in rng.sample(list(stored), max(1, len(stored) // rng.choice([1, 3]))):
            stored[d] = {c: (x * k if c.startswith("adj_") and c != "adj_volume" else x)
                         for c, x in stored[d].items()}
    elif extra < 0.15 and valid:
        stored[start - D(days=rng.choice([1, 3, 30]))] = to_stored(make_bar(rng, start, 1.0))
    elif extra < 0.18 and exp:
        off = _non_session(rng, start, end, set(exp))
        if off:
            stored[off] = to_stored(make_bar(rng, off, 1.0))
    elif stored and extra < 0.30:                                      # domain 3
        d = rng.choice(list(stored))
        stored[d] = corrupt_stored(rng, stored[d])
    elif stored and extra < 0.46:                                      # domain 5
        # one stored price field drifts around the 1e-6 tolerance
        d = rng.choice(list(stored))
        col = rng.choice(ref.COLS[:4] + ref.COLS[5:9])
        k = rng.choice([1 + 2e-6, 1 - 2e-6, 1 + 5e-7, 1 - 5e-7, 1 + 1e-4, 1 - 1e-4])
        stored[d] = {**stored[d], col: stored[d][col] * k}
    elif stored and extra < 0.54:                                      # domain 5
        # |a - b| <= 1e-6 * max(|a|, |b|): with b = 1e6 * 2**k the edge is
        # a = 999999 * 2**k exactly; draw it, its float neighbours or a value
        # well inside, on the raw or the adjusted fields, either side larger.
        d = rng.choice(list(stored))
        k = rng.randint(-3, 3)
        big, edge = 1e6 * 2 ** k, 999999.0 * 2 ** k
        small = rng.choice([edge, math.nextafter(edge, 0.0), math.nextafter(edge, math.inf),
                            999999.5 * 2 ** k])
        raw_side = rng.random() < 0.5
        a = flat_bar(d, big, big)
        b = flat_bar(d, small if raw_side else big, big if raw_side else small)
        if rng.random() < 0.5:
            a, b = b, a
        base[d] = a
        stored[d] = to_stored(b)
        provider = [dict(base[x]) for x in exp]

    # provider mutation (domains 1, 2, 7)
    p_kind = rng.choice([
        "complete", "complete", "complete", "prefix", "suffix", "middle", "holes",
        "duplicate", "unsorted", "off_session", "outside", "field", "field", "field",
        "empty", "body", "endpoints"])
    if p_kind == "prefix" and provider:
        provider = provider[:rng.randrange(len(provider))]
    elif p_kind == "suffix" and provider:
        provider = provider[rng.randrange(1, len(provider) + 1):]
    elif p_kind == "middle" and len(provider) > 2:
        i = rng.randrange(1, len(provider) - 1)
        provider = provider[i:rng.randrange(i, len(provider))]
    elif p_kind == "holes" and len(provider) > 2:
        drop = set(rng.sample(range(1, len(provider) - 1), rng.randint(1, max(1, len(provider) // 5))))
        provider = [b for i, b in enumerate(provider) if i not in drop]
    elif p_kind == "duplicate" and provider:
        provider.append(dict(rng.choice(provider)))
    elif p_kind == "unsorted":
        rng.shuffle(provider)
    elif p_kind == "off_session" and exp:
        off = _non_session(rng, start, end, set(exp))
        if off:
            provider.append(make_bar(rng, off, f1))
    elif p_kind == "outside" and valid:
        d = rng.choice([start - D(days=rng.randint(1, 5)), end + D(days=rng.randint(1, 5))])
        if d.year in ref.HOLIDAYS:
            provider.append(make_bar(rng, d, f1))
    elif p_kind == "field" and provider:
        i = rng.randrange(len(provider))
        provider[i] = corrupt_field(rng, provider[i])
    elif p_kind == "empty":
        provider = []
    elif p_kind == "body":
        provider = rng.choice([{"detail": "x"}, "oops", None])
    elif p_kind == "endpoints" and len(provider) > 2:
        provider = [provider[0], provider[-1]]
    if p_kind != "unsorted" and isinstance(provider, list) and rng.random() < 0.2:
        rng.shuffle(provider)

    gaps = frozenset()
    if exp and rng.random() < 0.1:
        gaps = frozenset(rng.sample(exp, rng.randint(1, max(1, len(exp) // 4))))
        if isinstance(provider, list) and rng.random() < 0.7:
            provider = [b for b in provider if not (isinstance(b, dict) and isinstance(b.get("date"), str)
                        and b["date"][:10] in {g.isoformat() for g in gaps})]
    return start, end, provider, stored, gaps, (s_kind, p_kind, special)


def coherent(row):
    """Test-local predicate: finite in-bounds numbers, OHLC order, one factor."""
    o, h, lo, c, vol, ao, ah, al, ac, av, div, split = row
    nums = (o, h, lo, c, vol, ao, ah, al, ac, av, div, split)
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) for x in nums):
        return False
    if not all(math.isfinite(x) for x in nums):
        return False
    if min(o, h, lo, c, ao, ah, al, ac, split) <= 0 or min(vol, av, div) < 0:
        return False
    if max(o, h, lo, c, ao, ah, al, ac, div) > 1e7 or max(vol, av) > 1e13 or split > 1e4:
        return False
    if not (lo <= min(o, c) and max(o, c) <= h and al <= min(ao, ac) and max(ao, ac) <= ah):
        return False
    f = Fraction(ac) / Fraction(c)
    return all(abs(Fraction(a) / Fraction(r) - f) <= f / 100 for a, r in ((ao, o), (ah, h), (al, lo)))


def _code(reason):
    return re.split(r"[:= ]", reason, maxsplit=1)[0]


def signature(verdict):
    return verdict.status, _code(verdict.reason), {r[1]: tuple(r[2:]) for r in verdict.rows}


# ---------------------------------------------------------------------------
# Deterministic sweep: every field, kind and edge, enumerated rather than drawn.

_SWEEP = (dt.date(2024, 3, 27), dt.date(2024, 4, 2))       # 4 sessions; Good Friday closed
_MISSING = object()
FIELD_KINDS = {
    "nan": lambda k, x: math.nan, "inf": lambda k, x: math.inf, "-inf": lambda k, x: -math.inf,
    "negative": lambda k, x: -abs(x) - 1.0, "zero": lambda k, x: 0.0,
    "neg_tiny": lambda k, x: -5e-324, "tiny": lambda k, x: 5e-324,
    "string": lambda k, x: str(x), "na": lambda k, x: "N/A", "true": lambda k, x: True,
    "false": lambda k, x: False, "none": lambda k, x: None, "missing": lambda k, x: _MISSING,
    "huge": lambda k, x: 1e308, "bigint": lambda k, x: 10**400,
    "at_bound": lambda k, x: BOUND[k], "over_bound": lambda k, x: math.nextafter(BOUND[k], math.inf),
}
STORED_KINDS = ("nan", "inf", "-inf", "negative", "zero", "neg_tiny", "none", "huge",
                "over_bound")
DATE_VARIANTS = (
    "{d}garbage", "{d}T00:00:00+01:00", "{d}T12:00:00Z", "{d} ", " {d}", "{d}T00:00:00.000",
    "{d}T00:00:00.123Z", "{d}T00:00:00.999Z", "{d}T00:00:00.5Z", "{d}T00:00:00.000000Z",
    "{d}T00:00:00", "{d}T00:00:00.000Z", "{d}T00:00:00Z", "{d}")


def sweep_bar(d, scale=1.0):
    """Raw (100, 200, 50, 100) and adjusted at half, scaled by a power of two:
    every value and every 1% / 1e-6 edge below is exactly representable."""
    o, h, lo, c = 100.0 * scale, 200.0 * scale, 50.0 * scale, 100.0 * scale
    return {"date": d.isoformat(), "open": o, "high": h, "low": lo, "close": c, "volume": 1000,
            "adjOpen": o / 2, "adjHigh": h / 2, "adjLow": lo / 2, "adjClose": c / 2,
            "adjVolume": 2000.0, "divCash": 0.0, "splitFactor": 1.0}


def _with(bar, key, value):
    bar = dict(bar)
    if value is _MISSING:
        del bar[key]
    else:
        bar[key] = value
    return bar


def sweep_cases():
    """Yield (start, end, provider, stored, gaps, kinds) for the enumerated domains."""
    a, b = _SWEEP
    days = sorted(own_sessions(a, b))
    base = [sweep_bar(d) for d in days]
    full = {d: to_stored(x) for d, x in zip(days, base)}
    one = days[1]
    iso = one.isoformat()

    # provider: every field x every kind, with an empty and with a complete store
    for key in KEYS:
        for kind, f in FIELD_KINDS.items():
            bars = list(base)
            bars[1] = _with(bars[1], key, f(key, bars[1][key]))
            yield a, b, bars, {}, frozenset(), ("field", key, kind)
            yield a, b, bars, dict(full), frozenset(), ("field+stored", key, kind)
    # provider: every date variant, other digit scripts, impossible dates, non-strings
    variants = [t.format(d=iso) for t in DATE_VARIANTS] + [
        _digits(iso, 0xFF10), _digits(iso, 0x0660), _digits(iso, 0x0966),
        iso.replace("-", "/"), "2024-13-01", "2024-02-30", 20240328, None]
    for variant in variants:
        bars = list(base)
        bars[1] = {**bars[1], "date": variant}
        yield a, b, bars, {}, frozenset(), ("date", variant)
    # stored: every column x every kind; and the gate's high-below-open row
    for col in ref.COLS:
        key = KEYS[ref.COLS.index(col)]
        for kind in STORED_KINDS:
            stored = dict(full)
            stored[one] = {**stored[one], col: FIELD_KINDS[kind](key, stored[one][col])}
            yield a, b, list(base), stored, frozenset(), ("stored", col, kind)
    flat = flat_bar(one, 100.0, 100.0)
    yield (one, one, [flat], {one: {**to_stored(flat), "high": 99.99995}}, frozenset(),
           ("stored", "high", "99.99995"))
    # one side's OHLC order broken by 0.5%, factors within 1%
    for field in ("high", "low", "adjHigh", "adjLow"):
        yield one, one, [break_order(sweep_bar(one), field)], {}, frozenset(), ("order", field)
    # series and store shapes on the 4-session window (Good Friday inside)
    friday, before, after = dt.date(2024, 3, 29), a - D(days=1), b + D(days=1)
    shapes = {
        "complete": (base, {}, frozenset()),
        "unsorted": (base[::-1], {}, frozenset()),
        "duplicate": (base + [base[1]], {}, frozenset()),
        "hole": (base[:1] + base[2:], {}, frozenset()),
        "hole_accepted": (base[:1] + base[2:], {}, frozenset({days[1]})),
        "prefix": (base[:-1], {}, frozenset()),
        "suffix": (base[1:], {}, frozenset()),
        "endpoints": ([base[0], base[-1]], {}, frozenset()),
        "provider_off_session": (base + [sweep_bar(friday)], {}, frozenset()),
        "provider_outside": (base + [sweep_bar(after)], {}, frozenset()),
        "stored_complete": (base, dict(full), frozenset()),
        "stored_partial": (base, {d: full[d] for d in days[:2]}, frozenset()),
        "stored_absent": (base[:1] + base[2:], dict(full), frozenset()),
        "stored_outside": (base, {**full, before: to_stored(sweep_bar(before))}, frozenset()),
        "stored_off_session": (base, {**full, friday: to_stored(sweep_bar(friday))}, frozenset()),
        "stored_rebased": (base, {d: {**r, "adj_close": r["adj_close"] * 2, "adj_open": r["adj_open"] * 2,
                                      "adj_high": r["adj_high"] * 2, "adj_low": r["adj_low"] * 2}
                                  for d, r in full.items()}, frozenset()),
        "empty": ([], {}, frozenset()),
        "not_a_list": ({"detail": "x"}, {}, frozenset()),
    }
    for label, (bars, stored, gaps) in shapes.items():
        yield a, b, list(bars) if isinstance(bars, list) else bars, stored, gaps, ("shape", label)
    # 1% factor edge on each of adjOpen/adjHigh/adjLow, both sides, at/inside/outside
    for i, j in ((0, 0), (-3, 3), (3, -3), (0, -1)):
        for key, raw in (("adjOpen", "open"), ("adjHigh", "high"), ("adjLow", "low")):
            for sign in (1, -1):
                x = sweep_bar(one, 2.0 ** i)
                f = 2.0 ** (j - i)
                x.update(adjOpen=x["open"] * f, adjHigh=x["high"] * f, adjLow=x["low"] * f,
                         adjClose=x["close"] * f)
                edge = x[raw] * f * (100 + sign) / 100
                for pos, val in (("at", edge), ("in", math.nextafter(edge, x[raw] * f)),
                                 ("out", math.nextafter(edge, sign * math.inf))):
                    yield one, one, [{**x, key: val}], {}, frozenset(), ("factor", key, sign, pos, i, j)
    # 1e-6 stored/provider edge on each price column, either side larger
    for k in (-3, 0, 2):
        m = 2.0 ** k
        big = {"open": 1e6 * m, "high": 2e6 * m, "low": 5e5 * m, "close": 1e6 * m}
        x = {**flat_bar(one, 1.0, 1.0), **big,
             "adjOpen": big["open"], "adjHigh": big["high"], "adjLow": big["low"],
             "adjClose": big["close"]}
        for col in ref.COLS[:4] + ref.COLS[5:9]:
            key = KEYS[ref.COLS.index(col)]
            edge = x[key] - x[key] / 1e6
            for pos, val in (("at", edge), ("in", math.nextafter(edge, math.inf)),
                             ("out", math.nextafter(edge, 0.0))):
                small = {**x, key: val}
                yield one, one, [x], {one: to_stored(small)}, frozenset(), ("tol", col, pos, k, "stored_small")
                yield one, one, [small], {one: to_stored(x)}, frozenset(), ("tol", col, pos, k, "stored_big")
    # magnitude bounds, at and just over, on coherent bars; extreme magnitudes
    over = math.nextafter(1e7, math.inf)
    unit = flat_bar(one, 1.0, 1.0)
    for label, bar in (
            ("price_at", flat_bar(one, 1e7, 1e7)), ("price_over", flat_bar(one, over, over)),
            ("raw_over", flat_bar(one, over, 1e7)), ("adj_over", flat_bar(one, 1e7, over)),
            ("price_tiny", flat_bar(one, 5e-324, 5e-324)),
            ("volume_at", {**unit, "volume": 10**13, "adjVolume": 1e13}),
            ("volume_over", {**unit, "volume": 10**13 + 1}),
            ("adj_volume_over", {**unit, "adjVolume": math.nextafter(1e13, math.inf)}),
            ("dividend_at", {**unit, "divCash": 1e7}),
            ("dividend_over", {**unit, "divCash": over}),
            ("split_at", {**unit, "splitFactor": 1e4}),
            ("split_over", {**unit, "splitFactor": math.nextafter(1e4, math.inf)}),
            ("split_tiny", {**unit, "splitFactor": 5e-324}),
            ("volume_negative_zero", {**unit, "volume": -0.0}),
            ("overflow_factor", {**flat_bar(one, 1e-308, 1e7), "adjOpen": 1e6, "adjLow": 1e6}),
            ("tiny_coherent", flat_bar(one, 1e-308, 1e7)),
            ("gate_extreme", {**flat_bar(one, 1e-308, 1e307), "adjHigh": 1e308, "adjClose": 1e308})):
        yield one, one, [bar], {}, frozenset(), ("bound", label)
    # the calendar domain edge and every reference holiday
    for lo, hi in ((dt.date(1969, 12, 31), dt.date(1970, 1, 9)),
                   (dt.date(1965, 3, 1), dt.date(1965, 3, 31)),
                   (dt.date(1970, 1, 1), dt.date(1970, 1, 1)),
                   (dt.date(1970, 1, 1), dt.date(1970, 1, 9)),
                   (dt.date(1970, 1, 2), dt.date(1970, 1, 9))):
        bars = [sweep_bar(d) for d in sorted(own_sessions(max(lo, ref.SUPPORTED_FROM), hi))]
        yield lo, hi, bars, {}, frozenset(), ("domain", lo, hi)
        yield lo, hi, bars[1:], {}, frozenset(), ("domain-1", lo, hi)
    for year in ref.YEARS:
        for md in ref.HOLIDAYS[year].split():
            h = dt.date(year, int(md[:2]), int(md[3:]))
            lo, hi = max(h - D(days=3), dt.date(year, 1, 1)), min(h + D(days=3), dt.date(year, 12, 31))
            bars = [sweep_bar(d) for d in sorted(own_sessions(lo, hi))]
            yield lo, hi, bars, {}, frozenset(), ("holiday", h)
            yield lo, hi, bars + [sweep_bar(h)], {}, frozenset(), ("holiday-bar", h)


def corpus(cases, seed):
    """The deterministic sweep, then ``cases`` random cases from ``seed``."""
    yield from sweep_cases()
    rng = random.Random(seed)
    for _ in range(cases):
        yield gen_case(rng)


def run_corpus(validator, digest, items, outcomes=None, calendar=CAL):
    """Yield (n, kinds, problem) for every case of ``items`` where ``validator``
    disagrees with the reference or breaks an invariant; ``outcomes`` counts
    verdict codes."""
    pick = random.Random(0)
    for n, (start, end, provider, stored, gaps, kinds) in enumerate(items):
        try:
            prod = validator("FZ", (start, end), provider, stored, calendar, gaps)
        except Exception as exc:                                        # noqa: BLE001
            yield n, kinds, f"raised {type(exc).__name__}: {exc}"
            continue
        want = ref.reference_verdict(start, end, provider, stored, gaps)
        got = signature(prod)
        if outcomes is not None:
            outcomes[got[:2]] += 1
        if got != want:
            yield n, kinds, f"differs: {got[:2]} vs {want[:2]}"
            continue
        rows = got[2]
        if prod.status in v.SUCCESS_VERDICTS:
            exp = own_sessions(start, end)
            final = set(stored) | set(rows)
            if not (exp - gaps <= final <= exp) or set(rows) & set(stored):
                yield n, kinds, "session invariant"
            elif not all(coherent(r) for r in rows.values()):
                yield n, kinds, "bar invariant"
        elif prod.rows:
            yield n, kinds, "rows on a refusal"
        if prod.digest != digest(dict(reversed(list(stored.items())))):
            yield n, kinds, "digest order"
        if stored:
            d = pick.choice(list(stored))
            col = pick.choice(ref.COLS)
            other = 12345.678 if stored[d][col] != 12345.678 else 1.0   # NaN-safe change
            changed = {**stored, d: {**stored[d], col: other}}
            if digest(changed) == prod.digest:
                yield n, kinds, f"digest blind to {col}"
            if digest({k: x for k, x in stored.items() if k != d}) == prod.digest:
                yield n, kinds, "digest blind to a deleted row"


def test_validator_matches_the_reference_and_keeps_the_invariant():
    outcomes = collections.Counter()
    problems = list(run_corpus(v.validate_series, v.stored_digest, corpus(CASES, SEED), outcomes))
    assert not problems, problems[:10]
    codes = {c for _, c in outcomes}
    assert codes >= {
        "empty_interval", "interval_outside_calendar", "malformed_body", "empty_window",
        "unusable_bar", "duplicate_dates", "bars_outside_request", "off_session_bar",
        "stored_bar_invalid", "raw_differs", "stored_outside_provider_range",
        "stored_off_session", "adjusted_moved", "stored_sessions_missing", "sessions_missing",
        "verified", "inserted"}, sorted(codes)
    print(f"\nfuzz sweep={sum(1 for _ in sweep_cases())} random={CASES} seed={SEED} "
          f"total={sum(outcomes.values())} outcomes={dict(sorted(outcomes.items()))}")


def test_reference_calendar_agrees_with_exchange_calendars_on_its_years():
    """The reference's hand-written NYSE closures and exchange_calendars agree
    on every reference year; a disagreement means one of them is wrong."""
    for year in ref.YEARS:
        a, b = dt.date(year, 1, 1), dt.date(year, 12, 31)
        assert ref.ref_sessions(a, b) == own_sessions(a, b) == set(CAL.sessions(a, b)), year


@pytest.mark.parametrize("value", [
    "2026-08-10garbage", "2026-08-10T00:00:00+01:00", "2026-08-10T12:00:00Z",
    "2026-08-10 ", "2026/08/10", "２０２６-０８-１０", "٢٠٢٦-٠٨-١٠", "२०२६-०८-१०", 20260810, None,
    "2026-02-30", "2026-08-10T00:00:00", "2026-08-10T00:00:00.123Z", "2026-08-10T00:00:00.5Z",
    "2026-08-10T00:00:00.000000Z"])
def test_strict_date_syntax(value):
    assert v.parse_bar_date(value) is None


@pytest.mark.parametrize("value", ["2026-08-10", "2026-08-10T00:00:00.000Z", "2026-08-10T00:00:00Z"])
def test_accepted_date_conventions(value):
    assert v.parse_bar_date(value) == dt.date(2026, 8, 10)
