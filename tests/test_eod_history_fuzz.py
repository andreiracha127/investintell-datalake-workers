"""Differential fuzz: ``validate_series`` vs an independent reference.

Seeded and reproducible. Each case draws a fixed interval, a provider series
(complete, prefix, suffix, middle slice, interior holes, duplicates, unsorted,
off-session, outside the interval, incoherent or malformed bars, malformed
dates, empty, a non-list body) and a stored state (empty, complete, partial,
gappy, outside the interval, off-session, on another adjusted basis, a raw
difference), sometimes with evidenced gaps. The production verdict, its
reason code and its rows must equal the reference's on every case; separately,
every certifying verdict must satisfy the invariant on its own terms, and the
stored-snapshot digest must change whenever the snapshot does.

``EOD_FUZZ_CASES`` sets the size (CI default 3,000; run locally at 50,000+).
``EOD_FUZZ_SEED`` sets the seed.
"""

from __future__ import annotations

import collections
import datetime as dt
import math
import os
import random
import re

import exchange_calendars as xcals
import pytest
from _eod_history_reference import reference_verdict

from src.workers import eod_history_validation as v

CASES = int(os.environ.get("EOD_FUZZ_CASES", "3000"))
SEED = int(os.environ.get("EOD_FUZZ_SEED", "20261010"))
_XNYS = xcals.get_calendar("XNYS", start="1900-01-01")
CAL = v.xnys_calendar()
FIRST, LAST = dt.date(2023, 1, 2), dt.date(2026, 9, 30)
D = dt.timedelta


def own_sessions(a, b):
    return {ts.date() for ts in _XNYS.sessions_in_range(a.isoformat(), b.isoformat())}


def _fmt_date(rng, d):
    return rng.choice([d.isoformat(), f"{d.isoformat()}T00:00:00.000Z",
                       f"{d.isoformat()}T00:00:00Z"])


def make_bar(rng, d, factor):
    o = rng.uniform(5, 300)
    c = o * rng.uniform(0.95, 1.05)
    h = max(o, c) * rng.uniform(1.0, 1.03)
    lo = min(o, c) * rng.uniform(0.97, 1.0)

    def adj(x):
        return round(x * factor, 3) if rng.random() < 0.5 else x * factor

    return {"date": _fmt_date(rng, d), "open": o, "high": h, "low": lo, "close": c,
            "volume": rng.randint(0, 10**7), "adjOpen": adj(o), "adjHigh": adj(h),
            "adjLow": adj(lo), "adjClose": adj(c), "adjVolume": float(rng.randint(0, 10**7)),
            "divCash": rng.choice([0.0, 0.0, 0.0, 0.25]), "splitFactor": rng.choice([1.0, 1.0, 2.0])}


def to_stored(bar):
    return {"open": bar["open"], "high": bar["high"], "low": bar["low"], "close": bar["close"],
            "adj_open": bar["adjOpen"], "adj_high": bar["adjHigh"], "adj_low": bar["adjLow"],
            "adj_close": bar["adjClose"]}


def _non_session(rng, start, end, exp):
    days = [start + D(days=i) for i in range((end - start).days + 1)]
    off = [d for d in days if d not in exp]
    return rng.choice(off) if off else None


def corrupt_bar(rng, bar):
    bar = dict(bar)
    kind = rng.choice([
        "swap_hl", "adj_factor", "negative", "zero", "nan", "inf", "string", "bool",
        "missing", "neg_volume", "split_zero", "adj_hl", "neg_div"])
    if kind == "swap_hl":
        bar["high"], bar["low"] = bar["low"] * 0.9, bar["high"] * 1.1
    elif kind == "adj_factor":
        bar["adjHigh"] = bar["adjHigh"] * rng.choice([1.5, 0.5, 900])
    elif kind == "negative":
        bar[rng.choice(["open", "close", "adjLow"])] = -1.0
    elif kind == "zero":
        bar[rng.choice(["open", "adjClose"])] = 0.0
    elif kind == "nan":
        bar[rng.choice(["close", "adjOpen", "volume"])] = float("nan")
    elif kind == "inf":
        bar[rng.choice(["high", "adjHigh"])] = float("inf")
    elif kind == "string":
        bar[rng.choice(["close", "open"])] = rng.choice(["N/A", "12.5", ""])
    elif kind == "bool":
        bar[rng.choice(["volume", "close"])] = True
    elif kind == "missing":
        del bar[rng.choice(["adjClose", "splitFactor", "divCash", "low"])]
    elif kind == "neg_volume":
        bar["volume"] = -5
    elif kind == "split_zero":
        bar["splitFactor"] = 0.0
    elif kind == "adj_hl":
        bar["adjLow"], bar["adjHigh"] = bar["adjHigh"] * 1.01, bar["adjLow"]
    elif kind == "neg_div":
        bar["divCash"] = -0.1
    return bar


def corrupt_date(rng, bar):
    bar = dict(bar)
    d = bar["date"][:10]
    bar["date"] = rng.choice([
        d + "garbage", d + "T00:00:00+01:00", d + "T12:00:00Z", d + " ", " " + d,
        d.replace("-", "/"), "2026-13-01", "2026-02-30", 20260810, None,
        "".join(chr(ord(ch) + 0xFEE0) if ch.isdigit() else ch for ch in d),
        d + "T00:00:00.000", d + "T00:00:00.123Z"])
    return bar


def gen_case(rng):
    if rng.random() < 0.01:
        start, end = dt.date(1899, 12, 20), dt.date(1900, 1, 20)
    else:
        start = FIRST + D(days=rng.randrange((LAST - FIRST).days))
        end = start + D(days=rng.choice([0, 2, 6, 13, 30, 61, 90]))
        if rng.random() < 0.01:
            start, end = end + D(days=1), start
    exp = sorted(own_sessions(start, end)) if start <= end and start.year >= 1900 else []
    split_at = rng.randrange(len(exp) + 1) if exp else 0
    f0, f1 = rng.choice([(1.0, 1.0), (0.5, 1.0), (0.8731, 0.9512), (1.0, 1.0)])
    base = {d: make_bar(rng, d, f0 if i < split_at else f1) for i, d in enumerate(exp)}
    provider = [dict(base[d]) for d in exp]

    # stored state
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
    if stored and extra < 0.06:
        d = rng.choice(list(stored))
        stored[d] = {k: (x * 2 if k == "close" else x) for k, x in stored[d].items()}
    elif stored and extra < 0.14:
        k = rng.choice([0.5, 0.98, 1 + 2e-6])
        for d in rng.sample(list(stored), max(1, len(stored) // rng.choice([1, 3]))):
            stored[d] = {c: (x * k if c.startswith("adj") else x) for c, x in stored[d].items()}
    elif extra < 0.18:
        stored[start - D(days=rng.choice([1, 3, 30]))] = to_stored(make_bar(rng, start, 1.0))
    elif extra < 0.21 and exp:
        off = _non_session(rng, start, end, set(exp))
        if off:
            stored[off] = to_stored(make_bar(rng, off, 1.0))
    elif stored and extra < 0.24:
        d = rng.choice(list(stored))
        stored[d] = {c: x * (1 + rng.choice([0.9e-6, 1.1e-6])) if c == "high" else x
                     for c, x in stored[d].items()}

    # provider mutation
    p_kind = rng.choice([
        "complete", "complete", "complete", "prefix", "suffix", "middle", "holes",
        "duplicate", "unsorted", "off_session", "outside", "incoherent", "bad_date",
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
        b = dict(rng.choice(provider))
        if rng.random() < 0.5:
            b["close"] = b["close"]  # identical copy
        provider.append(b)
    elif p_kind == "unsorted":
        rng.shuffle(provider)
    elif p_kind == "off_session" and exp:
        off = _non_session(rng, start, end, set(exp))
        if off:
            provider.append(make_bar(rng, off, f1))
    elif p_kind == "outside" and start <= end:
        d = rng.choice([start - D(days=rng.randint(1, 5)), end + D(days=rng.randint(1, 5))])
        provider.append(make_bar(rng, d, f1))
    elif p_kind == "incoherent" and provider:
        i = rng.randrange(len(provider))
        provider[i] = corrupt_bar(rng, provider[i])
    elif p_kind == "bad_date" and provider:
        i = rng.randrange(len(provider))
        provider[i] = corrupt_date(rng, provider[i])
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
            provider = [b for b in provider if not (isinstance(b, dict) and b.get("date")
                        and str(b["date"])[:10] in {g.isoformat() for g in gaps})]
    return start, end, provider, stored, gaps, (s_kind, p_kind)


def coherent(row):
    o, h, lo, c, vol, ao, ah, al, ac, av, div, split = row
    nums = (o, h, lo, c, vol, ao, ah, al, ac, av, div, split)
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x)
           for x in nums):
        return False
    if min(o, h, lo, c, ao, ah, al, ac, split) <= 0 or min(vol, av, div) < 0:
        return False
    if not (lo <= min(o, c) and max(o, c) <= h and al <= min(ao, ac) and max(ao, ac) <= ah):
        return False
    f = ac / c
    return all(abs(a / r - f) <= 0.01 * f for a, r in ((ao, o), (ah, h), (al, lo)))


def _code(reason):
    return re.split(r"[:= ]", reason, maxsplit=1)[0]


def test_validator_matches_the_reference_and_keeps_the_invariant():
    rng = random.Random(SEED)
    outcomes = collections.Counter()
    disagreements = []
    for n in range(CASES):
        start, end, provider, stored, gaps, kinds = gen_case(rng)
        prod = v.validate_series("FZ", (start, end), provider, stored, CAL, gaps)
        ref_status, ref_code, ref_rows = reference_verdict(start, end, provider, stored, gaps)
        prod_rows = {r[1]: tuple(r[2:]) for r in prod.rows}
        outcomes[(prod.status, _code(prod.reason))] += 1
        if (prod.status, _code(prod.reason), prod_rows) != (ref_status, ref_code, ref_rows):
            disagreements.append((n, kinds, prod.status, prod.reason, ref_status, ref_code))
            continue
        # The invariant, on its own terms (no reference involved).
        assert all(r[0] == "FZ" for r in prod.rows)
        if prod.status in v.SUCCESS_VERDICTS:
            exp = own_sessions(start, end)
            final = set(stored) | set(prod_rows)
            assert exp - gaps <= final <= exp, (n, kinds)
            assert not set(prod_rows) & set(stored), (n, kinds)
            assert all(coherent(r) for r in prod_rows.values()), (n, kinds)
            assert (prod.status == v.VERDICT_LOAD) == bool(prod_rows)
        else:
            assert prod.rows == ()
        # The digest names the snapshot exactly.
        assert prod.digest == v.stored_digest(dict(reversed(list(stored.items()))))
        if stored:
            d = rng.choice(list(stored))
            changed = dict(stored)
            changed[d] = dict(changed[d], adj_close=changed[d]["adj_close"] * 1.0000001)
            assert v.stored_digest(changed) != prod.digest
            fewer = dict(stored)
            del fewer[d]
            assert v.stored_digest(fewer) != prod.digest
    assert not disagreements, disagreements[:10]
    # Every rule was exercised.
    codes = {c for _, c in outcomes}
    assert codes >= {
        "empty_interval", "interval_outside_calendar", "malformed_body", "empty_window",
        "unusable_bar", "duplicate_dates", "bars_outside_request", "off_session_bar",
        "raw_differs", "stored_outside_provider_range", "stored_off_session",
        "adjusted_moved", "stored_sessions_missing", "sessions_missing", "verified",
        "inserted"}, sorted(codes)
    print(f"\nfuzz cases={CASES} seed={SEED} outcomes={dict(sorted(outcomes.items()))}")


@pytest.mark.parametrize("value", [
    "2026-08-10garbage", "2026-08-10T00:00:00+01:00", "2026-08-10T12:00:00Z",
    "2026-08-10 ", "2026/08/10", "２０２６-０８-１０", 20260810, None, "2026-02-30"])
def test_strict_date_syntax(value):
    assert v.parse_bar_date(value) is None


@pytest.mark.parametrize("value", ["2026-08-10", "2026-08-10T00:00:00.000Z", "2026-08-10T00:00:00Z"])
def test_accepted_date_conventions(value):
    assert v.parse_bar_date(value) == dt.date(2026, 8, 10)
