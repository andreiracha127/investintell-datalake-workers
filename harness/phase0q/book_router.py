"""L2 — the book router of the open_macro **v4.0-rev** regime engine (M-COMP4).

WHAT THIS LAYER IS
------------------
Layer 2 turns the pair ``(fiscal_state, guard_level)`` into a weight vector over a
FIXED eight-instrument universe. The quadrant is read and published, but under the
signed configuration it does NOT size anything:

    contained            -> CENTER                       (amplitude 0)
    dominance            -> expansion_c50 + B60-LQD barbell
    ALERT   (any state)  -> 0.5 * book_L2 + 0.5 * CENTER
    SEVERE  (contained)  -> contraction_c50

The universe is the six core sleeve instruments plus two credit legs at zero base
weight: LQD is the barbell's credit destination, HYG is the high-yield sensitivity
column, inert under the signed configuration.

THE CENTER
----------
``CENTER`` is the cross-quadrant MEAN of the four per-quadrant baselines that live
in the hash-pinned ``harness.phase0q.sleeve``. Those baselines are imported
READ-ONLY through ``sleeve.PER_QUADRANT_BASELINE_WEIGHTS`` / ``sleeve.QUADRANT_TO_KEY``
— this module never writes to them and never edits that file.

    SPY .3375  TLT .175  TIP .1375  GLD .1125  DBC .075  SHY .1625  LQD 0  HYG 0

COMPRESSION IS A PURE CONVEX COMBINATION
----------------------------------------
    compressed_f(q) = f * baseline[q] + (1 - f) * CENTER

No weight is dropped, nothing is renormalized, and the zero columns survive as
zeros. This DIFFERS from the pinned ``sleeve.compressed_quadrant_weights``, which
keeps only ``w > 0`` and renormalizes the survivors. At f = 0.5 the two agree
exactly over the six core tickers (no weight reaches zero and the sum is already
1, so both the filter and the renormalization are identities). They do NOT agree
on the v4 universe: the drop would delete LQD and HYG — including the barbell's
own destination — before the barbell could ever fill them.

That is the point of the dissolution: v4 emits over a fixed vector and the mandate
invariants hold NATIVELY, so none of the pinned ``_enforce_risk_cap`` /
``_enforce_defensive_floor`` / ``_renormalize`` machinery is needed to make a book
legal. Under the signed dial only two compressions are ever computed — λ = 0.5 and
amplitude 0 — and ``compressed_25``, the setting where the drop-and-renormalize
asymmetry would actually bite, is never reached.

THE BARBELL IS APPLIED EXACTLY ONCE
-----------------------------------
``bb_transform`` replaces the {TLT, TIP, SHY} sleeve by {SHY, LQD} at a 60/40
short/credit split, preserving the sleeve total. It is NOT idempotent: applying it
twice re-splits the SHY leg the first pass created and moves the book by 0.093.
The signed dominance book applies it once, here, at construction:

    SPY .39375  GLD .10625  DBC .1125  SHY .23249999999999998  LQD .155

INVARIANTS
----------
Every emitted book is checked fail-loud, in the spirit of the pinned sleeve's
constraint machinery but WITHOUT it: weights sum to 1 (±1e-9), the risk group
``SPY + DBC`` stays at or below 0.65, and the defensive group ``TLT + SHY + TIP``
stays at or above 0.20. The check REFUSES a book; it never repairs one.

This module is PURE and NON-PINNED. It imports the pinned sleeve read-only (a new
module may depend on a pinned one; the reverse would break the pin closure).
"""

from __future__ import annotations

from typing import Mapping

from harness.phase0q import sleeve as _sleeve

CORE_TICKERS: tuple[str, ...] = _sleeve.SLEEVE_TICKERS          # SPY TLT TIP GLD DBC SHY
CREDIT_TICKERS: tuple[str, ...] = ("LQD", "HYG")
UNIVERSE: tuple[str, ...] = CORE_TICKERS + CREDIT_TICKERS

QUADRANTS: tuple[str, ...] = ("recovery", "expansion", "slowdown", "contraction")

# The barbell's sleeve and its two destinations.
FI_LEGS: tuple[str, ...] = ("TLT", "TIP", "SHY")
BARBELL_SHARE_SHORT = 0.60
BARBELL_SHORT_TICKER = "SHY"
BARBELL_CREDIT_TICKER = "LQD"

# The signed dial. CONTAINED_AMPLITUDE = 0 is the whole v4 amendment: inside
# `contained` the quadrant is a published diagnostic and allocates nothing.
CONTAINED_AMPLITUDE = 0.0
DOMINANCE_COMPRESSION = 0.5
SEVERE_COMPRESSION = 0.5
DOMINANCE_QUADRANT = "expansion"
SEVERE_QUADRANT = "contraction"
ALERT_BLEND_WEIGHT = 0.50

# Mandate invariants, same bounds as the pinned sleeve's constraint baselines.
RISK_ASSETS: tuple[str, ...] = _sleeve.RISK_ASSETS               # SPY, DBC
DEFENSIVE_ASSETS: tuple[str, ...] = _sleeve.DEFENSIVE_ASSETS     # TLT, SHY, TIP
RISK_CAP = _sleeve.RISK_CAP_BASELINE                             # 0.65
DEFENSIVE_FLOOR = _sleeve.DEFENSIVE_FLOOR_BASELINE               # 0.20
WEIGHT_SUM_TOLERANCE = 1e-9

CENTER_BOOK_ID = "center"
SEVERE_BOOK_ID = "contraction_c50"
DOMINANCE_BASE_BOOK_ID = "expansion_c50"
BARBELL_SUFFIX = "+bb"


def _baseline(quadrant: str) -> dict[str, float]:
    """The pinned per-quadrant baseline, widened to the v4 universe with zeros.

    Read-only view of ``sleeve.PER_QUADRANT_BASELINE_WEIGHTS``; the credit legs
    join at zero base weight."""
    key = _sleeve.QUADRANT_TO_KEY[quadrant]
    pinned = _sleeve.PER_QUADRANT_BASELINE_WEIGHTS[key]
    return {t: float(pinned.get(t, 0.0)) for t in UNIVERSE}


BASELINE: dict[str, dict[str, float]] = {q: _baseline(q) for q in QUADRANTS}
CENTER: dict[str, float] = {
    t: sum(BASELINE[q][t] for q in QUADRANTS) / len(QUADRANTS) for t in UNIVERSE}


def compressed(quadrant: str, fraction: float) -> dict[str, float]:
    """``fraction * baseline[quadrant] + (1 - fraction) * CENTER``.

    A PURE convex combination over the fixed universe: no ``w > 0`` filter, no
    renormalization, zeros preserved. ``fraction = 0`` is the dial-0 identity and
    returns the CENTER exactly."""
    base = BASELINE[quadrant]
    return {t: fraction * base[t] + (1.0 - fraction) * CENTER[t] for t in UNIVERSE}


def blend(a: Mapping[str, float], b: Mapping[str, float],
          weight_a: float) -> dict[str, float]:
    """``weight_a * a + (1 - weight_a) * b`` over the fixed universe."""
    return {t: weight_a * a[t] + (1.0 - weight_a) * b[t] for t in UNIVERSE}


def bb_transform(book: Mapping[str, float],
                 share_short: float = BARBELL_SHARE_SHORT,
                 short_ticker: str = BARBELL_SHORT_TICKER,
                 credit_ticker: str = BARBELL_CREDIT_TICKER) -> dict[str, float]:
    """Replace the {TLT, TIP, SHY} sleeve by {short, credit}, total preserved.

    NOT IDEMPOTENT, by construction: after one pass the short leg IS part of the
    sleeve, so a second pass re-splits it. The signed dominance book applies this
    exactly once — see :data:`DOMINANCE_BOOK`."""
    out = {t: float(book[t]) for t in UNIVERSE}
    sleeve_total = sum(out[t] for t in FI_LEGS)
    for t in FI_LEGS:
        out[t] = 0.0
    if sleeve_total <= 0.0:
        return out
    out[short_ticker] = out.get(short_ticker, 0.0) + sleeve_total * share_short
    out[credit_ticker] = (out.get(credit_ticker, 0.0)
                          + sleeve_total * (1.0 - share_short))
    return out


# ─────────────────────────── the four signed books ─────────────────────────
DOMINANCE_BOOK: dict[str, float] = bb_transform(
    compressed(DOMINANCE_QUADRANT, DOMINANCE_COMPRESSION))
SEVERE_BOOK: dict[str, float] = compressed(SEVERE_QUADRANT, SEVERE_COMPRESSION)
CONTAINED_BOOKS: dict[str, dict[str, float]] = {
    q: compressed(q, CONTAINED_AMPLITUDE) for q in QUADRANTS}


# ─────────────────────────────── invariants ────────────────────────────────

class BookInvariantError(ValueError):
    """An emitted book violated a mandate invariant. Never repaired, only raised."""


def check_invariants(book: Mapping[str, float], book_id: str = "<book>") -> None:
    """Fail loud on any illegal book. Refuses; does not scale, drop or renormalize."""
    missing = [t for t in UNIVERSE if t not in book]
    if missing:
        raise BookInvariantError(f"{book_id}: missing universe columns {missing}")
    extra = [t for t in book if t not in UNIVERSE]
    if extra:
        raise BookInvariantError(f"{book_id}: weights outside the universe {extra}")
    total = sum(float(book[t]) for t in UNIVERSE)
    if abs(total - 1.0) > WEIGHT_SUM_TOLERANCE:
        raise BookInvariantError(
            f"{book_id}: weights sum to {total!r}, not 1 (±{WEIGHT_SUM_TOLERANCE})")
    negative = {t: book[t] for t in UNIVERSE if float(book[t]) < 0.0}
    if negative:
        raise BookInvariantError(f"{book_id}: negative weights {negative}")
    risk = sum(float(book[t]) for t in RISK_ASSETS)
    if risk > RISK_CAP + WEIGHT_SUM_TOLERANCE:
        raise BookInvariantError(
            f"{book_id}: risk group {'+'.join(RISK_ASSETS)} = {risk!r} exceeds the "
            f"risk cap {RISK_CAP}")
    defensive = sum(float(book[t]) for t in DEFENSIVE_ASSETS)
    if defensive < DEFENSIVE_FLOOR - WEIGHT_SUM_TOLERANCE:
        raise BookInvariantError(
            f"{book_id}: defensive group {'+'.join(DEFENSIVE_ASSETS)} = "
            f"{defensive!r} is below the defensive floor {DEFENSIVE_FLOOR}")


# ─────────────────────────── deterministic composition ─────────────────────

def l2_book(fiscal_state: str, quadrant: str | None) -> tuple[dict[str, float], str]:
    """The pre-guard (L2) book and its id.

    ``dominance`` takes the barbelled expansion book regardless of quadrant — the
    quadrant is a diagnostic there too. ``contained`` takes ``compressed_0(q)``,
    which IS the CENTER; the id still records which quadrant was read, so the
    ledger shows the diagnostic varying while the weights do not. With no quadrant
    at all the id degrades to ``center`` rather than pretending to a reading."""
    if fiscal_state == "dominance":
        return dict(DOMINANCE_BOOK), f"{DOMINANCE_BASE_BOOK_ID}{BARBELL_SUFFIX}"
    if fiscal_state != "contained":
        raise ValueError(f"l2_book: unknown fiscal state {fiscal_state!r}")
    if not isinstance(quadrant, str):
        return dict(CENTER), CENTER_BOOK_ID
    amplitude_id = int(CONTAINED_AMPLITUDE * 100)
    return dict(CONTAINED_BOOKS[quadrant]), f"compressed_{amplitude_id}({quadrant})"


def compose(fiscal_state: str | None, guard_level: str, quadrant: str | None,
            *, alert_weight: float = ALERT_BLEND_WEIGHT,
            validate: bool = True) -> tuple[dict[str, float] | None, str | None]:
    """``(fiscal_state, guard_level, quadrant)`` -> ``(weights, book_id)``.

    A month with no fiscal state has no book: ``(None, None)``. The engine does not
    publish a placeholder allocation for a month it could not route.

    ``guard_level``:
      ``off``     the L2 book, unchanged
      ``alert``   ``alert_weight * L2 + (1 - alert_weight) * CENTER`` — inside
                  ``contained`` under amplitude 0 the L2 book IS the CENTER, so the
                  blend is the identity and the guard is INERT on the portfolio
      ``severe``  the SEVERE book wholesale, replacing L2 entirely
    """
    if fiscal_state is None:
        return None, None
    book, book_id = l2_book(fiscal_state, quadrant)
    if guard_level == "severe":
        book, book_id = dict(SEVERE_BOOK), f"severe:{SEVERE_BOOK_ID}"
    elif guard_level == "alert":
        book = blend(book, CENTER, alert_weight)
        book_id = f"alert{int(alert_weight * 100)}:{book_id}"
    elif guard_level != "off":
        raise ValueError(f"compose: unknown guard level {guard_level!r}")
    if validate:
        check_invariants(book, book_id)
    return book, book_id


def emitted_books() -> dict[str, dict[str, float]]:
    """Every book the signed configuration can emit, keyed by book id.

    The complete set the invariant proof has to cover: the CENTER, the four
    ``compressed_0(q)`` ids (all equal to the CENTER), the barbelled dominance
    book, the SEVERE book, and every ALERT blend of the above."""
    out: dict[str, dict[str, float]] = {}
    for state, quadrants in (("contained", (*QUADRANTS, None)), ("dominance", (None,))):
        for quadrant in quadrants:
            for level in ("off", "alert", "severe"):
                book, book_id = compose(state, level, quadrant, validate=False)
                assert book is not None and book_id is not None
                out[book_id] = book
    return out
