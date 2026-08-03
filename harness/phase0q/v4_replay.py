"""Deterministic fixture-driven replay of the open_macro **v4.0-rev** engine.

WHAT THIS IS
------------
The end-to-end wiring of the three v4 formula layers — L1
:mod:`src.fiscal_state`, L3 :mod:`harness.direct_activation.credit_guard`, L2
:mod:`harness.phase0q.book_router` — driven entirely by PINNED CSV FIXTURES. No
database handle, no network, no clock: given the same eight inputs it produces
the same ledger, byte for byte, forever.

That is the point. The golden ledger is the acceptance contract of the signed
M-COMP4 configuration (owner-ratified 2026-08-02); a replay that needed the live
mirror could not prove reproduction, only agreement-on-the-day.

THE EIGHT PINNED INPUTS
-----------------------
``GDP``, ``MTSDS133FMS``, ``SUBLPDCILSLGNQ``, ``M2SL``, ``CFNAI``, ``CPIAUCSL``
(``macro_data``), the ``open_macro_v03_decision_chain`` and the seven-ticker
adjusted-close frame. Their canonical digests and the combined snapshot digest
live in ``tests/fixtures/open_macro_v4/inputs/manifest.json``.

THE DECISION INDEX
------------------
Month-ends 1959-01..2026-07, the full span the inputs can reach. Months before
the fiscal panel is finite carry NO state and therefore NO book — the ledger
prints them with an empty state and NaN weights rather than a placeholder
allocation. The golden ledger is the 2006-12..2026-05 slice, the window over
which the signed configuration was measured.

THE QUADRANT
------------
Published as a diagnostic, and — under amplitude 0 — allocating nothing. Two
sources, split at the first month the decision chain exists:

* on/after chain start: ``status == 'valid'`` reads FRESH; otherwise the last
  valid reading is CARRIED for up to 3 months, then the quadrant abstains
  (``no_signal``). This mirrors the ratified carry semantics.
* before chain start: a REPLAY-ONLY proxy — CFNAI 3-month average (lagged 1) for
  growth, CPI YoY acceleration (lagged 1) for inflation — so the pre-2014 history
  has a quadrant label at all. It is a historical reconstruction, never a live
  path.

SERIALIZATION
-------------
:func:`ledger_to_csv` reproduces the golden's exact encoding: ``%.17g`` for every
number (round-trip exact for float64, so the comparison has zero tolerance), the
literal ``nan`` for a non-finite float, an EMPTY FIELD for a Python ``None``,
``True``/``False`` for booleans, and ``\\n`` line endings.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import numpy as np
import pandas as pd

from harness.direct_activation import credit_guard as _guard
from harness.phase0q import book_router as _books
from src import fiscal_state as _fiscal

ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "open_macro_v4"
INPUT_DIR = FIXTURE_DIR / "inputs"
GOLDEN_LEDGER = FIXTURE_DIR / "golden_ledger.csv"
GOLDEN_META = FIXTURE_DIR / "golden_meta.json"

# The full decision index, and the window the signed configuration was measured on.
INDEX_START = "1959-01-31"
INDEX_END = "2026-07-31"
GOLDEN_WINDOW = (pd.Timestamp("2006-12-31"), pd.Timestamp("2026-05-31"))

MACRO_SERIES = ("GDP", "MTSDS133FMS", "SUBLPDCILSLGNQ", "M2SL", "CFNAI", "CPIAUCSL")
FRAME_TICKERS = ("SPY", "TLT", "TIP", "GLD", "DBC", "SHY", "LQD")

CFNAI_SERIES_ID = "CFNAI"
CPI_SERIES_ID = "CPIAUCSL"
PROXY_MA_MONTHS = 3
PROXY_LAG_MONTHS = 1
CPI_ACCELERATION_MONTHS = 12
MAX_CHAIN_CARRY_MONTHS = 3

LEDGER_STATE_COLUMNS = (
    "deficit_gdp", "fiscal_state", "fiscal_state_age_m", "sloos_z", "sloos_age_m",
    "m2_yoy", "arm_a", "arm_b", "guard_blind", "alert", "severe_candidate",
    "severe_run_age", "stress_confirmed", "severe_emit", "severe_degraded",
    "guard_level", "quadrant", "quadrant_source", "carry_age")
LEDGER_COLUMNS = ("date", *LEDGER_STATE_COLUMNS, "book_id", *_books.UNIVERSE)

# Columns the ledger publishes ON TOP of the golden's schema: the v4 per-arm
# freshness verdict and its token. They are appended by `ledger(..., extended=True)`
# for operators; the golden comparison uses the golden's own column list.
EXTENDED_COLUMNS = ("arm_a_fresh", "arm_b_fresh", "guard_coverage")


# ────────────────────────────── fixture loading ────────────────────────────

def decision_index(start: str = INDEX_START, end: str = INDEX_END) -> pd.DatetimeIndex:
    return pd.date_range(start, end, freq="ME")


def load_macro_series(series_id: str, input_dir: Path = INPUT_DIR) -> pd.Series:
    """One pinned ``macro_data`` series as a float64 Series indexed by obs_date.

    ``float_precision='round_trip'`` is load-bearing, not tidiness: the fixtures
    are written ``%.17g`` precisely so the parse is exact, and pandas' default
    float parser is fast rather than round-trip — it reproduced ``m2_yoy`` to only
    ~1e-14, which is enough to break a zero-tolerance ledger comparison."""
    frame = pd.read_csv(input_dir / f"{series_id}.csv", float_precision="round_trip")
    return pd.Series(
        frame["value"].astype("float64").to_numpy(),
        index=pd.DatetimeIndex(pd.to_datetime(frame["obs_date"])),
        dtype="float64", name=series_id)


def load_decision_chain(input_dir: Path = INPUT_DIR) -> pd.DataFrame:
    """The pinned ``open_macro_v03_decision_chain`` (as_of, quadrant, status).

    An empty CSV field is a SQL NULL and is read back as ``None``, not as the empty
    string: ``''`` would satisfy an ``isinstance(q, str)`` check and silently latch
    a quadrant that the chain refused to emit."""
    frame = pd.read_csv(input_dir / "decision_chain.csv", keep_default_na=False)
    return pd.DataFrame(
        {
            "chain_quadrant": [q if q else None for q in frame["quadrant"]],
            "chain_status": [s if s else None for s in frame["status"]],
        },
        index=pd.DatetimeIndex(pd.to_datetime(frame["as_of"])))


def load_price_frame(input_dir: Path = INPUT_DIR,
                     tickers: tuple[str, ...] = FRAME_TICKERS) -> pd.DataFrame:
    """Daily adjusted closes, restricted to dates where EVERY ticker has a price.

    No forward fill and no imputation: a date one instrument never traded is not a
    date the frame can price a portfolio on. DBC (from 2006-02-06) is what binds."""
    frame = pd.read_csv(input_dir / "eod_prices_7.csv", float_precision="round_trip")
    frame["date"] = pd.to_datetime(frame["date"])
    daily = frame.pivot(index="date", columns="ticker", values="adj_close")
    return daily[list(tickers)].dropna(how="any")


def month_end_prices(daily: pd.DataFrame) -> pd.DataFrame:
    return daily.resample("ME").last()


# ─────────────────────────────── the quadrant ──────────────────────────────

def proxy_quadrant(cfnai_raw: pd.Series, cpi_raw: pd.Series,
                   index: pd.DatetimeIndex) -> pd.DataFrame:
    """REPLAY-ONLY pre-chain quadrant: CFNAI ma3 (growth) x CPI YoY acceleration.

    Both axes are lagged one month. Growth up / inflation up = expansion, growth up
    / inflation down = recovery, growth down / inflation up = slowdown, growth down
    / inflation down = contraction. A month where either axis is missing abstains
    (``proxy_missing``) instead of defaulting to a quadrant."""
    growth = (cfnai_raw.resample("ME").last().dropna().reindex(index)
              .rolling(PROXY_MA_MONTHS).mean().shift(PROXY_LAG_MONTHS))
    cpi_monthly = cpi_raw.resample("ME").last().dropna().reindex(index)
    cpi_yoy = 100.0 * (cpi_monthly / cpi_monthly.shift(CPI_ACCELERATION_MONTHS) - 1.0)
    acceleration = (cpi_yoy - cpi_yoy.shift(CPI_ACCELERATION_MONTHS)).shift(
        PROXY_LAG_MONTHS)
    return pd.DataFrame({"growth": growth, "inflation_acceleration": acceleration},
                        index=index)


def quadrant_series(chain: pd.DataFrame, proxy: pd.DataFrame,
                    index: pd.DatetimeIndex,
                    max_carry_months: int = MAX_CHAIN_CARRY_MONTHS) -> pd.DataFrame:
    """The published quadrant diagnostic: chain where it exists, proxy before it.

    Returns ``quadrant`` (or ``None``), ``quadrant_source`` in
    ``chain_fresh`` | ``chain_carry`` | ``no_signal`` | ``proxy`` | ``proxy_missing``,
    and ``carry_age`` — how many months since the last fresh valid chain reading.
    """
    aligned = chain.reindex(index)
    status = aligned["chain_status"]
    chain_start = status.first_valid_index()

    quadrants: list[str | None] = []
    sources: list[str] = []
    carry_ages: list[int] = []
    last_valid: str | None = None
    age = 0
    for date in index:
        if chain_start is not None and date >= chain_start:
            row_status = status.at[date]
            row_quadrant = aligned.at[date, "chain_quadrant"]
            if row_status == "valid" and isinstance(row_quadrant, str):
                last_valid, age = row_quadrant, 0
                quadrants.append(row_quadrant)
                sources.append("chain_fresh")
                carry_ages.append(0)
            elif last_valid is not None and age < max_carry_months:
                age += 1
                quadrants.append(last_valid)
                sources.append("chain_carry")
                carry_ages.append(age)
            else:
                if last_valid is not None:
                    age += 1
                quadrants.append(None)
                sources.append("no_signal")
                carry_ages.append(age)
            continue
        growth = proxy.at[date, "growth"]
        inflation = proxy.at[date, "inflation_acceleration"]
        if not (np.isfinite(growth) and np.isfinite(inflation)):
            quadrants.append(None)
            sources.append("proxy_missing")
            carry_ages.append(0)
            continue
        up_growth, up_inflation = growth >= 0, inflation >= 0
        quadrant = (("expansion" if up_inflation else "recovery") if up_growth
                    else ("slowdown" if up_inflation else "contraction"))
        quadrants.append(quadrant)
        sources.append("proxy")
        carry_ages.append(0)
    return pd.DataFrame(
        {"quadrant": pd.Series(quadrants, index=index, dtype="object"),
         "quadrant_source": pd.Series(sources, index=index, dtype="object"),
         "carry_age": carry_ages},
        index=index)


# ──────────────────────────────── the replay ───────────────────────────────

def ledger(input_dir: Path = INPUT_DIR,
           index: pd.DatetimeIndex | None = None,
           *, extended: bool = False) -> pd.DataFrame:
    """Run L1 + L3 + L2 month by month and return the full ledger frame.

    ``extended`` appends the v4-only freshness columns (``arm_a_fresh``,
    ``arm_b_fresh``, ``guard_coverage``) after the golden's schema; the golden
    comparison uses the golden's own column list, so the extra diagnostics can
    never smuggle themselves into the pinned contract.
    """
    idx = decision_index() if index is None else index
    raw = {sid: load_macro_series(sid, input_dir) for sid in MACRO_SERIES}
    chain = load_decision_chain(input_dir)
    daily = load_price_frame(input_dir)
    monthly = month_end_prices(daily)

    # L1 — the fiscal router.
    l1 = _fiscal.fiscal_panel(raw[_fiscal.MTS_SERIES_ID], raw[_fiscal.GDP_SERIES_ID], idx)

    # L3 — the credit guard, including A8's price confirmation.
    stress = _guard.stress_confirmed_series(monthly[_guard.STRESS_TICKER], idx)
    l3 = _guard.build_guard(
        sloos_raw=raw[_guard.SLOOS_SERIES_ID], m2_raw=raw[_guard.M2_SERIES_ID],
        index=idx, fiscal_state=l1["fiscal_state"],
        fiscal_state_age_m=l1["fiscal_state_age_m"], stress_confirmed=stress)

    quadrants = quadrant_series(
        chain, proxy_quadrant(raw[CFNAI_SERIES_ID], raw[CPI_SERIES_ID], idx), idx)

    frame = pd.concat([l1.drop(columns=["fiscal_boundary"]), l3, quadrants], axis=1)
    frame["fiscal_boundary"] = l1["fiscal_boundary"]

    # L2 — the book, composed deterministically from the published state.
    rows: list[list[float]] = []
    book_ids: list[str | None] = []
    for date in idx:
        book, book_id = _books.compose(
            frame.at[date, "fiscal_state"], frame.at[date, "guard_level"],
            frame.at[date, "quadrant"])
        book_ids.append(book_id)
        rows.append([np.nan] * len(_books.UNIVERSE) if book is None
                    else [book[t] for t in _books.UNIVERSE])
    frame["book_id"] = pd.Series(book_ids, index=idx, dtype="object")
    weights = pd.DataFrame(rows, index=idx, columns=list(_books.UNIVERSE))
    frame = pd.concat([frame, weights], axis=1)

    columns = list(LEDGER_STATE_COLUMNS) + ["book_id"] + list(_books.UNIVERSE)
    if extended:
        columns += list(EXTENDED_COLUMNS)
    return frame[columns]


def golden_window(frame: pd.DataFrame,
                  window: tuple[pd.Timestamp, pd.Timestamp] = GOLDEN_WINDOW) -> pd.DataFrame:
    low, high = window
    return frame.loc[low:high]


# ────────────────────────── canonical serialization ────────────────────────

_QUADRANT_COLUMNS = ("quadrant", "quadrant_source")


def _g17(value) -> str:
    return f"{float(value):.17g}"


def _field(value, column: str) -> str:
    """The golden's encoding, restated: booleans first, then None, then numbers.

    ``None`` is an EMPTY field; a non-finite float is the literal ``nan``. The two
    are not the same fact — one is "the engine abstained", the other is "the
    arithmetic had no answer" — and collapsing them would erase the distinction the
    ledger exists to record."""
    if isinstance(value, (bool, np.bool_)):
        return "True" if value else "False"
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float) and not np.isfinite(value):
        return "" if column in _QUADRANT_COLUMNS else _g17(value)
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        return _g17(value)
    return str(value)


def ledger_to_csv(frame: pd.DataFrame,
                  columns: tuple[str, ...] = LEDGER_COLUMNS) -> str:
    """Serialize a ledger frame in the golden's exact encoding (``\\n`` endings)."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(list(columns))
    body = [c for c in columns if c != "date"]
    for date in frame.index:
        row = [f"{date:%Y-%m-%d}"]
        for column in body:
            row.append(_field(frame.at[date, column], column))
        writer.writerow(row)
    return buffer.getvalue()


def replay_golden_csv(input_dir: Path = INPUT_DIR) -> str:
    """The one call the acceptance test makes: fixtures in, golden CSV text out."""
    return ledger_to_csv(golden_window(ledger(input_dir)))


def main() -> int:
    text = replay_golden_csv()
    import hashlib
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    expected = hashlib.sha256(GOLDEN_LEDGER.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    print(f"replayed rows      {len(text.splitlines()) - 1}")
    print(f"replay sha256      {digest}")
    print(f"golden  sha256     {expected}")
    print("VERDICT            " + ("MATCH" if digest == expected else "MISMATCH"))
    return 0 if digest == expected else 1


if __name__ == "__main__":
    raise SystemExit(main())
