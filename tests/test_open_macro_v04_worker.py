"""Unit tests for the open_macro v4.0-rev monthly runtime worker.

No real Postgres. The gates that precede the connection (the formulation freeze)
need none, and everything downstream is driven through a duck-typed fake conn that
serves the PINNED FIXTURES as if they were the database. That is the point of the
central test here: the worker is run END TO END over the same eight inputs the
golden ledger was cut from, and what it would publish is compared to
``golden_ledger.csv`` byte for byte.

Two things that check are NOT: it is not "the worker agrees with a second
implementation" (there is only one — ``v4_replay.build_ledger``), and it is not "the
numbers look right" (the comparison has zero tolerance, on the golden's own
``%.17g`` encoding).
"""

from __future__ import annotations

import csv
import datetime as _dt
import decimal
import hashlib
import json
import re
from pathlib import Path

import pandas as pd
import pytest

import src.workers.open_macro_v04 as w
from harness.phase0q import book_router as books
from harness.phase0q import v4_replay as replay
from src import fiscal_state as fiscal

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "open_macro_v4"
INPUTS = FIXTURES / "inputs"
SCHEMAS = ROOT / "schemas"

GOLDEN_LEDGER_SHA256 = "02a3b5ee791a8712eab0c40122d4491e513aa5886a6bd8248628999c6abd6cd3"
INPUT_SNAPSHOT_SHA256 = "42253855212736a948c2ae865676c3511aa271e3be698b91ba7fa9537b79da28"

# The run date that makes the pinned snapshot the WHOLE input: every fixture series
# ends on or before 2026-07-31, so the worker's `obs_date <= horizon` bound excludes
# nothing and the recomputed input digest must equal the pinned snapshot digest.
RUN_TODAY = _dt.date(2026, 8, 3)
LATEST = _dt.date(2026, 7, 31)
FAKE_COMMIT = "0" * 40


# --------------------------------------------------------------------------- #
# Fixture loading — the fake database's contents
# --------------------------------------------------------------------------- #
def _macro_rows(series_id: str) -> list[tuple]:
    """``(obs_date, value)`` as psycopg would hand them back.

    The values are full-precision floats, i.e. this fake stands in for a mirror that
    kept them. Production's ``macro_data.value`` is NUMERIC(24,6) and will therefore
    produce a DIFFERENT digest from the same recipe — stated in the worker docstring,
    and not something this test pretends away."""
    with INPUTS.joinpath(f"{series_id}.csv").open(newline="", encoding="utf-8") as fh:
        return [(_dt.date.fromisoformat(r["obs_date"]), float(r["value"]))
                for r in csv.DictReader(fh)]


def _price_rows() -> list[tuple]:
    with INPUTS.joinpath("eod_prices_7.csv").open(newline="", encoding="utf-8") as fh:
        return [(r["ticker"], _dt.date.fromisoformat(r["date"]), float(r["adj_close"]))
                for r in csv.DictReader(fh)]


def _chain_rows() -> list[tuple]:
    """``(as_of, quadrant, status, candidate_confidence)``.

    The fixture carries no confidence column, so a DETERMINISTIC synthetic one is
    attached — the point is to watch it be carried with a carried quadrant and
    dropped where no chain reading is in force, which needs values that differ per
    month."""
    with INPUTS.joinpath("decision_chain.csv").open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return [(_dt.date.fromisoformat(r["as_of"]), r["quadrant"] or None,
             r["status"] or None, round(0.50 + 0.001 * i, 6))
            for i, r in enumerate(rows)]


def _catalog_rows() -> list[tuple]:
    """information_schema.columns exactly as the committed DDL produces it."""
    return [(table, column, *signature)
            for table in sorted(w.EXPECTED_COLUMNS)
            for column, signature in w.EXPECTED_COLUMNS[table].items()]


# --------------------------------------------------------------------------- #
# The fake conn
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self.rowcount = -1
        self._rows: list = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None) -> None:
        self.conn.executed.append((sql, params))
        response = self.conn.responder(sql, params)
        self.rowcount = response.get("rowcount", 1)
        self._rows = response.get("rows", [])

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, responder) -> None:
        self.executed: list = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.responder = responder

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.responder.transaction_commit()

    def rollback(self) -> None:
        self.rollbacks += 1
        self.responder.transaction_rollback()

    def close(self) -> None:
        self.closed = True


class FakeDatabase:
    """A responder that serves the pinned fixtures and records every upsert."""

    def __init__(self, *, existing_as_of: set | None = None,
                 invalidated: set | None = None,
                 catalog: list[tuple] | None = None,
                 macro_override: dict[str, list[tuple]] | None = None) -> None:
        self.existing = existing_as_of or set()
        self.invalidated = invalidated or set()
        self.catalog = _catalog_rows() if catalog is None else catalog
        self.macro_override = macro_override or {}
        self.decisions: list[dict] = []
        self.allocations: list[dict] = []
        self.captures: list[dict] = []
        self._committed = ([], [], [])

    def transaction_commit(self) -> None:
        self._committed = (
            [dict(row) for row in self.decisions],
            [dict(row) for row in self.allocations],
            [dict(row) for row in self.captures],
        )

    def transaction_rollback(self) -> None:
        self.decisions, self.allocations, self.captures = (
            [dict(row) for row in rows] for rows in self._committed
        )

    # -- the served tables ------------------------------------------------- #
    def _macro(self, series_id: str, horizon) -> list[tuple]:
        if series_id in self.macro_override:
            return self.macro_override[series_id]
        return [r for r in _macro_rows(series_id) if r[0] <= horizon]

    def __call__(self, sql, params=None):
        params = params or {}
        if "pg_try_advisory_lock" in sql:
            return {"rows": [(True,)]}
        if "pg_advisory_unlock" in sql:
            return {"rows": [(1,)]}
        if sql.startswith("SET search_path"):
            return {"rows": []}
        if sql.startswith("SHOW search_path"):
            return {"rows": [("public",)]}
        if "information_schema.columns" in sql:
            return {"rows": list(self.catalog)}
        if sql is w.MACRO_SERIES_SQL:
            return {"rows": self._macro(params["series_id"], params["horizon"])}
        if sql is w.PRICES_SQL:
            return {"rows": [r for r in _price_rows() if r[1] <= params["horizon"]]}
        if sql is w.CHAIN_SQL:
            return {"rows": [r for r in _chain_rows() if r[0] <= params["horizon"]]}
        if sql is w.EXISTING_BASIS_SQL:
            return {"rows": [(d, "live") for d in sorted(self.existing)]}
        if sql is w.DECISION_UPSERT_SQL:
            if params["as_of"] in self.invalidated:
                return {"rowcount": 0}
            self.decisions.append(dict(params))
            return {"rowcount": 1}
        if sql is w.ALLOCATION_UPSERT_SQL:
            if params["as_of"] in self.invalidated:
                return {"rowcount": 0}
            self.allocations.append(dict(params))
            return {"rowcount": 1}
        if sql is w.CAPTURE_INSERT_SQL:
            existing = next((row for row in self.captures
                             if (row["as_of"], row["series_id"])
                             == (params["as_of"], params["series_id"])), None)
            if existing is not None:
                return {"rowcount": 0, "rows": []}
            self.captures.append(dict(params))
            columns = ("series_digest_sha256", "row_count", "min_obs_date",
                       "max_obs_date", "producer_run_id", "global_input_digest_sha256")
            return {"rows": [tuple(params[column] for column in columns)]}
        if sql is w.CAPTURE_READ_SQL:
            row = next((row for row in self.captures
                        if (row["as_of"], row["series_id"])
                        == (params["as_of"], params["series_id"])), None)
            columns = ("series_digest_sha256", "row_count", "min_obs_date",
                       "max_obs_date", "producer_run_id", "global_input_digest_sha256")
            return {"rows": [] if row is None else [tuple(row[column] for column in columns)]}
        if sql is w.CAPTURE_COUNT_SQL:
            return {"rows": [(sum(row["as_of"] == params["as_of"]
                                  for row in self.captures),)]}
        if sql.startswith("SELECT count(*)"):
            table = "open_macro_v04_decisions" if "v04_decisions" in sql else None
            n = len(self.decisions) if table else len(self.allocations)
            return {"rows": [(n,)]}
        if sql is w.READBACK_DECISION_SQL:
            return {"rows": [self._readback(self.decisions, w._DECISION_COLUMNS,
                                            params["as_of"])]}
        if sql is w.READBACK_ALLOCATION_SQL:
            return {"rows": [self._readback(self.allocations, w._ALLOCATION_COLUMNS,
                                            params["as_of"])]}
        raise AssertionError(f"unexpected SQL in the worker: {sql[:120]!r}")

    @staticmethod
    def _readback(rows: list[dict], columns, as_of):
        """Re-read a written row through the driver's TYPE SHIFTS, not as the dict
        that went in: NUMERIC comes back as Decimal. A readback that handed the exact
        Python object back would never catch a float8 truncation, which is the whole
        reason post_write_verify exists."""
        row = next(r for r in rows if r["as_of"] == as_of)
        return tuple(row[c] for c in columns) + ("published", "valid")


def run_worker(**kwargs) -> tuple[dict, FakeDatabase]:
    database = FakeDatabase(**kwargs)
    conn = _FakeConn(database)
    database.conn = conn
    original = w.connect
    w.connect = lambda dsn: conn                    # noqa: E731 - test seam
    try:
        stats = w.run("postgresql://fake", today=RUN_TODAY)
    finally:
        w.connect = original
    return stats, database


@pytest.fixture(autouse=True)
def _pinned_commit(monkeypatch):
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", FAKE_COMMIT)
    monkeypatch.delenv(w.AS_OF_ENV, raising=False)


@pytest.fixture(scope="module")
def published() -> tuple[dict, FakeDatabase]:
    import os
    os.environ["RAILWAY_GIT_COMMIT_SHA"] = FAKE_COMMIT
    os.environ.pop(w.AS_OF_ENV, None)
    return run_worker()


# =========================================================================== #
# 1. The formulation freeze gate                                              #
# =========================================================================== #
def test_the_committed_freeze_passes_and_returns_its_canonical_digest() -> None:
    digest = w.verify_formulation_freeze()
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    assert digest == freeze["pins"]["formulation_sha256"]
    assert len(digest) == 64


def test_a_truncated_pin_manifest_is_refused() -> None:
    """A manifest that simply OMITS a formula module must not pass by iterating only
    the keys it happens to carry."""
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    freeze["pins"]["modules"].pop("harness/phase0q/book_router.py")
    with pytest.raises(w.OpenMacroV04Error) as excinfo:
        w.verify_formulation_freeze(freeze)
    assert "book_router.py" in str(excinfo.value)
    assert "missing=" in str(excinfo.value)


def test_an_extra_pinned_module_is_refused() -> None:
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    freeze["pins"]["modules"]["src/db.py"] = "0" * 64
    with pytest.raises(w.OpenMacroV04Error, match="unexpected="):
        w.verify_formulation_freeze(freeze)


def test_a_tampered_module_sha_is_refused() -> None:
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    freeze["pins"]["modules"]["src/fiscal_state.py"] = "f" * 64
    with pytest.raises(w.OpenMacroV04Error) as excinfo:
        w.verify_formulation_freeze(freeze)
    assert "src/fiscal_state.py" in str(excinfo.value)
    assert "not the frozen one" in str(excinfo.value)


def test_a_formulation_block_edited_without_re_deriving_its_digest_is_refused() -> None:
    """The module pins alone cannot catch this: the FILES are untouched and only the
    artifact's own statement of the formula moved."""
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    freeze["gates"]["G2"]["bound"] = -0.99
    with pytest.raises(w.OpenMacroV04Error, match="formulation_sha256"):
        w.verify_formulation_freeze(freeze)


def test_a_freeze_missing_a_digest_scope_block_is_refused() -> None:
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    freeze.pop("books")
    with pytest.raises(w.OpenMacroV04Error, match="missing the blocks"):
        w.verify_formulation_freeze(freeze)


def test_the_pinned_module_set_is_the_v4_formula_closure() -> None:
    assert w.FORMULA_MODULES == (
        "src/fiscal_state.py",
        "harness/direct_activation/credit_guard.py",
        "harness/phase0q/book_router.py",
        "harness/phase0q/v4_replay.py")
    for relative in w.FORMULA_MODULES:
        assert (ROOT / relative).is_file(), relative


def test_the_published_weight_columns_are_the_universe_minus_hyg() -> None:
    w.check_universe_coverage()
    published = {t for _, t in w.WEIGHT_COLUMNS}
    assert published == set(books.UNIVERSE) - {"HYG"}
    assert "HYG" in books.UNIVERSE


# =========================================================================== #
# 2. Inputs and catalog: what a missing thing does                            #
# =========================================================================== #
@pytest.mark.parametrize("series_id", w.REQUIRED_SERIES)
def test_a_missing_required_series_refuses_and_names_it(series_id: str) -> None:
    with pytest.raises(w.OpenMacroV04Error) as excinfo:
        run_worker(macro_override={series_id: []})
    message = str(excinfo.value)
    assert series_id in message
    assert "macro_ingestion" in message, "the error must say what to run"


@pytest.mark.parametrize("series_id", w.PROXY_SERIES)
def test_a_missing_proxy_series_degrades_instead_of_refusing(series_id: str) -> None:
    """The pre-chain proxy is a REPLAY reconstruction, never a live path. Its absence
    costs the pre-2014 months their quadrant label and nothing else."""
    stats, database = run_worker(macro_override={series_id: []})
    assert stats["status"] == "published"
    sources = {r["quadrant_source"] for r in database.decisions
               if r["as_of"] < _dt.date(2014, 3, 31)}
    assert sources == {"proxy_missing"}


def test_an_absent_catalog_refuses_before_any_write() -> None:
    with pytest.raises(w.OpenMacroV04Error) as excinfo:
        run_worker(catalog=[])
    message = str(excinfo.value)
    assert "open_macro_v04_decisions" in message
    assert "open_macro_v04_allocations" in message
    assert "apply schemas/" in message


def test_a_drifted_column_signature_refuses() -> None:
    """CHAR(40) where CHAR(64) was declared is exactly the drift a name-only gate
    waves through."""
    catalog = [r if r[1] != "input_digest_sha256"
               else (r[0], r[1], "character", 40, r[4], r[5])
               for r in _catalog_rows()]
    with pytest.raises(w.OpenMacroV04Error, match="input_digest_sha256"):
        run_worker(catalog=catalog)


def test_an_unexpected_extra_column_refuses() -> None:
    catalog = _catalog_rows() + [
        ("open_macro_v04_decisions", "high_pressure", "boolean", None, "YES", None)]
    with pytest.raises(w.OpenMacroV04Error, match="high_pressure"):
        run_worker(catalog=catalog)


def test_run_never_issues_a_ddl_statement(published) -> None:
    """Schema lifecycle belongs to the operator. A worker that quietly CREATEs its
    own table would turn a missing migration into a partial catalog behind an abort,
    which is exactly what the v03 pair learned not to do."""
    _, database = published
    for sql, _ in database.conn.executed:
        head = sql.lstrip().split(None, 1)[0].upper()
        assert head not in ("CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT"), sql[:120]


# =========================================================================== #
# 3. PARITY — the worker reproduces the signed ledger byte for byte           #
# =========================================================================== #
def _golden_text() -> str:
    return FIXTURES.joinpath("golden_ledger.csv").read_bytes().replace(
        b"\r\n", b"\n").decode("utf-8")


def _worker_ledger() -> pd.DataFrame:
    """The ledger through the WORKER's own call, fed the pinned fixtures."""
    series = {sid: pd.Series([v for _, v in _macro_rows(sid)],
                             index=pd.DatetimeIndex([d for d, _ in _macro_rows(sid)]),
                             dtype="float64", name=sid)
              for sid in (*w.REQUIRED_SERIES, *w.PROXY_SERIES)}
    rows = _price_rows()
    frame = pd.DataFrame({"ticker": [r[0] for r in rows],
                          "date": pd.to_datetime([r[1] for r in rows]),
                          "adj_close": [r[2] for r in rows]})
    prices = frame.pivot(index="date", columns="ticker",
                         values="adj_close")[list(w.BOOK_TICKERS)].dropna(how="any")
    chain_rows = _chain_rows()
    chain = pd.DataFrame(
        {"chain_quadrant": [r[1] for r in chain_rows],
         "chain_status": [r[2] for r in chain_rows]},
        index=pd.DatetimeIndex([pd.Timestamp(r[0]) for r in chain_rows]))
    return w.build_worker_ledger(series, chain, prices, LATEST)


def test_the_worker_ledger_reproduces_the_golden_ledger_byte_for_byte() -> None:
    """ZERO tolerance, on the golden's own %.17g encoding.

    This is the load-bearing test of the whole worker. It does not check that the
    worker agrees with a second implementation — there is only one, and both callers
    reach it through v4_replay.build_ledger. It checks that the code path the WORKER
    drives, over the inputs the ledger was signed on, produces the signed bytes."""
    replayed = w.ledger_csv(replay.golden_window(_worker_ledger()))
    golden = _golden_text()
    assert replayed.splitlines()[0] == golden.splitlines()[0]
    assert replayed == golden
    assert hashlib.sha256(replayed.encode("utf-8")).hexdigest() == GOLDEN_LEDGER_SHA256


def test_the_published_rows_are_the_golden_ledger(published) -> None:
    """The parity claim, restated on the ROWS rather than on the ledger frame.

    The frame test proves the engine reproduces; this one proves nothing is lost or
    reshaped between the engine and the two upserts — the projection into columns is
    where a transcription error would actually live."""
    _, database = published
    with FIXTURES.joinpath("golden_ledger.csv").open(newline="", encoding="utf-8") as fh:
        golden = {r["date"]: r for r in csv.DictReader(fh)}
    decisions = {r["as_of"].isoformat(): r for r in database.decisions}
    allocations = {r["as_of"].isoformat(): r for r in database.allocations}
    assert set(golden) <= set(decisions), "every golden month must be published"
    assert len(golden) == 234

    for date, want in golden.items():
        got = decisions[date]
        assert got["fiscal_state"] == want["fiscal_state"], date
        assert got["fiscal_state_age_m"] == int(want["fiscal_state_age_m"]), date
        assert got["guard_level"] == want["guard_level"], date
        assert got["arm_a"] is (want["arm_a"] == "True"), date
        assert got["arm_b"] is (want["arm_b"] == "True"), date
        assert got["severe_run_age"] == int(want["severe_run_age"]), date
        assert got["severe_degraded"] is (want["severe_degraded"] == "True"), date
        assert got["stress_confirmed"] is (want["stress_confirmed"] == "True"), date
        assert (got["quadrant"] or "") == want["quadrant"], date
        assert got["quadrant_source"] == want["quadrant_source"], date
        assert got["carry_age"] == int(want["carry_age"]), date
        assert f"{float(got['deficit_gdp']):.17g}" == want["deficit_gdp"], date

        book = allocations[date]
        assert book["book_id"] == want["book_id"], date
        for column, ticker in w.WEIGHT_COLUMNS:
            assert f"{float(book[column]):.17g}" == want[ticker], (date, ticker)
        assert want["HYG"] == "0", date   # the column the schema does not carry


def test_truncating_the_index_at_the_run_month_changes_nothing_before_it() -> None:
    """Every transform is causal — trailing windows, backward shifts, forward state
    walks — so a shorter index must not move an earlier month. If it did, the
    worker's answer would depend on WHEN it ran."""
    short = w.build_worker_ledger(
        *_engine_inputs(), _dt.date(2026, 5, 31))
    full = _worker_ledger()
    assert w.ledger_csv(short.loc[:pd.Timestamp("2026-05-31")]) == w.ledger_csv(
        full.loc[:pd.Timestamp("2026-05-31")])


def _engine_inputs():
    series = {sid: pd.Series([v for _, v in _macro_rows(sid)],
                             index=pd.DatetimeIndex([d for d, _ in _macro_rows(sid)]),
                             dtype="float64", name=sid)
              for sid in (*w.REQUIRED_SERIES, *w.PROXY_SERIES)}
    rows = _price_rows()
    frame = pd.DataFrame({"ticker": [r[0] for r in rows],
                          "date": pd.to_datetime([r[1] for r in rows]),
                          "adj_close": [r[2] for r in rows]})
    prices = frame.pivot(index="date", columns="ticker",
                         values="adj_close")[list(w.BOOK_TICKERS)].dropna(how="any")
    chain_rows = _chain_rows()
    chain = pd.DataFrame(
        {"chain_quadrant": [r[1] for r in chain_rows],
         "chain_status": [r[2] for r in chain_rows]},
        index=pd.DatetimeIndex([pd.Timestamp(r[0]) for r in chain_rows]))
    return series, chain, prices


def test_the_input_digest_recipe_reproduces_the_pinned_snapshot(published) -> None:
    """Fed the pinned snapshot, the worker's digest IS the pinned snapshot digest.

    That is what makes the digest an input pin and not a decoration: the recipe is
    the fixture manifest's, and it is exercised here through the worker's own
    function rather than through a copy of it."""
    stats, _ = published
    assert stats["input_digest"] == INPUT_SNAPSHOT_SHA256
    manifest = json.loads(INPUTS.joinpath("manifest.json").read_text(encoding="utf-8"))
    for series_id in ("GDP", "MTSDS133FMS", "SUBLPDCILSLGNQ", "M2SL", "CFNAI",
                      "CPIAUCSL"):
        assert stats["input_digest_parts"][series_id] == \
            manifest["series"][series_id]["sha256"], series_id
    assert stats["input_digest_parts"]["chain"] == manifest["chain"]["sha256"]
    assert stats["input_digest_parts"]["prices"] == manifest["prices"]["sha256"]


def test_the_fiscal_boundary_restatement_agrees_with_the_router() -> None:
    """`fiscal_boundary` is recomputed in the worker because the ledger's published
    columns do not carry it. It must be the SAME fact, not a near one."""
    series, _, _ = _engine_inputs()
    index = pd.date_range(replay.INDEX_START, pd.Timestamp(LATEST), freq="ME")
    routed = fiscal.fiscal_panel(series[fiscal.MTS_SERIES_ID],
                                 series[fiscal.GDP_SERIES_ID], index)
    for date, row in routed.iterrows():
        assert w.fiscal_boundary(float(row["deficit_gdp"])) == bool(
            row["fiscal_boundary"]), date


# =========================================================================== #
# 4. Publication: columns, exactness, basis, tokens, invariants               #
# =========================================================================== #
def test_the_run_publishes_every_month_of_the_window(published) -> None:
    stats, database = published
    assert stats["status"] == "published"
    assert stats["latest_as_of"] == LATEST.isoformat()
    months = [r["as_of"] for r in database.decisions]
    assert months[0] == w.PUBLISH_START
    assert months[-1] == LATEST
    assert months == sorted(months) and len(set(months)) == len(months)
    # 2006-12 .. 2026-07 inclusive
    assert len(months) == 236
    assert stats["n_published"] == 236
    assert stats["n_months_without_state"] == 0
    assert len(database.allocations) == len(database.decisions)


def test_every_published_column_is_written_exactly_once(published) -> None:
    _, database = published
    for row in database.decisions:
        assert set(row) == set(w._DECISION_COLUMNS)
    for row in database.allocations:
        assert set(row) == set(w._ALLOCATION_COLUMNS)


def test_numeric_columns_are_written_as_exact_decimals(published) -> None:
    """A raw float8 parameter is cast to NUMERIC through 15 significant digits, which
    silently truncated a v03 confidence in production on 2026-07-06. Decimal(repr(x))
    is the shortest round-tripping string, so the stored value IS the computed one."""
    ledger = _worker_ledger()
    for row in database_rows(published, "allocations"):
        date = pd.Timestamp(row["as_of"])
        for column, ticker in w.WEIGHT_COLUMNS:
            value = row[column]
            assert isinstance(value, decimal.Decimal), column
            assert float(value) == float(ledger.at[date, ticker])
    for row in database_rows(published, "decisions"):
        date = pd.Timestamp(row["as_of"])
        assert isinstance(row["deficit_gdp"], decimal.Decimal)
        assert float(row["deficit_gdp"]) == float(ledger.at[date, "deficit_gdp"])


def database_rows(published, which: str) -> list[dict]:
    _, database = published
    return database.decisions if which == "decisions" else database.allocations


def test_only_the_current_month_is_live_and_only_when_it_is_new(published) -> None:
    stats, database = published
    live = [r["as_of"] for r in database.decisions
            if r["decision_basis"] == w.BASIS_LIVE]
    assert live == [LATEST]
    assert stats["n_live"] == 1
    assert stats["n_bootstrap"] == stats["n_published"] - 1


def test_new_live_month_captures_exactly_four_horizon_bounded_inputs(published) -> None:
    stats, database = published
    assert {row["series_id"] for row in database.captures} == set(w.REQUIRED_SERIES)
    assert {row["as_of"] for row in database.captures} == {LATEST}
    assert {row["producer_run_id"] for row in database.captures} == {stats["run_id"]}
    for row in database.captures:
        source = pd.Series(
            [value for date, value in _macro_rows(row["series_id"]) if date <= LATEST],
            index=pd.to_datetime([date for date, _ in _macro_rows(row["series_id"])
                                  if date <= LATEST]),
        )
        assert row["row_count"] == len(source)
        assert row["min_obs_date"] == source.index.min().date()
        assert row["max_obs_date"] == source.index.max().date()
        assert row["series_digest_sha256"] == w._digest([
            f"{date:%Y-%m-%d}|{float(value):.17g}" for date, value in source.items()
        ])


def test_existing_latest_month_and_bootstrap_rows_create_no_capture() -> None:
    _, database = run_worker(existing_as_of={LATEST})
    assert database.captures == []


def test_conflicting_immutable_capture_fails_closed_and_rolls_back(published) -> None:
    _, database = published
    original = database.captures[0]
    conflict = {**original, "series_digest_sha256": "f" * 64}
    decision = dict(database.decisions[-1])
    allocation = dict(database.allocations[-1])
    decisions_before = len(database.decisions)
    allocations_before = len(database.allocations)

    with pytest.raises(w.OpenMacroV04Error, match="immutable input capture conflict"):
        w.publish(
            database.conn, [decision], [allocation], LATEST, capture_rows=[conflict])

    assert database.conn.rollbacks == 1
    assert len(database.decisions) == decisions_before
    assert len(database.allocations) == allocations_before
    assert database.captures[0] == original


def test_a_month_that_already_existed_is_not_relabelled_live() -> None:
    """The row was not lived through THIS run. Whatever it says, this run did not
    witness it."""
    _, database = run_worker(existing_as_of={LATEST})
    bases = {r["as_of"]: r["decision_basis"] for r in database.decisions}
    assert bases[LATEST] == w.BASIS_BOOTSTRAP
    assert set(bases.values()) == {w.BASIS_BOOTSTRAP}


def test_the_upsert_can_never_demote_decision_basis() -> None:
    """The guarantee is in the SQL, not in the Python that computes the value: the
    UPDATE branch does not mention the column at all."""
    assert "decision_basis" not in w._DECISION_UPDATE_COLUMNS
    _, update_clause = w.DECISION_UPSERT_SQL.split("DO UPDATE SET", 1)
    assert "decision_basis" not in update_clause
    # ...and it IS written on insert, or the column would have no value at all.
    insert_clause = w.DECISION_UPSERT_SQL.split("VALUES", 1)[0]
    assert "decision_basis" in insert_clause


def test_an_invalidated_history_month_is_skipped_not_resurrected() -> None:
    killed = _dt.date(2015, 6, 30)
    stats, database = run_worker(invalidated={killed})
    assert stats["status"] == "published"
    assert stats["n_skipped_invalidated"] == 1
    assert stats["skipped_as_of"] == [killed.isoformat()]
    assert killed not in {r["as_of"] for r in database.decisions}
    # the allocation is skipped with its decision, or the FK would dangle
    assert killed not in {r["as_of"] for r in database.allocations}


def test_an_invalidated_current_month_is_fatal() -> None:
    """A killed historical month must not wedge the publisher; a killed CURRENT month
    means the decision the Light reads cannot be published, which is an outage."""
    with pytest.raises(w.OpenMacroV04Error, match="CURRENT month"):
        run_worker(invalidated={LATEST})


def test_the_upsert_never_resurrects_an_invalidated_row() -> None:
    for sql in (w.DECISION_UPSERT_SQL, w.ALLOCATION_UPSERT_SQL):
        assert sql.rstrip().endswith("valid_status <> 'invalidated'")


def test_the_five_validity_tokens_derive_from_the_state() -> None:
    assert w.decision_validity("blind", "contained", "chain_fresh") == "guard_blind"
    assert w.decision_validity("blind", "dominance", "chain_fresh") == "guard_blind"
    assert w.decision_validity("full", "dominance", "chain_fresh") == "dominance_baseline"
    assert w.decision_validity("partial_a", "dominance", "no_signal") == "dominance_baseline"
    assert w.decision_validity("full", "contained", "chain_fresh") == "fresh"
    assert w.decision_validity("full", "contained", "chain_carry") == "carried"
    assert w.decision_validity("full", "contained", "no_signal") == "no_signal"
    assert w.decision_validity("full", "contained", "proxy") == "no_signal"
    assert w.decision_validity("full", "contained", "proxy_missing") == "no_signal"


def test_every_published_row_carries_the_derived_validity_token(published) -> None:
    _, database = published
    seen = set()
    for row in database.decisions:
        expected = w.decision_validity(row["guard_coverage"], row["fiscal_state"],
                                       row["quadrant_source"])
        assert row["decision_validity"] == expected, row["as_of"]
        seen.add(expected)
    # the fixtures are gapless and quarterly, so 'guard_blind' is unreachable here —
    # asserted rather than assumed, so a future fixture that DOES go blind is noticed.
    assert seen == {"dominance_baseline", "fresh", "carried", "no_signal"}


def test_a_carried_quadrant_carries_its_seeds_confidence(published) -> None:
    _, database = published
    confidence = {r[0]: r[3] for r in _chain_rows()}
    last_fresh = None
    n_carried = 0
    for row in database.decisions:
        source, value = row["quadrant_source"], row["quadrant_confidence"]
        # the write goes through _exact_numeric, so the parameter is a Decimal that
        # round-trips to the float exactly; float() here compares the VALUE.
        value = None if value is None else float(value)
        if source == "chain_fresh":
            last_fresh = confidence[row["as_of"]]
            assert value == last_fresh, row["as_of"]
        elif source == "chain_carry":
            assert value == last_fresh, row["as_of"]
            n_carried += 1
        else:
            assert value is None, (row["as_of"], source)
    assert last_fresh is not None, "the fixture chain has fresh readings"
    assert n_carried > 0, "the fixture chain has carried months to exercise the carry"


def test_priced_at_is_a_session_that_prices_the_whole_book(published) -> None:
    _, database = published
    _, _, prices = _engine_inputs()
    sessions = {d.date() for d in prices.index}
    for row in database.allocations:
        assert row["priced_at"] in sessions, row["as_of"]
        assert row["priced_at"] <= row["as_of"]


def test_valid_until_is_the_next_month_end_at_14_utc(published) -> None:
    _, database = published
    for row in database.decisions:
        horizon = row["valid_until"]
        assert horizon.tzinfo is not None and horizon.hour == 14
        assert horizon.date() > row["as_of"]
        assert (horizon.date() + _dt.timedelta(days=1)).day == 1
    assert w.valid_until(_dt.date(2026, 5, 31)) == _dt.datetime(
        2026, 6, 30, 14, tzinfo=_dt.timezone.utc)
    assert w.valid_until(_dt.date(2026, 12, 31)) == _dt.datetime(
        2027, 1, 31, 14, tzinfo=_dt.timezone.utc)


def test_provenance_is_stamped_on_both_rows(published) -> None:
    stats, database = published
    for rows in (database.decisions, database.allocations):
        for row in rows:
            assert row["input_digest_sha256"] == stats["input_digest"]
            assert row["formulation_sha256"] == stats["formulation_sha"]
            assert row["code_commit"] == FAKE_COMMIT
            assert row["run_id"] == stats["run_id"]
            assert row["run_id"].startswith("open_macro_v04-2026-07-31-")


def test_the_stats_dict_reports_what_the_operator_has_to_check(published) -> None:
    stats, _ = published
    for key in ("n_published", "n_bootstrap", "n_live", "latest_as_of",
                "fiscal_state", "guard_level", "guard_coverage", "book_id",
                "input_digest", "formulation_sha"):
        assert key in stats, key
    assert stats["fiscal_state"] in ("contained", "dominance")
    assert stats["guard_level"] in ("off", "alert", "severe")
    assert stats["guard_coverage"] in ("full", "partial_a", "partial_b", "blind")
    assert stats["book_id"] in set(books.emitted_books())
    assert stats["live_freshness"]["arm_a"]["series_id"] == "SUBLPDCILSLGNQ"
    assert stats["live_freshness"]["arm_b"]["series_id"] == "M2SL"


# =========================================================================== #
# 5. The DDL's own consistency rules, evaluated on what would be written      #
# =========================================================================== #
def _named_check_constraints(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(re.findall(r"CONSTRAINT (\w+)\s+CHECK", text))


DECISION_INVARIANTS = {
    "open_macro_v04_decisions_month_end":
        lambda r: (r["as_of"] + _dt.timedelta(days=1)).day == 1,
    "open_macro_v04_decisions_severe_requires_contained":
        lambda r: r["guard_level"] != "severe" or r["fiscal_state"] == "contained",
    "open_macro_v04_decisions_severe_requires_contained_age":
        lambda r: r["guard_level"] != "severe" or r["fiscal_state_age_m"] >= 3,
    "open_macro_v04_decisions_degraded_is_alert":
        lambda r: not r["severe_degraded"] or r["guard_level"] == "alert",
    "open_macro_v04_decisions_candidate_is_not_off":
        lambda r: r["severe_run_age"] == 0 or r["guard_level"] != "off",
    "open_macro_v04_decisions_blind_is_not_off":
        lambda r: r["guard_coverage"] != "blind" or r["guard_level"] != "off",
    "open_macro_v04_decisions_off_has_no_armed_arm":
        lambda r: r["guard_level"] != "off" or not (r["arm_a"] or r["arm_b"]),
    "open_macro_v04_decisions_arm_a_requires_coverage":
        lambda r: not r["arm_a"] or r["guard_coverage"] in ("full", "partial_b"),
    "open_macro_v04_decisions_arm_b_requires_coverage":
        lambda r: not r["arm_b"] or r["guard_coverage"] in ("full", "partial_a"),
    "open_macro_v04_decisions_quadrant_source_consistent":
        lambda r: (r["quadrant"] is None)
                  == (r["quadrant_source"] in ("no_signal", "proxy_missing")),
    "open_macro_v04_decisions_carry_age_consistent":
        lambda r: ((r["quadrant_source"] != "chain_fresh" or r["carry_age"] == 0)
                   and (r["quadrant_source"] != "chain_carry" or r["carry_age"] >= 1)),
    "open_macro_v04_decisions_confidence_requires_chain":
        lambda r: (r["quadrant_confidence"] is None
                   or r["quadrant_source"] in ("chain_fresh", "chain_carry")),
    "open_macro_v04_decisions_validity_derivation":
        lambda r: r["decision_validity"] == w.decision_validity(
            r["guard_coverage"], r["fiscal_state"], r["quadrant_source"]),
}

ALLOCATION_INVARIANTS = {
    "open_macro_v04_allocations_month_end":
        lambda r: (r["as_of"] + _dt.timedelta(days=1)).day == 1,
    "open_macro_v04_allocations_weights_sum":
        lambda r: abs(sum(float(r[c]) for c, _ in w.WEIGHT_COLUMNS) - 1) < 1e-9,
    "open_macro_v04_allocations_risk_cap":
        lambda r: float(r["w_spy"]) + float(r["w_dbc"]) <= 0.65 + 1e-9,
    "open_macro_v04_allocations_defensive_floor":
        lambda r: (float(r["w_tlt"]) + float(r["w_shy"]) + float(r["w_tip"])
                   >= 0.20 - 1e-9),
}


def test_every_named_ddl_check_has_a_counterpart_here() -> None:
    """A new constraint in the DDL without a test that exercises it is a rule nobody
    has ever seen fire. `invalidation_consistent` is exempt: the worker never writes
    an invalidated row, so there is nothing here to exercise it with."""
    exempt = {"open_macro_v04_decisions_invalidation_consistent",
              "open_macro_v04_allocations_invalidation_consistent"}
    declared = _named_check_constraints(SCHEMAS / "open_macro_v04_decisions.sql")
    assert declared - exempt == set(DECISION_INVARIANTS)
    declared = _named_check_constraints(SCHEMAS / "open_macro_v04_allocations.sql")
    assert declared - exempt == set(ALLOCATION_INVARIANTS)


def test_no_published_decision_row_violates_a_ddl_constraint(published) -> None:
    _, database = published
    for row in database.decisions:
        for name, holds in DECISION_INVARIANTS.items():
            assert holds(row), f"{name} fails at {row['as_of']}"


def test_no_published_allocation_row_violates_a_ddl_constraint(published) -> None:
    _, database = published
    for row in database.allocations:
        for name, holds in ALLOCATION_INVARIANTS.items():
            assert holds(row), f"{name} fails at {row['as_of']}"


def test_the_vocabularies_the_worker_writes_are_the_ddl_vocabularies(published) -> None:
    decisions_ddl = (SCHEMAS / "open_macro_v04_decisions.sql").read_text(
        encoding="utf-8")
    _, database = published
    for column in ("fiscal_state", "guard_level", "guard_coverage",
                   "quadrant_source", "decision_validity", "decision_basis"):
        body = re.search(rf"CHECK \({column} IN \(([^)]*)\)\)", decisions_ddl)
        assert body is not None, f"{column} has no vocabulary CHECK in the DDL"
        allowed = set(re.findall(r"'([a-z_]+)'", body.group(1)))
        written = {r[column] for r in database.decisions}
        assert written <= allowed, (column, written - allowed)
        assert allowed, column


# =========================================================================== #
# 6. The book_id allowlist: derived from the router, never transcribed        #
# =========================================================================== #
def _ddl_book_ids() -> set[str]:
    """The allowlist as the DDL states it, parsed rather than retyped."""
    text = (SCHEMAS / "open_macro_v04_allocations.sql").read_text(encoding="utf-8")
    body = re.search(r"CHECK \(book_id IN \((.*?)\)\)\s*,", text, re.S).group(1)
    return set(re.findall(r"'([^']+)'", body))


def test_the_ddl_book_id_allowlist_is_exactly_what_the_router_can_emit() -> None:
    """Derived from the module on BOTH sides: the router's own `emitted_books()` is
    the authority and the DDL text is parsed, so neither is a hand copy of the other.
    A router that gains a book fails here instead of failing at 02:00 UTC against a
    CHECK constraint."""
    emittable = set(books.emitted_books())
    assert len(emittable) == 13
    assert _ddl_book_ids() == emittable


def test_every_published_book_id_is_in_the_allowlist(published) -> None:
    _, database = published
    written = {r["book_id"] for r in database.allocations}
    assert written <= _ddl_book_ids()
    assert written, "the run published no book at all"


def test_no_emitted_book_holds_hyg() -> None:
    """The reason w_hyg has no column. Structural, so it is asserted over EVERY
    emittable book rather than over the months that happened to occur."""
    for book_id, book in books.emitted_books().items():
        assert book["HYG"] == 0.0, book_id


# =========================================================================== #
# 7. as_of resolution                                                         #
# =========================================================================== #
def test_the_runbook_quotes_the_owners_normative_text_verbatim() -> None:
    """A runbook that paraphrases the step rule is worse than one that omits it: the
    comfortable half ('degrades to ALERT, never off') is true of the STATE and false
    of the PORTFOLIO, which is the entire reason the owner required the wording."""
    freeze = json.loads(w.FREEZE_PATH.read_text(encoding="utf-8"))
    normative = freeze["formulation"]["L3_guard"]["A8"]["normative_text"]
    runbook = (ROOT / "docs" / "open_macro_v4_runbook.md").read_text(encoding="utf-8")
    quoted = re.search(r"> (Sob amplitude 0.*?carteira\.)\n", runbook, re.S)
    assert quoted is not None, "the runbook must quote the A8 normative text"
    assert " ".join(quoted.group(1).replace("\n> ", " ").split()) == normative


def test_the_runbook_cites_the_current_formulation_digest() -> None:
    """The operator is told to check that every row carries ONE formulation_sha256.
    A runbook naming a stale one turns that check into a false alarm."""
    runbook = (ROOT / "docs" / "open_macro_v4_runbook.md").read_text(encoding="utf-8")
    assert w.verify_formulation_freeze() in runbook
    assert "open_macro_v04_decisions   | 29" in runbook
    assert "open_macro_v04_allocations | 21" in runbook
    assert len(w.EXPECTED_COLUMNS["open_macro_v04_decisions"]) == 29
    assert len(w.EXPECTED_COLUMNS["open_macro_v04_allocations"]) == 21


def test_the_default_as_of_is_the_last_complete_month_end() -> None:
    assert w.last_complete_month_end(_dt.date(2026, 8, 3)) == _dt.date(2026, 7, 31)
    assert w.last_complete_month_end(_dt.date(2026, 3, 1)) == _dt.date(2026, 2, 28)
    assert w.last_complete_month_end(_dt.date(2024, 3, 1)) == _dt.date(2024, 2, 29)
    assert w.resolve_as_of(today=_dt.date(2026, 8, 3)) == _dt.date(2026, 7, 31)


def test_a_mid_month_override_is_refused() -> None:
    with pytest.raises(w.OpenMacroV04Error, match="not a month-end"):
        w.resolve_as_of("2026-07-15", today=RUN_TODAY)


def test_a_future_month_override_is_refused() -> None:
    with pytest.raises(w.OpenMacroV04Error, match="beyond the last complete"):
        w.resolve_as_of("2026-08-31", today=RUN_TODAY)


def test_a_past_month_end_override_is_honoured() -> None:
    assert w.resolve_as_of("2020-02-29", today=RUN_TODAY) == _dt.date(2020, 2, 29)


def test_the_override_env_is_read(monkeypatch) -> None:
    monkeypatch.setenv(w.AS_OF_ENV, "2026-06-30")
    assert w.resolve_as_of(today=RUN_TODAY) == _dt.date(2026, 6, 30)
