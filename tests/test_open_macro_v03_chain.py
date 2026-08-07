"""Tests for the ``open_macro_v03_decision_chain`` producer.

Two families:

* DB-FREE unit tests over stub connections — the append-only contract, the readiness
  rule, the prefix gate, the write chokepoint. They are fast and run everywhere.
* REPLAY tests that call the certified engine over the committed pack. They are slow
  (~2 min each: the series is path-dependent, so there is no short version) and they
  are the ones that actually prove the ported logic is the script's logic.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import json
import os
import re
from pathlib import Path

import pytest

from src.workers import open_macro_v03_chain as w

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "open_macro_v03_chain"
CERTIFIED_CSV = FIXTURES / "certified_chain_2026_06.csv"
LIVE_DELTA_JSON = FIXTURES / "live_delta_2026_08_07.json"

CERTIFIED_END = _dt.date(2026, 6, 30)
NEXT_MONTH = _dt.date(2026, 7, 31)


# --------------------------------------------------------------------------- #
# fixtures carved from the live table (read-only capture, 2026-08-07)
# --------------------------------------------------------------------------- #
def load_certified_rows() -> dict[_dt.date, dict]:
    """The 148 rows exactly as the live table holds them."""
    rows: dict[_dt.date, dict] = {}
    lines = CERTIFIED_CSV.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    for line in lines[1:]:
        values = dict(zip(header, line.split(",")))
        as_of = _dt.date.fromisoformat(values["as_of"])
        rows[as_of] = {
            "quadrant": values["quadrant"] or None,
            "candidate_quadrant": values["candidate_quadrant"] or None,
            "status": values["status"],
            "candidate_confidence": _dec(values["candidate_confidence"]),
            "growth_score": _dec(values["growth_score"]),
            "inflation_score": _dec(values["inflation_score"]),
            "coverage_quality": _dec(values["coverage_quality"]),
            "transition_pending": values["transition_pending"] == "t",
            "basis": values["basis"],
            "pack_sha256": values["pack_sha256"],
            "chain_start": _dt.date.fromisoformat(values["chain_start"]),
        }
    return rows


def _dec(raw: str):
    return decimal.Decimal(raw) if raw else None


# --------------------------------------------------------------------------- #
# stub connection — the repo's duck-typed idiom
# --------------------------------------------------------------------------- #
class StubCursor:
    def __init__(self, conn):
        self._conn = conn
        self._result: list = []
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append((" ".join(str(sql).split()), params))
        self._result, self.rowcount = self._conn.answer(str(sql), params)

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0] if self._result else None


class StubConn:
    """Answers by SQL fragment. Records every statement so a test can assert that a
    run issued NO write at all."""

    def __init__(self, answers: dict[str, tuple[list, int]]):
        self.answers = answers
        self.executed: list[tuple[str, object]] = []
        self.commits = 0

    def cursor(self):
        return StubCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def answer(self, sql: str, params):
        for fragment, result in self.answers.items():
            if fragment in " ".join(sql.split()):
                return result
        return [], -1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def close(self):
        pass


# --------------------------------------------------------------------------- #
# APPEND-ONLY — the contract
# --------------------------------------------------------------------------- #
def _module_string_literals() -> list[str]:
    """Every string literal in the worker EXCEPT docstrings.

    Docstrings are excluded because the module's prose legitimately explains what the
    manual rebuild's DELETE+INSERT is and why this worker has none; what must be
    audited is the executable string surface — every SQL this module can ever send.
    """
    import ast
    tree = ast.parse((ROOT / "src" / "workers"
                      / "open_macro_v03_chain.py").read_text("utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            body = getattr(node, "body", [])
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                docstrings.add(id(body[0].value))
    return [n.value for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and id(n) not in docstrings]


MUTATION = re.compile(
    r"\b(DELETE\s+FROM|UPDATE\s+\w|TRUNCATE|DROP\s+\w|ALTER\s+\w|"
    r"CREATE\s+(TABLE|INDEX|VIEW)|DO\s+UPDATE)\b")


def test_the_module_carries_no_delete_and_no_update_of_the_chain():
    """The certified months are history: this worker must have no way to touch them.

    Asserted over the module's whole executable string surface, not over one code
    path, so a DELETE added tomorrow inside some helper fails here too."""
    for literal in _module_string_literals():
        found = MUTATION.search(" ".join(literal.split()).upper())
        assert found is None, (
            f"module carries a {found.group(0)!r} statement: {literal!r}")
    assert "ON CONFLICT (AS_OF) DO NOTHING" in w.INSERT_SQL.upper()
    assert w.INSERT_SQL.upper().startswith("INSERT INTO")


def test_the_only_assembled_sql_that_writes_is_the_append():
    """Every statement psycopg can receive from this module is a module-level
    ``*_SQL`` constant or one of the two session-hygiene literals; exactly one of them
    writes, and it is the append."""
    assembled = {name: value for name, value in vars(w).items()
                 if name.endswith("_SQL") and isinstance(value, str)}
    assert set(assembled) == {"COLUMNS_SQL", "READ_CHAIN_SQL", "ARM_FRESHNESS_SQL",
                              "MARKET_FRESHNESS_SQL", "MACRO_DELTA_SQL",
                              "EOD_DELTA_SQL", "INSERT_SQL", "READBACK_SQL"}
    writers = [name for name, sql in assembled.items()
               if not sql.upper().lstrip().startswith("SELECT")]
    assert writers == ["INSERT_SQL"], writers
    assert w.INSERT_SQL.upper().rstrip().endswith("ON CONFLICT (AS_OF) DO NOTHING")


def test_publish_is_a_no_op_when_the_month_is_already_there():
    """Re-running over a published month writes nothing — decided by the primary key,
    not by Python having checked first."""
    conn = StubConn({"INSERT INTO open_macro_v03_decision_chain": ([], 0)})
    row = _row(_dt.date(2026, 6, 30))
    assert w.publish(conn, row) == 0
    inserts = [sql for sql, _ in conn.executed if sql.upper().startswith("INSERT")]
    assert len(inserts) == 1
    assert "ON CONFLICT (as_of) DO NOTHING" in inserts[0]


def test_publish_reports_the_row_it_landed():
    conn = StubConn({"INSERT INTO open_macro_v03_decision_chain": ([], 1)})
    assert w.publish(conn, _row(_dt.date(2026, 7, 31))) == 1


def _row(as_of: _dt.date) -> dict:
    return {
        "as_of": as_of, "quadrant": "slowdown", "candidate_quadrant": "slowdown",
        "status": "valid", "candidate_confidence": 0.7254330463532251,
        "growth_score": 0.2386835808610429, "inflation_score": 1.004267438236338,
        "coverage_quality": 1.0, "transition_pending": False,
        "basis": w.BASIS, "pack_sha256": "a" * 64, "chain_start": w.CHAIN_START,
        "code_commit": "b" * 40,
        "loaded_at": _dt.datetime(2026, 8, 31, tzinfo=_dt.timezone.utc),
    }


# --------------------------------------------------------------------------- #
# the write chokepoint
# --------------------------------------------------------------------------- #
def test_every_float_reaches_the_driver_as_an_exact_decimal():
    """``_exact_numeric`` by TYPE: a NUMERIC column added tomorrow is covered."""
    params = w._exact_numeric_params(_row(_dt.date(2026, 7, 31)))
    for key, value in params.items():
        assert not isinstance(value, float), f"{key} reached the driver as a float"
    confidence = params["candidate_confidence"]
    assert confidence == decimal.Decimal("0.7254330463532251")
    assert float(confidence) == 0.7254330463532251
    # the defect this guards against: the float8 -> NUMERIC cast keeps 15 digits
    assert confidence != decimal.Decimal("%.15g" % 0.7254330463532251)


def test_exact_numeric_params_does_not_mutate_the_caller_s_row():
    row = _row(_dt.date(2026, 7, 31))
    w._exact_numeric_params(row)
    assert isinstance(row["candidate_confidence"], float)


# --------------------------------------------------------------------------- #
# readiness — native frequency, not calendar months
# --------------------------------------------------------------------------- #
def _freshness(**overrides) -> dict[str, _dt.datetime]:
    base = {sid: _dt.datetime(2026, 8, 4, tzinfo=_dt.timezone.utc)
            for sid in w.arm_series_ids()}
    base.update(overrides)
    return base


def _readiness_conn(freshness: dict, market_latest: _dt.date | None):
    return StubConn({
        "FROM macro_observation_vintage": (list(freshness.items()), -1),
        "FROM eod_prices": ([(market_latest,)], -1),
    })


def test_a_month_is_ready_only_when_every_arm_printed_past_the_cutoff():
    conn = _readiness_conn(_freshness(), _dt.date(2026, 8, 6))
    gate = w.readiness(conn, NEXT_MONTH)
    assert gate["ready"] is True
    assert gate["pending"] == []
    assert gate["cutoff"] == "2026-07-31T00:00:00+00:00"


def test_an_arm_that_printed_only_at_the_cutoff_is_not_settled():
    """MICH prints at the END of its own month, i.e. AT the decision cutoff. That
    print is INSIDE the PIT read, so it is not proof that nothing else will land
    below the cutoff — settlement needs a STRICTLY later print."""
    conn = _readiness_conn(
        _freshness(MICH=_dt.datetime(2026, 7, 31, tzinfo=_dt.timezone.utc)),
        _dt.date(2026, 8, 6))
    gate = w.readiness(conn, NEXT_MONTH)
    assert gate["ready"] is False
    assert gate["pending"] == ["MICH"]


def test_the_arm_that_is_behind_is_named():
    conn = _readiness_conn(
        _freshness(MICH=_dt.datetime(2026, 6, 26, tzinfo=_dt.timezone.utc),
                   PAYEMS=_dt.datetime(2026, 7, 2, tzinfo=_dt.timezone.utc)),
        _dt.date(2026, 8, 6))
    gate = w.readiness(conn, NEXT_MONTH)
    assert sorted(gate["pending"]) == ["MICH", "PAYEMS"]
    behind = {a["arm"]: a["last_available_at"] for a in gate["arms"]}
    assert behind["MICH"].startswith("2026-06-26")


def test_a_missing_arm_is_unsettled_not_silently_skipped():
    freshness = _freshness()
    freshness.pop("ACOGNO")
    conn = _readiness_conn(freshness, _dt.date(2026, 8, 6))
    gate = w.readiness(conn, NEXT_MONTH)
    assert "ACOGNO" in gate["pending"]
    assert [a for a in gate["arms"] if a["arm"] == "ACOGNO"][0][
        "last_available_at"] is None


def test_the_market_sensor_is_an_arm_too():
    conn = _readiness_conn(_freshness(), _dt.date(2026, 7, 31))
    gate = w.readiness(conn, NEXT_MONTH)
    assert gate["pending"] == ["SPY:eod_prices"]


def test_readiness_reads_the_arms_from_the_model_registry():
    """Never a re-listed basket: a series added to SEED_SOURCES moves readiness."""
    from src.macro_sources import SEED_SOURCES
    assert w.arm_series_ids() == sorted({s.series_id for s in SEED_SOURCES})
    assert len(w.arm_series_ids()) == 8


# --------------------------------------------------------------------------- #
# month arithmetic
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("latest,expected", [
    (_dt.date(2026, 6, 30), _dt.date(2026, 7, 31)),
    (_dt.date(2026, 1, 31), _dt.date(2026, 2, 28)),
    (_dt.date(2027, 1, 31), _dt.date(2027, 2, 28)),
    (_dt.date(2028, 1, 31), _dt.date(2028, 2, 29)),
    (_dt.date(2026, 11, 30), _dt.date(2026, 12, 31)),
    (_dt.date(2026, 12, 31), _dt.date(2027, 1, 31)),
])
def test_next_month_end(latest, expected):
    assert w.next_month_end(latest) == expected


@pytest.mark.parametrize("today,expected", [
    (_dt.date(2026, 8, 7), _dt.date(2026, 7, 31)),
    (_dt.date(2026, 8, 1), _dt.date(2026, 7, 31)),
    (_dt.date(2026, 7, 31), _dt.date(2026, 6, 30)),
    (_dt.date(2027, 1, 1), _dt.date(2026, 12, 31)),
])
def test_last_complete_month_end(today, expected):
    assert w.last_complete_month_end(today) == expected


# --------------------------------------------------------------------------- #
# "is this the certified chain?"
# --------------------------------------------------------------------------- #
PACK_SHA = "914b06b52dc966049d5c680c7c840b204864451dc6b9ba1332106245ee7ca804"


def test_the_committed_fixture_is_the_certified_chain():
    stored = load_certified_rows()
    assert len(stored) == 148
    assert min(stored) == _dt.date(2014, 3, 31)
    assert max(stored) == CERTIFIED_END
    assert sum(1 for r in stored.values() if r["status"] == "valid") == 118
    w.verify_is_the_certified_chain(stored, PACK_SHA)


def test_an_empty_chain_is_refused_not_bootstrapped():
    with pytest.raises(w.DecisionChainError, match="EMPTY"):
        w.verify_is_the_certified_chain({}, PACK_SHA)


def test_a_hole_in_the_chain_is_refused():
    stored = load_certified_rows()
    stored.pop(_dt.date(2020, 5, 31))
    with pytest.raises(w.DecisionChainError, match="contiguous"):
        w.verify_is_the_certified_chain(stored, PACK_SHA)


def test_a_chain_from_a_different_pack_is_refused():
    with pytest.raises(w.DecisionChainError, match="same pack"):
        w.verify_is_the_certified_chain(load_certified_rows(), "c" * 64)


def test_a_chain_with_a_different_genesis_is_refused():
    stored = load_certified_rows()
    stored[_dt.date(2014, 3, 31)]["chain_start"] = _dt.date(2010, 1, 1)
    with pytest.raises(w.DecisionChainError, match="genesis"):
        w.verify_is_the_certified_chain(stored, PACK_SHA)


# --------------------------------------------------------------------------- #
# the certified pack
# --------------------------------------------------------------------------- #
def test_the_pinned_pack_digests_match_the_checked_out_pack():
    identity = w.verify_pack()
    assert identity["input_pack_sha256"] == PACK_SHA
    assert identity["input_pack_id"] == "open_macro_v03_certified_input_pack_003"


def test_the_pack_boundaries_are_observed_not_hardcoded():
    _macro, _eod, macro_boundary, eod_boundary = w.load_pack_inputs()
    assert macro_boundary == _dt.datetime(2026, 6, 25, tzinfo=_dt.timezone.utc)
    assert eod_boundary == _dt.date(2026, 6, 30)
    # the boundary is BELOW the chain's last month: the live MICH vintage of
    # 2026-06-26 is exactly the row the pack does not have.
    assert macro_boundary.date() < CERTIFIED_END


# --------------------------------------------------------------------------- #
# schema gate
# --------------------------------------------------------------------------- #
def _catalog_rows(**mutate):
    columns = dict(w.EXPECTED_COLUMNS)
    columns.update(mutate)
    return [(name, *spec) for name, spec in columns.items()]


def test_schema_gate_accepts_the_measured_catalog():
    conn = StubConn({"information_schema.columns": (_catalog_rows(), -1)})
    w.verify_schema(conn)


def test_schema_gate_refuses_an_unknown_column():
    rows = _catalog_rows() + [("regime_note", "text", None, "YES")]
    conn = StubConn({"information_schema.columns": (rows, -1)})
    with pytest.raises(w.DecisionChainError, match="unknown columns"):
        w.verify_schema(conn)


def test_schema_gate_refuses_a_missing_table():
    conn = StubConn({"information_schema.columns": ([], -1)})
    with pytest.raises(w.DecisionChainError, match="does not create it"):
        w.verify_schema(conn)


def test_schema_gate_refuses_a_retyped_column():
    conn = StubConn({"information_schema.columns": (
        _catalog_rows(candidate_confidence=("double precision", None, "YES")), -1)})
    with pytest.raises(w.DecisionChainError, match="candidate_confidence"):
        w.verify_schema(conn)


# --------------------------------------------------------------------------- #
# the prefix gate
# --------------------------------------------------------------------------- #
class FakeDecision:
    def __init__(self, as_of, **kw):
        self.as_of = as_of
        self.quadrant = kw.get("quadrant")
        self.candidate_quadrant = kw.get("candidate_quadrant")
        self.status = kw.get("status", "valid")
        self.transition_pending = kw.get("transition_pending", False)
        self.candidate_confidence = kw.get("candidate_confidence")
        self.growth_score = kw.get("growth_score")
        self.inflation_score = kw.get("inflation_score")
        self.coverage_quality = kw.get("coverage_quality")


def _stored_one(**kw):
    base = {"quadrant": "slowdown", "candidate_quadrant": "slowdown",
            "status": "valid", "transition_pending": False,
            "candidate_confidence": decimal.Decimal("0.5"),
            "growth_score": decimal.Decimal("0.25"),
            "inflation_score": decimal.Decimal("-0.25"),
            "coverage_quality": decimal.Decimal("1")}
    base.update(kw)
    return {_dt.date(2026, 6, 30): base}


def _replayed_one(**kw):
    base = {"quadrant": "slowdown", "candidate_quadrant": "slowdown",
            "status": "valid", "transition_pending": False,
            "candidate_confidence": 0.5, "growth_score": 0.25,
            "inflation_score": -0.25, "coverage_quality": 1.0}
    base.update(kw)
    return [FakeDecision(_dt.date(2026, 6, 30), **base)]


def test_prefix_gate_passes_when_the_consumable_projection_holds():
    report = w.prefix_gate(_replayed_one(), _stored_one())
    assert report["months_verified"] == 1
    assert report["candidate_quadrant_drift"] == []
    assert report["numeric_drift_cells"] == 0


@pytest.mark.parametrize("column,value", [
    ("quadrant", "contraction"), ("status", "low_confidence"),
    ("transition_pending", True),
])
def test_prefix_gate_hard_fails_on_consumable_drift(column, value):
    replayed = _replayed_one(**{column: value})
    if column == "status":                       # keep the table's CHECK coherent
        replayed[0].quadrant = None
        stored = _stored_one()
    else:
        stored = _stored_one()
    with pytest.raises(w.DecisionChainError, match="CONSUMABLE"):
        w.prefix_gate(replayed, stored)


def test_prefix_gate_reports_candidate_quadrant_drift_without_failing():
    """The model's endpoint dependence moves candidate_quadrant; no consumer reads
    it and the stored row is never rewritten, so it is evidence, not a stop."""
    report = w.prefix_gate(_replayed_one(candidate_quadrant="recovery"),
                           _stored_one())
    assert report["candidate_quadrant_drift"] == [
        {"as_of": "2026-06-30", "stored": "slowdown", "replayed": "recovery"}]


def test_prefix_gate_reports_numeric_drift_with_its_magnitude():
    report = w.prefix_gate(_replayed_one(candidate_confidence=0.504),
                           _stored_one())
    assert report["numeric_drift_cells"] == 1
    assert report["numeric_drift_max"] == pytest.approx(0.004)
    assert report["numeric_drift_months"] == ["2026-06-30"]


def test_prefix_gate_accepts_the_15_digit_rendering_the_old_rows_carry():
    """The 2026-07-17 rebuild predates ``_exact_numeric``: its NUMERICs are the
    float8->NUMERIC 15-digit rendering of the very doubles the engine recomputes.
    That is a statement about storage, not a tolerance — 17-digit equality is tried
    first and a genuinely different double still fails."""
    value = 0.8038180058508771
    stored = _stored_one(candidate_confidence=decimal.Decimal("%.15g" % value))
    report = w.prefix_gate(_replayed_one(candidate_confidence=value), stored)
    assert report["numeric_drift_cells"] == 0
    assert w._stored_matches_double(decimal.Decimal(repr(value)), value)
    assert not w._stored_matches_double(decimal.Decimal("0.80381800585088"), value)


def test_prefix_gate_refuses_a_replay_that_does_not_cover_the_stored_span():
    with pytest.raises(w.DecisionChainError, match="stored months"):
        w.prefix_gate([], _stored_one())


# --------------------------------------------------------------------------- #
# row projection
# --------------------------------------------------------------------------- #
def test_build_row_refuses_a_row_the_table_s_own_check_would_reject():
    valid_without_quadrant = FakeDecision(NEXT_MONTH, status="valid", quadrant=None)
    with pytest.raises(w.DecisionChainError, match="CHECK"):
        w.build_row(valid_without_quadrant, "a" * 64, "b" * 40,
                    _dt.datetime.now(_dt.timezone.utc))
    abstained_with_quadrant = FakeDecision(NEXT_MONTH, status="low_confidence",
                                           quadrant="slowdown")
    with pytest.raises(w.DecisionChainError, match="CHECK"):
        w.build_row(abstained_with_quadrant, "a" * 64, "b" * 40,
                    _dt.datetime.now(_dt.timezone.utc))


def test_build_row_stamps_the_certified_prefix_and_this_worker_s_revision():
    now = _dt.datetime(2026, 8, 31, tzinfo=_dt.timezone.utc)
    row = w.build_row(FakeDecision(NEXT_MONTH, quadrant="slowdown",
                                   candidate_quadrant="slowdown", status="valid",
                                   candidate_confidence=0.7, growth_score=0.2,
                                   inflation_score=1.0, coverage_quality=1.0),
                      PACK_SHA, "b" * 40, now)
    assert tuple(row) == w.ROW_COLUMNS
    assert row["basis"] == "certified_chain"
    assert row["pack_sha256"] == PACK_SHA
    assert row["chain_start"] == w.CHAIN_START
    assert row["code_commit"] == "b" * 40
    assert row["loaded_at"] == now


def test_code_commit_refuses_a_value_the_char_40_column_would_pad(monkeypatch):
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "deadbeef")
    with pytest.raises(w.DecisionChainError, match="char\\(40\\)"):
        w.code_commit()


# --------------------------------------------------------------------------- #
# run() — the no-op paths write nothing
# --------------------------------------------------------------------------- #
def _run_conn(chain_rows, freshness, market_latest):
    return StubConn({
        "SET search_path": ([], -1),
        "SHOW search_path": ([("public",)], -1),
        "information_schema.columns": (_catalog_rows(), -1),
        "FROM open_macro_v03_decision_chain ORDER BY as_of": (chain_rows, -1),
        "FROM macro_observation_vintage WHERE series_id = ANY": (
            list(freshness.items()), -1),
        "FROM eod_prices WHERE ticker": ([(market_latest,)], -1),
    })


def _chain_tuples():
    rows = []
    for as_of, r in sorted(load_certified_rows().items()):
        rows.append((as_of, r["quadrant"], r["candidate_quadrant"], r["status"],
                     r["candidate_confidence"], r["growth_score"],
                     r["inflation_score"], r["coverage_quality"],
                     r["transition_pending"], r["basis"], r["pack_sha256"],
                     r["chain_start"]))
    return rows


def _patch_connection(monkeypatch, conn):
    monkeypatch.setattr(w, "connect", lambda dsn, **kw: conn)
    monkeypatch.setattr(w, "resolve_dsn", lambda dsn=None: "stub")
    monkeypatch.setattr(w, "advisory_lock", _always_acquired)


import contextlib  # noqa: E402


@contextlib.contextmanager
def _always_acquired(conn, lock_id):
    assert lock_id == 900_220
    yield True


# max(available_at) per arm as MEASURED in the live datalake on 2026-08-07, and the
# SPY session mirror's last date. This is the rehearsal of the first run.
MEASURED_2026_08_07 = {
    "ACOGNO": _dt.datetime(2026, 8, 4, tzinfo=_dt.timezone.utc),
    "AHETPI": _dt.datetime(2026, 7, 2, tzinfo=_dt.timezone.utc),
    "CPILFESL": _dt.datetime(2026, 7, 14, tzinfo=_dt.timezone.utc),
    "INDPRO": _dt.datetime(2026, 7, 17, tzinfo=_dt.timezone.utc),
    "MICH": _dt.datetime(2026, 6, 26, tzinfo=_dt.timezone.utc),
    "PAYEMS": _dt.datetime(2026, 7, 2, tzinfo=_dt.timezone.utc),
    "PCEC96": _dt.datetime(2026, 7, 30, tzinfo=_dt.timezone.utc),
    "PPIFIS": _dt.datetime(2026, 7, 15, tzinfo=_dt.timezone.utc),
}
MEASURED_SPY_2026_08_07 = _dt.date(2026, 8, 6)


def test_run_writes_nothing_while_an_arm_is_behind(monkeypatch):
    """Rehearsal of the FIRST RUN against the measured 2026-08-07 datalake.

    2026-07-31 is a complete month and SPY has settled, but seven of the eight macro
    arms had not yet printed past the cutoff — ACOGNO alone had (2026-08-04). The
    month is NOT published and every pending arm is named. No forward-fill, no row.
    """
    conn = _run_conn(_chain_tuples(), dict(MEASURED_2026_08_07),
                     MEASURED_SPY_2026_08_07)
    _patch_connection(monkeypatch, conn)
    stats = w.run("stub", today=_dt.date(2026, 8, 7))
    assert stats["published"] == 0
    assert stats["reason"] == "inputs_not_settled"
    assert stats["chain_rows"] == 148
    assert stats["chain_latest"] == "2026-06-30"
    assert stats["target"] == "2026-07-31"
    assert stats["last_complete_month_end"] == "2026-07-31"
    settled = {a["arm"] for a in stats["readiness"]["arms"] if a["settled"]}
    assert settled == {"ACOGNO", "SPY:eod_prices"}
    assert sorted(stats["readiness"]["pending"]) == [
        "AHETPI", "CPILFESL", "INDPRO", "MICH", "PAYEMS", "PCEC96", "PPIFIS"]
    assert not [sql for sql, _ in conn.executed if "INSERT" in sql.upper()]
    # and it never even paid for the replay: no delta was read
    assert not [sql for sql, _ in conn.executed if "available_at >" in sql]


def test_run_writes_nothing_while_the_month_is_still_in_progress(monkeypatch):
    conn = _run_conn(_chain_tuples(), _freshness(), _dt.date(2026, 7, 20))
    _patch_connection(monkeypatch, conn)
    stats = w.run("stub", today=_dt.date(2026, 7, 20))
    assert stats["published"] == 0
    assert stats["reason"] == "month_in_progress"
    assert stats["target"] == "2026-07-31"
    assert not [sql for sql, _ in conn.executed if "INSERT" in sql.upper()]


def test_run_writes_nothing_when_a_concurrent_run_holds_the_lock(monkeypatch):
    conn = _run_conn(_chain_tuples(), _freshness(), _dt.date(2026, 8, 6))

    @contextlib.contextmanager
    def _held(_conn, _lock_id):
        yield False

    monkeypatch.setattr(w, "connect", lambda dsn, **kw: conn)
    monkeypatch.setattr(w, "resolve_dsn", lambda dsn=None: "stub")
    monkeypatch.setattr(w, "advisory_lock", _held)
    stats = w.run("stub", today=_dt.date(2026, 8, 7))
    assert stats == {"input_pack_id": "open_macro_v03_certified_input_pack_003",
                     "published": 0, "skipped": "advisory_lock_held"}
    assert conn.executed == []


def test_run_refuses_a_non_public_search_path(monkeypatch):
    conn = _run_conn(_chain_tuples(), _freshness(), _dt.date(2026, 8, 6))
    conn.answers["SHOW search_path"] = ([("mirror, public",)], -1)
    _patch_connection(monkeypatch, conn)
    with pytest.raises(w.DecisionChainError, match="search_path"):
        w.run("stub", today=_dt.date(2026, 8, 7))


# --------------------------------------------------------------------------- #
# registration
# --------------------------------------------------------------------------- #
def test_the_worker_is_dispatchable_by_name():
    import importlib
    source = (ROOT / "src" / "run_worker.py").read_text("utf-8")
    assert "|open_macro_v03_chain" in source
    module = importlib.import_module("src.workers.open_macro_v03_chain")
    assert callable(module.run)


def test_the_advisory_lock_is_its_own():
    from src import db
    assert db.LOCK_OPEN_MACRO_V03_CHAIN == 900_220
    others = {name: value for name, value in vars(db).items()
              if name.startswith("LOCK_") and name != "LOCK_OPEN_MACRO_V03_CHAIN"}
    assert 900_220 not in others.values()


# --------------------------------------------------------------------------- #
# REPLAY tests — slow, and the only ones that prove the math
# --------------------------------------------------------------------------- #
SLOW = pytest.mark.skipif(
    os.environ.get("CHAIN_REPLAY_TESTS", "1") == "0",
    reason="CHAIN_REPLAY_TESTS=0: the ~2 min certified replays are disabled")


@pytest.fixture(scope="module")
def pack_inputs():
    macro, eod, macro_boundary, eod_boundary = w.load_pack_inputs()
    return macro, eod, macro_boundary, eod_boundary


@pytest.fixture(scope="module")
def replay_to_certified_end(pack_inputs):
    macro, eod, _mb, _eb = pack_inputs
    return w.compute_series(macro, eod, CERTIFIED_END)


@pytest.fixture(scope="module")
def replay_to_next_month(pack_inputs):
    macro, eod, macro_boundary, eod_boundary = pack_inputs
    delta = json.loads(LIVE_DELTA_JSON.read_text(encoding="utf-8"))
    for row in delta["macro_delta"]:
        assert w._parse_available_at(row["available_at"]) > macro_boundary
    for row in delta["eod_delta"]:
        assert _dt.date.fromisoformat(row["date"]) > eod_boundary
    return w.compute_series(macro + delta["macro_delta"],
                            eod + delta["eod_delta"], NEXT_MONTH)


@SLOW
def test_golden_replay_reproduces_the_certified_chain(replay_to_certified_end):
    """THE GOLDEN: the pinned certified pack, replayed through the worker's own call,
    reproduces every one of the 148 published months — categorical columns exactly,
    NUMERICs to the float8->NUMERIC rendering the 2026-07-17 rebuild wrote.

    This is what makes "the ported logic IS the script's logic" a measurement.
    """
    stored = load_certified_rows()
    series = replay_to_certified_end
    assert [r.as_of for r in series] == sorted(stored)
    for row in series:
        want = stored[row.as_of]
        assert row.quadrant == want["quadrant"], row.as_of
        assert row.candidate_quadrant == want["candidate_quadrant"], row.as_of
        assert row.status == want["status"], row.as_of
        assert bool(row.transition_pending) == want["transition_pending"], row.as_of
        for column in w.NUMERIC_COLUMNS:
            assert w._stored_matches_double(want[column], getattr(row, column)), (
                f"{row.as_of} {column}: stored {want[column]} vs "
                f"replayed {getattr(row, column)!r}")


@SLOW
def test_the_prefix_gate_passes_on_the_real_extension(replay_to_next_month):
    """Extending to 2026-07-31 over pack + the captured live delta keeps every
    published month's CONSUMABLE projection. The endpoint drift the model carries is
    reported, and pinned here so a change in its shape is visible in CI."""
    stored = load_certified_rows()
    report = w.prefix_gate(replay_to_next_month, stored)
    assert report["months_verified"] == 148
    # measured 2026-08-07 — one candidate_quadrant cell moves, nothing consumable
    assert report["candidate_quadrant_drift"] == [
        {"as_of": "2023-11-30", "stored": "contraction", "replayed": "recovery"}]
    assert report["numeric_drift_cells"] == 46
    assert report["numeric_drift_months"][0] == "2022-10-31"
    assert report["numeric_drift_months"][-1] == "2026-06-30"


@SLOW
def test_the_extension_appends_exactly_one_month(replay_to_next_month):
    stored = load_certified_rows()
    series = replay_to_next_month
    assert [r.as_of for r in series][:-1] == sorted(stored)
    assert series[-1].as_of == NEXT_MONTH
    row = w.build_row(series[-1], PACK_SHA, "b" * 40,
                      _dt.datetime(2026, 8, 31, tzinfo=_dt.timezone.utc))
    assert row["as_of"] == NEXT_MONTH
    assert row["basis"] == "certified_chain"
    # the table's CHECK, restated as an assertion on the projection itself
    assert (row["status"] == "valid") == (row["quadrant"] is not None)


@SLOW
def test_the_certified_prefix_is_pinned_to_the_pack_not_to_the_live_store(
        pack_inputs, replay_to_next_month):
    """The live delta starts STRICTLY after the pack's window, so no decision at or
    before 2026-06-25 can see a row the certified rebuild did not see. The MICH
    vintage of 2026-06-26 — present live, absent from the pack — is the row that makes
    this concrete: it enters the replay (it is after the boundary) and it is why the
    recomputed 2026-06-30 inflation_score differs from the stored one, on a row this
    worker never writes."""
    _macro, _eod, macro_boundary, _eb = pack_inputs
    delta = json.loads(LIVE_DELTA_JSON.read_text(encoding="utf-8"))
    mich = [r for r in delta["macro_delta"] if r["series_id"] == "MICH"]
    assert len(mich) == 1
    assert mich[0]["available_at"].startswith("2026-06-26")
    assert w._parse_available_at(mich[0]["available_at"]) > macro_boundary
    june = [r for r in replay_to_next_month if r.as_of == CERTIFIED_END][0]
    stored_june = load_certified_rows()[CERTIFIED_END]
    assert june.quadrant == stored_june["quadrant"]
    assert not w._stored_matches_double(stored_june["inflation_score"],
                                        june.inflation_score)
