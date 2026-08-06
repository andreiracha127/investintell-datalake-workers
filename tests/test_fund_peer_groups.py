"""Unit tests for the fund_peer_groups quarterly partitioner.

No real PostgreSQL. The worker is driven END TO END over a synthetic anchor whose
group structure is DESIGNED (``tests/_fund_peer_groups_fixtures.py``) and whose
resulting partition is pinned byte for byte against a committed golden. What that
buys, and what it deliberately does not:

  * It IS a lock on the algorithm. Every frozen constant — the 0.70 block threshold,
    theta, the Louvain seed, the resolution ladder, the recursion budget, the 0.05
    coherence floor, the CUSIP-6 collapse — changes the golden if it moves, and
    ``params_sha256`` is pinned against a literal digest besides.
  * It is NOT a claim that the production anchor reproduces the published figures.
    That is a 7023 x 7023 matrix built from tens of millions of rows; it is
    reproduced by an operator run against the real anchor and compared to the fact
    sheet (docs/fund_peer_groups_runbook.md). Committing a 200 MB fixture to restate
    a number that already exists in a document would be ceremony.
"""

from __future__ import annotations

import collections
import datetime as _dt
import decimal
import json
import re
from pathlib import Path

import networkx as nx
import pytest

import src.workers.fund_peer_groups as w
from _fund_peer_groups_fixtures import (ANCHOR, FakeConn, FakeDatabase,
                                        anchor_rows, catalog_rows, eligible_series,
                                        run_worker, served_universe,
                                        unidentified_rows)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "fund_peer_groups_v1.sql"
GOLDEN_PATH = ROOT / "tests" / "fixtures" / "fund_peer_groups" / "golden_partition.json"
GOLDEN = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))

TODAY = _dt.date(2026, 2, 15)                 # the suggested cron day
FAKE_COMMIT = "0" * 40

# The digest of the SHIPPED default parameters. A constant that moves without this
# moving means two anchors could carry the same digest under different recipes, which
# is the one thing params_sha256 exists to make impossible.
DEFAULT_PARAMS_SHA256 = \
    "cd2a9018eca5e56fd9ccb6bc49fb394ad8713743beb72566fd1f95135d5f80b3"

DEFAULT_PARAMS = dict(size_cap_frac=w.DEFAULT_SIZE_CAP_FRAC,
                      cap_waive_min_median=w.DEFAULT_CAP_WAIVE_MIN_MEDIAN,
                      cap_waive_hard_ceiling=w.DEFAULT_CAP_WAIVE_HARD_CEILING,
                      ident_floor=w.DEFAULT_IDENT_FLOOR)


@pytest.fixture(autouse=True)
def _pinned_environment(monkeypatch):
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", FAKE_COMMIT)
    for name in (w.ANCHOR_ENV, w.SIZE_CAP_ENV, w.CAP_WAIVE_MEDIAN_ENV,
                 w.CAP_WAIVE_CEILING_ENV, w.IDENT_FLOOR_ENV, w.UNIVERSE_DSN_ENV):
        monkeypatch.delenv(name, raising=False)


def _set_cap_policy(monkeypatch, *, cap: float, ceiling: float,
                    waive: float | None = None) -> None:
    monkeypatch.setenv(w.SIZE_CAP_ENV, str(cap))
    monkeypatch.setenv(w.CAP_WAIVE_CEILING_ENV, str(ceiling))
    if waive is not None:
        monkeypatch.setenv(w.CAP_WAIVE_MEDIAN_ENV, str(waive))


@pytest.fixture
def golden_run(monkeypatch):
    """The run the golden was recorded from: the fixture under its own cap policy.

    The cap sits above every designed group, so the golden is a statement about the
    BASE algorithm — pre-split, Louvain, coherence — with the waiver never reached.
    The waiver has its own tests, on both sides of the threshold."""
    _set_cap_policy(monkeypatch, cap=GOLDEN["size_cap_frac"],
                    ceiling=GOLDEN["cap_waive_hard_ceiling"],
                    waive=GOLDEN["cap_waive_min_median"])
    return run_worker(w, monkeypatch, today=TODAY)


# =========================================================================== #
# 1. The golden partition                                                     #
# =========================================================================== #
def test_the_partition_reproduces_the_golden_row_for_row(golden_run) -> None:
    stats, database = golden_run
    assert stats["status"] == "published"
    published = {r["series_id"]: r for r in database.published}
    assert sorted(published) == sorted(GOLDEN["rows"])
    for series_id, expected in GOLDEN["rows"].items():
        row = published[series_id]
        for column, value in expected.items():
            assert row[column] == value, f"{series_id}.{column}"


def test_the_headline_statistics_reproduce_the_golden(golden_run) -> None:
    stats, _ = golden_run
    for key, value in GOLDEN["stats"].items():
        assert stats[key] == value, key
    assert stats["anchor_date"] == GOLDEN["anchor_date"]
    assert stats["params_sha256"] == GOLDEN["params_sha256"]


def test_the_designed_structure_is_the_structure_that_came_out(golden_run) -> None:
    """The golden is only worth locking if it is the structure the fixture DESIGNED.

    Asserted separately from the byte comparison so that a future regeneration cannot
    quietly enshrine an accidental identifier collision as "the golden"."""
    _, database = golden_run
    members = collections.defaultdict(set)
    for row in database.published:
        members[row["group_id"]].add(row["series_id"])

    named = {frozenset(v) for k, v in members.items() if k is not None}
    assert {f"FIA{i:02d}" for i in range(8)} in named        # one paper per issuer
    assert {f"FIB{i:02d}" for i in range(8)} in named        # TWO papers per issuer
    assert {f"EQC{i:02d}" for i in range(10)} in named
    assert {f"EQD{i:02d}" for i in range(6)} in named
    assert {f"MXF{i:02d}" for i in range(6)} in named

    # the star and the four disjoint books: connected or not, none is a peer group
    without_group = members[None]
    assert without_group == ({"EQGHUB"} | {f"EQG{i:02d}" for i in range(6)}
                             | {f"EQN{i:02d}" for i in range(4)})


def test_the_fixed_income_group_exists_only_because_of_the_issuer_collapse() -> None:
    """FIB funds hold DIFFERENT papers of the SAME issuers: on a pure security ruler
    they share nothing at all. If the CUSIP-6 collapse ever stopped applying, they
    would not be a group — which is precisely the copy lock the granularity column
    carries."""
    books = collections.defaultdict(set)
    for series_id, _rdate, cusip, _isin, asset_class, _pct in anchor_rows():
        if series_id.startswith("FIB"):
            books[series_id].add((cusip, asset_class))
    even = {c for c, _ in books["FIB00"]}
    odd = {c for c, _ in books["FIB01"]}
    assert even & odd == set(), "the fixture no longer alternates paper lines"
    collapsed_even = {w.to_mixed(w.norm_id(c, None), a) for c, a in books["FIB00"]}
    collapsed_odd = {w.to_mixed(w.norm_id(c, None), a) for c, a in books["FIB01"]}
    assert len(collapsed_even & collapsed_odd) == 10


def test_the_fixture_identifier_ranges_do_not_collide() -> None:
    """An accidental shared security silently bridges two groups the fixture calls
    unrelated. This is the guard that keeps the golden meaningful."""
    books = collections.defaultdict(set)
    for series_id, _rdate, cusip, _isin, asset_class, _pct in anchor_rows():
        books[series_id].add(w.to_mixed(w.norm_id(cusip, None), asset_class))
    for disjoint in (f"EQN{i:02d}" for i in range(4)):
        for other, book in books.items():
            if other == disjoint:
                continue
            assert not (books[disjoint] & book), f"{disjoint} touches {other}"
    for j in range(6):
        for k in range(j + 1, 6):
            assert not (books[f"EQG{j:02d}"] & books[f"EQG{k:02d}"])


# =========================================================================== #
# 2. The frozen parameters and their digest                                   #
# =========================================================================== #
def test_the_shipped_parameters_hash_to_their_pinned_digest() -> None:
    assert w.params_sha256(w.canonical_params(**DEFAULT_PARAMS)) \
        == DEFAULT_PARAMS_SHA256


def test_the_frozen_constants_are_the_pre_registered_ones() -> None:
    """Transcribed from PREREG_P16 sections 1.1-1.3, 2 and 4, the P0/P1.5 universe
    rule, and the cap measurement's recommended arm. A change here is a recipe
    change, not a tuning."""
    assert w.SEED == 20260805
    assert w.THETA == 0.10
    assert w.RESOLUTION == 1.0
    assert w.LOUVAIN_THRESHOLD == 1e-07
    assert w.BLOCK_THRESHOLD == 0.70
    assert w.MAX_DEPTH == 3
    assert w.RESOLUTION_LADDER == (1.0, 2.0, 4.0)
    assert w.COHERENCE_MIN_MEDIAN == 0.05
    assert w.DEFAULT_SIZE_CAP_FRAC == 0.08
    assert w.DEFAULT_CAP_WAIVE_MIN_MEDIAN == 0.10
    assert w.DEFAULT_CAP_WAIVE_HARD_CEILING == 0.20
    assert w.MAX_LAG == "4 months 15 days"
    assert w.MIN_POSITIONS == 10
    assert w.MIN_COVERAGE == 0.50


@pytest.mark.parametrize("changed", [
    {"size_cap_frac": 0.12},
    {"cap_waive_min_median": 0.12},
    {"cap_waive_hard_ceiling": 0.25},
    {"ident_floor": 0.99},
])
def test_moving_any_cap_policy_parameter_moves_the_digest(changed) -> None:
    """All three cap parameters are IN the digest. Two anchors run under different
    cap policy must never be comparable by accident — the waiver threshold changes
    which communities survive whole, which is as much a recipe change as the cap."""
    assert w.params_sha256(w.canonical_params(**{**DEFAULT_PARAMS, **changed})) \
        != DEFAULT_PARAMS_SHA256


@pytest.mark.parametrize("value", ["0", "-0.1", "1.5", "abc"])
def test_an_inadmissible_size_cap_is_refused(monkeypatch, value) -> None:
    monkeypatch.setenv(w.SIZE_CAP_ENV, value)
    with pytest.raises(w.FundPeerGroupsError, match=w.SIZE_CAP_ENV):
        w.resolve_size_cap_frac()


def test_a_ceiling_below_the_cap_is_refused(monkeypatch) -> None:
    """It would make the waiver unreachable while the configuration claims to have
    one. Refuse rather than run a silently different policy."""
    monkeypatch.setenv(w.CAP_WAIVE_CEILING_ENV, "0.05")
    with pytest.raises(w.FundPeerGroupsError, match="below"):
        w.resolve_cap_waive_hard_ceiling(0.08)


# --------------------------------------------------------------------------- #
# The cohesion waiver — BOTH SIDES of the threshold                            #
# --------------------------------------------------------------------------- #
def test_the_waiver_keeps_a_cohesive_over_cap_group_whole(monkeypatch) -> None:
    """The 10-fund clique's median overlap is 0.80. Above the cap, under the
    ceiling, cohesive: it stays whole. This is the whole point of the waiver — the
    cap was never wrong in its value, it was wrong in being blind to cohesion."""
    _set_cap_policy(monkeypatch, cap=0.15, ceiling=0.30, waive=0.10)
    stats, database = run_worker(w, monkeypatch, today=TODAY)
    n = stats["n_universe"]
    assert stats["size_cap_nodes"] == pytest.approx(0.15 * n)
    assert stats["n_cap_waived"] >= 1
    waived_sizes = {c["size"] for c in stats["cap_waived"]}
    assert 10 in waived_sizes
    assert stats["largest_community"] == 10
    assert stats["n_oversized_after_cap"] == 0
    assert stats["n_above_hard_ceiling"] == 0
    clique = {f"EQC{i:02d}" for i in range(10)}
    groups = {r["group_id"] for r in database.published if r["series_id"] in clique}
    assert len(groups) == 1 and None not in groups


def test_below_the_waiver_threshold_the_same_group_is_split(monkeypatch) -> None:
    """Same universe, same cap, waiver raised above the clique's cohesion. It splits.

    A uniform synthetic clique shatters hard under the resolution ladder; real
    clusters are not uniform. What this asserts is the invariant — nothing above the
    cap survives once the waiver does not admit it."""
    _set_cap_policy(monkeypatch, cap=0.15, ceiling=0.30, waive=1.0)
    stats, database = run_worker(w, monkeypatch, today=TODAY)
    n = stats["n_universe"]
    assert stats["n_cap_waived"] == 0
    assert stats["largest_community"] <= 0.15 * n
    assert stats["n_oversized_after_cap"] == 0
    assert stats["n_communities"] > GOLDEN["stats"]["n_communities"]
    sizes = collections.Counter(r["group_id"] for r in database.published
                                if r["group_id"])
    assert all(size <= 0.15 * n for size in sizes.values())


def test_the_hard_ceiling_is_never_waived(monkeypatch) -> None:
    """The ceiling cuts between two EQUALLY cohesive groups, on size alone.

    Cap 7.35, ceiling 8.82. The 8-fund groups (median 0.80) are waived; the 10-fund
    clique (median 0.80, exactly as cohesive) is not — it is over the ceiling, and
    cohesion is the one thing that cannot buy past it."""
    _set_cap_policy(monkeypatch, cap=0.15, ceiling=0.18, waive=0.10)
    stats, database = run_worker(w, monkeypatch, today=TODAY)
    n = stats["n_universe"]
    cap, ceiling = 0.15 * n, 0.18 * n
    assert cap < 8 <= ceiling < 10, "the fixture must straddle both bounds"

    waived_sizes = sorted(c["size"] for c in stats["cap_waived"])
    assert waived_sizes == [8, 8]
    assert 10 not in waived_sizes
    assert stats["largest_community"] <= ceiling
    assert stats["n_above_hard_ceiling"] == 0
    assert stats["n_oversized_after_cap"] == 0

    # The clique does not survive as one group. What it becomes instead is the
    # resolution ladder's business — a uniform synthetic clique shatters, a real
    # cluster does not — so the claim asserted here is only that the ceiling held.
    clique = {f"EQC{i:02d}" for i in range(10)}
    clique_rows = [r for r in database.published if r["series_id"] in clique]
    assert len(clique_rows) == 10
    assert not any(r["group_size"] == 10 for r in clique_rows)


@pytest.mark.parametrize("median,expected", [
    (0.12, "waived"),
    (0.10, "waived"),        # the threshold itself admits
    (0.09997, "split"),      # the measured knife-edge: misses by 3e-5
    (0.05, "split"),
])
def test_the_waiver_threshold_is_exact_at_the_knife_edge(median, expected) -> None:
    """A real measured anchor carries a block of 1,006 funds with median 0.09997 that
    misses the waiver by 3e-5. The threshold is a parameter, not a magic constant, and
    the comparison has to be exact in both directions rather than approximately
    right — so this drives the decision with a stubbed median instead of contorting a
    fixture into producing one."""
    G = nx.complete_graph(12)
    nx.set_edge_attributes(G, 1.0, "weight")
    parts = w.partition_with_cap(G, 6.0, hard_ceiling=20.0,
                                 waive_min_median=0.10,
                                 median_of=lambda members: median)
    statuses = {status for _members, _depth, status in parts}
    if expected == "waived":
        assert parts == [(set(range(12)), 0, "waived")]
    else:
        assert "waived" not in statuses
        assert max(len(m) for m, _, _ in parts) <= 6


def test_a_singleton_over_the_cap_cannot_be_waived() -> None:
    """A community of one has no pair, therefore no median, therefore no cohesion to
    claim. It can never buy the waiver — which only matters if someone ever sets a
    cap below 1, but a None median silently comparing as "cohesive" is the kind of
    thing that only shows up in production."""
    G = nx.Graph()
    G.add_nodes_from(range(3))
    parts = w.partition_with_cap(G, 0.5, hard_ceiling=10.0, waive_min_median=0.0,
                                 median_of=lambda members: None)
    assert {status for _m, _d, status in parts} == {"irreducible"}


def test_the_default_policy_waives_by_cohesion_and_respects_the_ceiling(
        monkeypatch) -> None:
    """At the SHIPPED defaults on this universe: cap 3.92, ceiling 9.8. The cohesive
    8-fund groups are waived; the 10-fund clique is over the ceiling and is not."""
    stats, database = run_worker(w, monkeypatch, today=TODAY)
    n = stats["n_universe"]
    assert stats["size_cap_frac"] == w.DEFAULT_SIZE_CAP_FRAC
    assert stats["params_sha256"] == DEFAULT_PARAMS_SHA256
    assert stats["n_cap_waived"] >= 1
    assert stats["largest_community"] <= w.DEFAULT_CAP_WAIVE_HARD_CEILING * n
    assert stats["n_above_hard_ceiling"] == 0
    assert stats["n_oversized_after_cap"] == 0
    for community in stats["cap_waived"]:
        assert community["size"] > w.DEFAULT_SIZE_CAP_FRAC * n
        assert community["size"] <= w.DEFAULT_CAP_WAIVE_HARD_CEILING * n
        assert community["median_overlap"] >= w.DEFAULT_CAP_WAIVE_MIN_MEDIAN
    # every waived community is published as a real group, medians and all
    published_sizes = {r["group_size"] for r in database.published
                       if r["group_size"] is not None}
    assert {c["size"] for c in stats["cap_waived"]} <= published_sizes


def test_the_waiver_alert_fires_before_the_ceiling_does(monkeypatch) -> None:
    """The measured waived block grew to 16.77% of the universe with its biggest jump
    on the last anchor. The quarter it approaches 20% has to be visible before it
    arrives, not after."""
    _set_cap_policy(monkeypatch, cap=0.15, ceiling=0.30, waive=0.10)
    stats, _ = run_worker(w, monkeypatch, today=TODAY)
    n = stats["n_universe"]
    assert stats["cap_waive_alert_frac"] == w.CAP_WAIVE_ALERT_FRAC
    biggest = max(c["size"] for c in stats["cap_waived"])
    assert stats["cap_waive_alert"] == (biggest > w.CAP_WAIVE_ALERT_FRAC * n)
    assert stats["cap_waive_alert"] is True     # 10 of 49 is 20.4%, past the 15%


# =========================================================================== #
# 3. The identifier guard — the known 2024-11 / 2025-01 defect                #
# =========================================================================== #
def test_a_thin_identifier_anchor_is_refused_naming_the_number(monkeypatch) -> None:
    """One row in three without an identifier: coverage lands near 67%, far under the
    0.95 floor. The refusal has to carry the measurement, because the operator's next
    move is re-ingesting the named report dates."""
    with pytest.raises(w.FundPeerGroupsError) as excinfo:
        run_worker(w, monkeypatch, today=TODAY, rows=unidentified_rows(3))
    message = str(excinfo.value)
    assert "identifier coverage is BELOW the floor" in message
    assert "0.9500" in message
    assert re.search(r"rows 0\.6\d{3}", message), message
    assert "2025-11-30" in message                    # the report date is named
    assert w.IDENT_FLOOR_ENV in message


def test_the_guard_refuses_before_anything_is_written(monkeypatch) -> None:
    database = FakeDatabase(w, rows=unidentified_rows(3))
    conn = FakeConn(database)
    database.conn = conn
    monkeypatch.setattr(w, "connect", lambda dsn, **kw: conn)
    with pytest.raises(w.FundPeerGroupsError):
        w.run("postgresql://fake", today=TODAY)
    assert database.published == []
    assert database.deleted_anchors == []
    assert not any(sql is w.DELETE_ANCHOR_SQL for sql, _ in conn.executed)


def test_a_mildly_thin_anchor_is_still_refused_at_the_shipped_floor(
        monkeypatch) -> None:
    """One row in twelve: every fund still clears the eligibility floors, so the
    universe is INTACT and only the identifier guard stands between a 91.7% quarter
    and publication. That is the shape of the real defect — it does not announce
    itself by emptying the universe."""
    with pytest.raises(w.FundPeerGroupsError, match="identifier coverage is BELOW"):
        run_worker(w, monkeypatch, today=TODAY, rows=unidentified_rows(12))


def test_a_lowered_floor_lets_the_same_anchor_through(monkeypatch) -> None:
    """The floor is a parameter, not a law — but it is IN the digest, so an anchor
    published under a relaxed floor can never be mistaken for a clean one."""
    monkeypatch.setenv(w.IDENT_FLOOR_ENV, "0.5")
    stats, _ = run_worker(w, monkeypatch, today=TODAY, rows=unidentified_rows(12))
    assert stats["status"] == "published"
    assert stats["identifier_coverage_floor"] == 0.5
    assert stats["identifier_coverage"]["row_coverage"] < 0.95
    assert stats["params_sha256"] != DEFAULT_PARAMS_SHA256


def test_a_clean_anchor_reports_full_identifier_coverage(golden_run) -> None:
    stats, _ = golden_run
    coverage = stats["identifier_coverage"]
    assert coverage["row_coverage"] == 1.0
    assert coverage["weight_coverage"] == 1.0
    assert [d["report_date"] for d in coverage["by_report_date"]] == ["2025-11-30"]


def test_row_coverage_and_weight_coverage_are_both_gates() -> None:
    """2024-11-30 passed a 95% ROW floor and failed on WEIGHT. Guarding one alone
    would have let that date through."""
    heavy_but_rare = w.IdentifierCoverage()
    for _ in range(97):
        heavy_but_rare.observe("2025-11-30", 0.5, True)
    for _ in range(3):
        heavy_but_rare.observe("2025-11-30", 20.0, False)
    assert heavy_but_rare.row_coverage > 0.95
    assert heavy_but_rare.weight_coverage < 0.95
    with pytest.raises(w.FundPeerGroupsError, match="identifier coverage is BELOW"):
        w.check_identifier_coverage(heavy_but_rare, 0.95, ANCHOR)


# =========================================================================== #
# 4. The catalog gate — the worker never owns the schema                      #
# =========================================================================== #
def test_a_missing_table_is_refused_pointing_at_the_ddl(monkeypatch) -> None:
    with pytest.raises(w.FundPeerGroupsError) as excinfo:
        run_worker(w, monkeypatch, today=TODAY, catalog=[])
    assert "table missing from the public catalog" in str(excinfo.value)
    assert "schemas/fund_peer_groups_v1.sql" in str(excinfo.value)


def test_a_dropped_column_is_refused(monkeypatch) -> None:
    catalog = [row for row in catalog_rows(w) if row[0] != "params_sha256"]
    with pytest.raises(w.FundPeerGroupsError, match="missing=\\['params_sha256'\\]"):
        run_worker(w, monkeypatch, today=TODAY, catalog=catalog)


def test_an_extra_column_is_refused(monkeypatch) -> None:
    catalog = catalog_rows(w) + [("peer_label", "text", None, "YES", None)]
    with pytest.raises(w.FundPeerGroupsError, match="unexpected=\\['peer_label'\\]"):
        run_worker(w, monkeypatch, today=TODAY, catalog=catalog)


def test_a_retyped_column_is_refused(monkeypatch) -> None:
    """A bare name check waves this through; the signature comparison does not."""
    catalog = [(name, "text", None, nullable, default) if name == "group_size"
               else (name, dtype, length, nullable, default)
               for name, dtype, length, nullable, default in catalog_rows(w)]
    with pytest.raises(w.FundPeerGroupsError, match="group_size: signature"):
        run_worker(w, monkeypatch, today=TODAY, catalog=catalog)


def test_a_dropped_default_is_refused(monkeypatch) -> None:
    catalog = [(name, dtype, length, nullable, None) if name == "computed_at"
               else (name, dtype, length, nullable, default)
               for name, dtype, length, nullable, default in catalog_rows(w)]
    with pytest.raises(w.FundPeerGroupsError, match="computed_at: signature"):
        run_worker(w, monkeypatch, today=TODAY, catalog=catalog)


def test_run_never_issues_a_ddl_statement(golden_run) -> None:
    _, database = golden_run
    forbidden = re.compile(r"\b(create|alter|drop|truncate|grant)\b", re.IGNORECASE)
    for sql, _params in database.conn.executed:
        assert not forbidden.search(sql), sql


def test_a_busy_lock_publishes_nothing(monkeypatch) -> None:
    database = FakeDatabase(w, lock_free=False)
    conn = FakeConn(database)
    database.conn = conn
    monkeypatch.setattr(w, "connect", lambda dsn, **kw: conn)
    assert w.run("postgresql://fake", today=TODAY) == {"status": "lock_busy"}
    assert database.published == []


def test_a_non_public_search_path_is_refused(monkeypatch) -> None:
    class _Responder(FakeDatabase):
        def __call__(self, sql, params=None):
            if sql.startswith("SHOW search_path"):
                return {"rows": [("look_alike, public",)]}
            return super().__call__(sql, params)

    database = _Responder(w)
    conn = FakeConn(database)
    database.conn = conn
    monkeypatch.setattr(w, "connect", lambda dsn, **kw: conn)
    with pytest.raises(w.FundPeerGroupsError, match="search_path"):
        w.run("postgresql://fake", today=TODAY)


# =========================================================================== #
# 5. The DDL and the worker's expectation of it                               #
# =========================================================================== #
_COLUMN_RE = re.compile(
    r"^\s{4}(?P<name>[a-z_0-9]+)\s+(?P<type>DATE|TEXT|INTEGER|NUMERIC|TIMESTAMPTZ)"
    r"(?P<rest>.*)$")
_DDL_TYPES = {"DATE": "date", "TEXT": "text", "INTEGER": "integer",
              "NUMERIC": "numeric", "TIMESTAMPTZ": "timestamp with time zone"}


def _declared_columns() -> dict[str, tuple]:
    """The committed DDL's column signatures, in information_schema's vocabulary."""
    declared: dict[str, tuple] = {}
    for line in SCHEMA_PATH.read_text(encoding="utf-8").splitlines():
        match = _COLUMN_RE.match(line)
        if not match:
            continue
        rest = match.group("rest")
        nullable = "NO" if "NOT NULL" in rest else "YES"
        default = None
        if "DEFAULT" in rest:
            default = rest.split("DEFAULT", 1)[1].strip().rstrip(",").strip()
        declared[match.group("name")] = (
            _DDL_TYPES[match.group("type")], None, nullable, default)
    return declared


def test_the_worker_expectation_is_the_committed_ddl() -> None:
    """EXPECTED_COLUMNS is transcribed, not captured from a live catalog. This is what
    makes the transcription a checked claim rather than a hope."""
    assert _declared_columns() == w.EXPECTED_COLUMNS


def test_the_row_columns_are_exactly_the_table_columns() -> None:
    assert set(w.ROW_COLUMNS) == set(w.EXPECTED_COLUMNS)


def test_no_published_row_violates_a_ddl_check(golden_run) -> None:
    _, database = golden_run
    for row in database.published:
        assert row["block"] in ("fi", "eq", "mixed")
        assert row["group_state"] in ("empirical", "no_empirical_group")
        assert row["granularity"] == {"fi": "issuer", "eq": "security",
                                      "mixed": "mixed"}[row["block"]]
        empirical = row["group_state"] == "empirical"
        assert (row["group_id"] is not None) == empirical
        assert (row["group_size"] is not None) == empirical
        assert (row["group_median_overlap"] is not None) == empirical
        if empirical:
            assert row["group_size"] >= 2
            assert row["group_median_overlap"] >= w.COHERENCE_MIN_MEDIAN
            assert 0.0 <= row["group_median_overlap"] <= 1.0
            assert re.search(r":(fi|eq|mixed):[0-9]+$", row["group_id"])
            assert row["group_id"].startswith(f"{row['anchor_date'].isoformat()}:")
        assert re.fullmatch(r"[0-9a-f]{64}", row["params_sha256"])


def test_every_ddl_vocabulary_is_the_worker_vocabulary() -> None:
    ddl = SCHEMA_PATH.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS fund_peer_groups_v1" in ddl
    assert "does NOT repair drift" in ddl
    assert f"'{w.STATE_EMPIRICAL}'" in ddl and f"'{w.STATE_NO_GROUP}'" in ddl
    for block, granularity in w.BLOCK_GRANULARITY.items():
        assert f"'{block}'" in ddl and f"'{granularity}'" in ddl
    assert "PRIMARY KEY (anchor_date, series_id)" in ddl
    assert "ON fund_peer_groups_v1 (series_id)" in ddl


# =========================================================================== #
# 6. The universe, the anchor and the publication                             #
# =========================================================================== #
def test_the_universe_is_the_eligible_set_not_the_served_set(golden_run) -> None:
    stats, database = golden_run
    assert stats["n_served_universe"] == len(served_universe())
    assert sorted(r["series_id"] for r in database.published) == eligible_series()
    assert stats["reject"] == {"no_report_in_window": 2, "lt_min_positions": 1}


def test_an_empty_served_universe_is_refused(monkeypatch) -> None:
    with pytest.raises(w.FundPeerGroupsError, match="served universe is EMPTY"):
        run_worker(w, monkeypatch, today=TODAY, universe=[])


@pytest.mark.parametrize("today,expected", [
    (_dt.date(2026, 2, 15), "2025-12-31"),
    (_dt.date(2026, 5, 15), "2026-03-31"),
    (_dt.date(2026, 8, 15), "2026-06-30"),
    (_dt.date(2026, 11, 15), "2026-09-30"),
    (_dt.date(2026, 1, 1), "2025-12-31"),
    (_dt.date(2026, 3, 31), "2025-12-31"),
    (_dt.date(2026, 4, 1), "2026-03-31"),
])
def test_the_anchor_is_the_last_closed_quarter_end(today, expected) -> None:
    assert w.last_closed_quarter_end(today).isoformat() == expected
    assert w.resolve_anchor(today=today).isoformat() == expected


def test_a_mid_quarter_anchor_override_is_refused() -> None:
    with pytest.raises(w.FundPeerGroupsError, match="not a quarter-end"):
        w.resolve_anchor("2025-11-30", today=TODAY)


def test_an_anchor_in_the_future_is_refused() -> None:
    with pytest.raises(w.FundPeerGroupsError, match="beyond the last closed"):
        w.resolve_anchor("2026-03-31", today=TODAY)


def test_the_anchor_env_override_is_honoured(monkeypatch) -> None:
    monkeypatch.setenv(w.ANCHOR_ENV, "2025-09-30")
    assert w.resolve_anchor(today=TODAY).isoformat() == "2025-09-30"


def test_publication_replaces_the_anchor_in_one_transaction(golden_run) -> None:
    _, database = golden_run
    statements = [sql for sql, _ in database.conn.executed]
    assert statements.count(w.DELETE_ANCHOR_SQL) == 1
    delete_at = statements.index(w.DELETE_ANCHOR_SQL)
    first_insert = statements.index(w.INSERT_SQL)
    assert delete_at < first_insert, "the anchor must be cleared before inserting"
    assert database.conn.commits >= 1
    assert database.deleted_anchors == [ANCHOR]


def test_a_second_run_replaces_rather_than_duplicates(monkeypatch) -> None:
    _set_cap_policy(monkeypatch, cap=GOLDEN["size_cap_frac"],
                    ceiling=GOLDEN["cap_waive_hard_ceiling"],
                    waive=GOLDEN["cap_waive_min_median"])
    database = FakeDatabase(w)
    conn = FakeConn(database)
    database.conn = conn
    monkeypatch.setattr(w, "connect", lambda dsn, **kw: conn)
    first = w.run("postgresql://fake", today=TODAY)
    second = w.run("postgresql://fake", today=TODAY)
    assert first["n_published"] == second["n_published"] == len(database.published)
    assert len({r["computed_at"] for r in database.published}) == 1


def test_every_row_of_an_anchor_shares_one_timestamp(golden_run) -> None:
    _, database = golden_run
    assert len({r["computed_at"] for r in database.published}) == 1
    assert len({r["code_commit"] for r in database.published}) == 1
    assert len({r["params_sha256"] for r in database.published}) == 1


def test_post_write_verification_catches_a_short_publication(golden_run) -> None:
    """The verification reads the TABLE back. Dropping a row after the fact must be
    caught by it, otherwise it is only the worker agreeing with itself."""
    _, database = golden_run
    rows = [dict(r) for r in database.published]
    database.published = database.published[:-1]
    with pytest.raises(w.FundPeerGroupsError, match="row count"):
        w.post_write_verify(database.conn, ANCHOR, rows, cap=999.0,
                            hard_ceiling=999.0, waive_min_median=0.0)


def test_post_write_verification_catches_a_group_above_the_ceiling(golden_run) -> None:
    _, database = golden_run
    rows = [dict(r) for r in database.published]
    with pytest.raises(w.FundPeerGroupsError, match="above the hard ceiling"):
        w.post_write_verify(database.conn, ANCHOR, rows, cap=999.0,
                            hard_ceiling=5.0, waive_min_median=0.0)


def test_post_write_verification_catches_a_lying_group_size(golden_run) -> None:
    _, database = golden_run
    rows = [dict(r) for r in database.published]
    for row in database.published:
        if row["group_size"] is not None:
            row["group_size"] = row["group_size"] + 1
    for row in rows:
        if row["group_size"] is not None:
            row["group_size"] = row["group_size"] + 1
    with pytest.raises(w.FundPeerGroupsError, match="disagrees with the member count"):
        w.post_write_verify(database.conn, ANCHOR, rows, cap=999.0,
                            hard_ceiling=999.0, waive_min_median=0.0)


def test_post_write_verification_catches_a_mutated_value(golden_run) -> None:
    _, database = golden_run
    rows = [dict(r) for r in database.published]
    database.published[0] = dict(database.published[0], granularity="mixed"
                                 if rows[0]["granularity"] != "mixed" else "issuer")
    with pytest.raises(w.FundPeerGroupsError, match="granularity re-read as"):
        w.post_write_verify(database.conn, ANCHOR, rows, cap=999.0,
                            hard_ceiling=999.0, waive_min_median=0.0)


# =========================================================================== #
# 6b. The re-read of a NUMERIC column, at the precision Postgres actually keeps #
# =========================================================================== #
# The 2026-06-30 anchor published completely and then FAILED its own Gate 9, because
# the probe's median came back as Decimal('0.210447803139687') against a computed
# 0.21044780313968658. Nothing was wrong with the row: Postgres stores a float8
# parameter in a NUMERIC column through DBL_DIG = 15 significant digits, and an
# IEEE-754 double needs up to 17. 81 of that anchor's 87 distinct medians do not fit
# in 15, so the old zero-tolerance form was decided by which series sorted first.
PROBE_2026_06_30 = (decimal.Decimal("0.210447803139687"), 0.21044780313968658)
PROBE_2026_03_31 = (decimal.Decimal("0.213141173124313"), 0.213141173124313)


def _as_stored(value: float) -> decimal.Decimal:
    """What the column holds after a float8 parameter is cast to NUMERIC.

    The 15 is Postgres's DBL_DIG, stated here independently of the worker so these
    tests pin the STORAGE fact rather than the module's opinion of it; the module is
    cross-checked against it below."""
    return decimal.Decimal(f"{value:.15g}")


def test_the_worker_knows_what_the_cast_keeps() -> None:
    assert w.FLOAT8_TO_NUMERIC_DIGITS == 15


def _probe_first(published: list[dict], series_id: str) -> list[dict]:
    """``post_write_verify`` probes ``rows[0]``; put the series we mean there."""
    chosen = [r for r in published if r["series_id"] == series_id]
    return chosen + [r for r in published if r["series_id"] != series_id]


def _an_empirical_series(published: list[dict]) -> str:
    for row in sorted(published, key=lambda r: r["series_id"]):
        if row["group_median_overlap"] is not None:
            return row["series_id"]
    raise AssertionError("the golden fixture has no empirical row to probe")


def _store_median(database, series_id: str, value) -> None:
    for i, row in enumerate(database.published):
        if row["series_id"] == series_id:
            database.published[i] = dict(row, group_median_overlap=value)
            return
    raise AssertionError(f"{series_id} was never published")


def test_the_fact_sheet_probe_is_not_a_mismatch() -> None:
    """The exact pair the 2026-06-30 anchor refused on, and the 2026-03-31 pair that
    passed only because it happened to fit in 15 digits."""
    assert w._equal(*PROBE_2026_06_30) is True
    assert w._equal(*PROBE_2026_03_31) is True
    assert float(PROBE_2026_06_30[0]) != PROBE_2026_06_30[1]    # the loss is real


@pytest.mark.parametrize("stored", [
    # One digit off INSIDE the 15 Postgres keeps — the storage rendering cannot
    # explain it, so it stays a mismatch. This is the case that proves the fix is a
    # change of space and not a loosened tolerance.
    decimal.Decimal("0.210447803139688"),
    decimal.Decimal("0.210447803139686"),
    decimal.Decimal("0.21044780313969"),
    decimal.Decimal("0.310447803139687"),
    decimal.Decimal("0.5"),
    decimal.Decimal("0"),
])
def test_a_value_the_cast_cannot_explain_is_still_a_mismatch(stored) -> None:
    assert w._equal(stored, PROBE_2026_06_30[1]) is False


def test_a_present_value_never_equals_an_absent_one() -> None:
    assert w._equal(None, PROBE_2026_06_30[1]) is False
    assert w._equal(PROBE_2026_06_30[0], None) is False
    assert w._equal(None, None) is True


def test_the_readback_accepts_the_precision_the_column_stores(golden_run) -> None:
    """The production failure, reproduced end to end through the real gate: the row is
    intact and only its stored rendering differs, and Gate 9 must certify it."""
    _, database = golden_run
    series_id = _an_empirical_series(database.published)
    rows = _probe_first([dict(r) for r in database.published], series_id)
    computed = rows[0]["group_median_overlap"]
    stored = _as_stored(computed)
    assert float(stored) != computed        # the fixture really exercises the loss
    _store_median(database, series_id, stored)

    verified = w.post_write_verify(database.conn, ANCHOR, rows, cap=999.0,
                                   hard_ceiling=999.0, waive_min_median=0.0)
    assert verified["verified_rows"] == len(rows)


def test_the_readback_still_catches_a_median_that_moved(golden_run) -> None:
    """A median genuinely off by one unit in the LAST digit Postgres stores is still
    corruption, and is still named."""
    _, database = golden_run
    series_id = _an_empirical_series(database.published)
    rows = _probe_first([dict(r) for r in database.published], series_id)
    stored = _as_stored(rows[0]["group_median_overlap"])
    # one unit in the last digit the column actually holds
    moved = stored + decimal.Decimal(1).scaleb(stored.as_tuple().exponent)
    assert moved != stored
    _store_median(database, series_id, moved)
    with pytest.raises(w.FundPeerGroupsError,
                       match="group_median_overlap re-read as"):
        w.post_write_verify(database.conn, ANCHOR, rows, cap=999.0,
                            hard_ceiling=999.0, waive_min_median=0.0)


def test_no_series_can_be_the_probe_that_fails_the_anchor(golden_run) -> None:
    """The defect was that the verdict depended on WHICH series sorted first. With
    every median stored as the column keeps it, every row must certify the same
    anchor — the gate is a property of the data again, not of the row order."""
    _, database = golden_run
    published = [dict(r) for r in database.published]
    lossy = 0
    for row in published:
        if row["group_median_overlap"] is not None:
            stored = _as_stored(row["group_median_overlap"])
            lossy += float(stored) != row["group_median_overlap"]
            _store_median(database, row["series_id"], stored)
    assert lossy > 0, "the fixture must contain a median that does not fit in 15 digits"

    for i in range(len(published)):
        rotated = published[i:] + published[:i]
        w.post_write_verify(database.conn, ANCHOR, rotated, cap=999.0,
                            hard_ceiling=999.0, waive_min_median=0.0)


# =========================================================================== #
# 7. The pieces of the recipe, in isolation                                   #
# =========================================================================== #
@pytest.mark.parametrize("cusip,isin,expected", [
    ("037833100", None, "C:037833100"),
    ("037833100", "US0378331005", "C:037833100"),
    (None, "US0378331005", "C:037833100"),
    (None, "DE0005557508", "I:DE0005557508"),
    ("IS:US0378331005", None, "C:037833100"),
    ("IS:DE0005557508", None, "I:DE0005557508"),
    ("LE:SOME ENTITY", None, None),
    ("H:12345", None, None),
    ("CIK:0000320193", None, None),
    ("", "", None),
    (None, None, None),
    ("12345", None, None),
])
def test_norm_id_is_the_ported_identifier_rule(cusip, isin, expected) -> None:
    assert w.norm_id(cusip, isin) == expected


def test_norm_id_rejects_non_ascii_look_alikes() -> None:
    """``str.isalnum()`` would accept full-width digits. The ported patterns do not,
    and an identifier that is not ASCII is not an identifier."""
    assert w.norm_id("０３７８３３１００", None) is None
    assert w.norm_id(None, "ＵＳ0378331005") is None


def test_the_position_floor_counts_securities_not_issuers() -> None:
    """A bond fund holding twelve papers from four issuers has TWELVE positions.

    Counting the four would reject it for a thinness that is an artefact of the
    CUSIP-6 collapse, and the frozen recipe counts the security-level book for exactly
    that reason. This is the one place where the two books must not be confused."""
    rows = []
    for paper in range(12):
        issuer = paper // 3                       # four issuers, three papers each
        cusip = f"AA{issuer:04d}" + "ABC"[paper % 3] + "Z1"
        rows.append(("THICKPAPER", _dt.date(2025, 11, 30), cusip, None, "DBT",
                     100.0 / 12))
    for paper in range(8):
        rows.append(("THINBOOK", _dt.date(2025, 11, 30), f"BB{paper:04d}XZ1", None,
                     "DBT", 12.5))

    database = FakeDatabase(w, rows=rows, universe=["THICKPAPER", "THINBOOK"])
    conn = FakeConn(database)
    loaded = w.load_anchor(conn, ANCHOR, ["THICKPAPER", "THINBOOK"])

    assert set(loaded["weights"]) == {"THICKPAPER"}
    assert loaded["reject"]["lt_min_positions"] == 1
    assert loaded["meta"]["THICKPAPER"]["n_securities"] == 12
    assert loaded["meta"]["THICKPAPER"]["n_positions"] == 4     # after the collapse
    assert len(loaded["weights"]["THICKPAPER"]) == 4
    assert sum(loaded["weights"]["THICKPAPER"].values()) == pytest.approx(1.0)


def test_the_mixed_ruler_collapses_only_fixed_income() -> None:
    assert w.to_mixed("C:037833100", "EC") == "C:037833100"
    assert w.to_mixed("C:037833100", "DBT") == "C6:037833"
    assert w.to_mixed("C:037833100", "ABS-MBS") == "C6:037833"
    assert w.to_mixed("I:DE0005557508", "DBT") == "I:DE0005557508"
    assert w.to_mixed("C:037833100", None) == "C:037833100"


@pytest.mark.parametrize("fi,eq,expected", [
    (0.90, 0.05, "FI"),
    (0.70, 0.30, "FI"),
    (0.05, 0.90, "EQ"),
    (0.30, 0.70, "EQ"),
    (0.69, 0.31, "MIXED"),
    (0.48, 0.52, "MIXED"),
    (0.0, 0.0, "MIXED"),
])
def test_the_block_pre_split_is_the_seventy_percent_rule(fi, eq, expected) -> None:
    assert w.block_of({"fi_share": fi, "eq_share": eq}) == expected


def test_no_edge_crosses_a_block(golden_run) -> None:
    """Bond x equity contamination is zero BY CONSTRUCTION, not by inspection: the
    three blocks are separate graphs. This asserts the construction."""
    _, database = golden_run
    by_group = collections.defaultdict(set)
    for row in database.published:
        if row["group_id"]:
            by_group[row["group_id"]].add(row["block"])
    assert all(len(blocks) == 1 for blocks in by_group.values())


def test_the_overlap_matrix_is_the_raw_min_sum() -> None:
    weights = {
        "A": {"x": 0.5, "y": 0.3, "z": 0.2},
        "B": {"x": 0.4, "y": 0.6},
        "C": {"q": 1.0},
    }
    M = w.overlap_matrix(["A", "B", "C"], weights)
    assert M[0][1] == pytest.approx(0.4 + 0.3, rel=1e-6)      # min(.5,.4)+min(.3,.6)
    assert M[0][2] == 0.0
    assert M[0][0] == 1.0 and M[1][1] == 1.0
    assert M[0][1] == M[1][0]


def test_the_coherence_floor_is_a_median_not_a_mean() -> None:
    """Six rich pairs and fifteen empty ones: the MEAN calls that a group, the MEDIAN
    refuses to. A hub that overlaps everyone while nobody else overlaps anybody is
    exactly the shape a mean would launder into a peer group."""
    import numpy as np
    values = np.array([0.30] * 6 + [0.0] * 15, dtype=np.float32)
    assert float(values.mean()) > w.COHERENCE_MIN_MEDIAN
    assert float(np.median(values)) < w.COHERENCE_MIN_MEDIAN


def test_the_star_in_the_fixture_is_a_connected_non_group(golden_run) -> None:
    """The star is CONNECTED — every spoke has a real 12% edge to the hub — and it is
    still not a peer group. 'no_empirical_group' is not a synonym for 'isolated'."""
    _, database = golden_run
    star = {f"EQG{i:02d}" for i in range(6)} | {"EQGHUB"}
    rows = [r for r in database.published if r["series_id"] in star]
    assert len(rows) == 7
    assert {r["group_state"] for r in rows} == {"no_empirical_group"}
    assert {r["group_id"] for r in rows} == {None}
    assert {r["block"] for r in rows} == {"eq"}


def test_the_worker_is_registered_for_railway() -> None:
    """WORKER=fund_peer_groups has to be a name run_worker admits, and the service
    contract has to be documented where the operator looks for it."""
    entry_point = (ROOT / "src" / "run_worker.py").read_text(encoding="utf-8")
    assert "fund_peer_groups" in entry_point
    railway = (ROOT / "railway.toml").read_text(encoding="utf-8")
    assert "fund_peer_groups" in railway
    assert "docs/fund_peer_groups_runbook.md" in railway
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert "networkx" in requirements


def test_the_advisory_lock_is_unique_to_this_worker() -> None:
    from src import db
    assert db.LOCK_FUND_PEER_GROUPS == 900_219
    others = [value for name, value in vars(db).items()
              if name.startswith("LOCK_") and name != "LOCK_FUND_PEER_GROUPS"]
    assert db.LOCK_FUND_PEER_GROUPS not in others
