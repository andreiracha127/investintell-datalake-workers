"""A synthetic N-PORT anchor with a KNOWN group structure, plus a fake database.

WHY SYNTHETIC AND NOT A SLICE OF THE REAL ANCHOR
------------------------------------------------
The reference anchor of the frozen recipe is 7023 series and a 7023 x 7023 float32
overlap matrix (~197 MB) built from tens of millions of holding rows. Committing it to
re-assert numbers the fact sheet already carries would put a 200 MB artifact in the
repo. This fixture instead builds a 49-fund universe whose group structure is
DESIGNED, so the golden partition can be read and argued with rather than merely
trusted, and the real anchor is reproduced by an operator run
(``docs/fund_peer_groups_runbook.md``).

WHAT THE UNIVERSE IS DESIGNED TO EXERCISE
-----------------------------------------
  FI_A   8 funds  a clean fixed-income cluster (shared issuers, one paper each)
  FI_B   8 funds  the SAME issuers seen through DIFFERENT papers: even funds hold
                  the ``AA1`` line, odd funds the ``AB2`` line of every issuer. On a
                  pure security ruler these funds share NOTHING; they are a group only
                  because fixed income is collapsed to the CUSIP-6 issuer. This is the
                  mixed ruler under test, not a decoration.
  EQ_C  10 funds  a uniform equity clique — the group the SIZE CAP has to break
  EQ_D   6 funds  a second, smaller equity clique
  EQ_G   7 funds  a STAR: one hub sharing 12% with each of six spokes, spokes sharing
                  nothing with each other. Louvain reads a star as ONE community, and
                  15 of its 21 pairs share zero portfolio, so its median is 0 and it is
                  NOT a peer group. This is the 'no_empirical_group' state with a
                  connected community behind it — the case the product exists to be
                  honest about.
  EQ_N   4 funds  disjoint books: isolated nodes, singletons, no group
  MX     6 funds  balanced (48% fixed income / 52% equity): neither side reaches the
                  0.70 block threshold, so they land in 'mixed'
  plus   1 fund   with 6 positions — fails the >= 10 identified positions floor
  plus   1 fund   with a report OUTSIDE the lag window
  plus   1 fund   served but never filed
"""

from __future__ import annotations

import datetime as _dt
from typing import Any, Iterable

ANCHOR = _dt.date(2025, 12, 31)
REPORT_DATE = _dt.date(2025, 11, 30)
STALE_REPORT_DATE = _dt.date(2025, 3, 31)      # outside the 4 months 15 days window

# (series_id, report_date, cusip, isin, asset_class, pct_of_nav)
Row = tuple[str, _dt.date, str | None, str | None, str | None, float]


def _cusip(prefix: str, number: int, tail: str) -> str:
    """A 9-character alphanumeric CUSIP whose first SIX characters are the issuer.

    Two papers of one issuer differ only in the tail, which is exactly what the mixed
    ruler collapses."""
    issuer = f"{prefix}{number:04d}"
    assert len(issuer) == 6, issuer
    security = issuer + tail
    assert len(security) == 9 and security.isalnum(), security
    return security


def _fi_paper(issuer: int, line: str = "AA1") -> str:
    return _cusip("FI", issuer, line)


def _eq_paper(number: int) -> str:
    return _cusip("EQ", number, "ZZ9")


def _rows_for(series_id: str, holdings: list[tuple[str, str, float]],
              report_date: _dt.date = REPORT_DATE) -> list[Row]:
    return [(series_id, report_date, cusip, None, asset_class, pct)
            for cusip, asset_class, pct in holdings]


def _fi_group(tag: str, n_funds: int, core: int, private: int, *,
              alternate_lines: bool) -> list[Row]:
    """``n_funds`` bond funds over ten shared issuers, plus two private issuers each.

    With ``alternate_lines`` the funds split across two papers of the SAME issuer
    (``AA1`` / ``AB2``), so on a pure security ruler they share nothing and they are a
    group ONLY after the CUSIP-6 collapse."""
    rows: list[Row] = []
    for f in range(n_funds):
        line = ("AA1", "AB2")[f % 2] if alternate_lines else "AA1"
        holdings = [(_fi_paper(core + i, line), "DBT", 8.0) for i in range(10)]
        holdings += [(_fi_paper(private + 10 * f + k, "AA1"), "DBT", 10.0)
                     for k in range(2)]
        rows += _rows_for(f"{tag}{f:02d}", holdings)
    return rows


def _eq_clique(tag: str, n_funds: int, core: int, private: int) -> list[Row]:
    rows: list[Row] = []
    for f in range(n_funds):
        holdings = [(_eq_paper(core + i), "EC", 8.0) for i in range(10)]
        holdings += [(_eq_paper(private + 10 * f + k), "EC", 10.0) for k in range(2)]
        rows += _rows_for(f"{tag}{f:02d}", holdings)
    return rows


def _eq_star(tag: str, n_spokes: int, link: int, hub_fill: int,
             spoke_fill: int) -> list[Row]:
    """A hub joined to every spoke, spokes joined to nothing.

    hub-spoke overlap = min(0.14, 0.12) = 0.12, comfortably above the 0.10 edge
    threshold; spoke-spoke overlap = 0. Louvain reads that as ONE community, and 15 of
    its 21 pairs are empty, so the median is 0 and the community is not a peer group."""
    rows: list[Row] = []
    hub = [(_eq_paper(link + j), "EC", 14.0) for j in range(n_spokes)]
    hub += [(_eq_paper(hub_fill + k), "EC", 2.0) for k in range(8)]
    rows += _rows_for(f"{tag}HUB", hub)
    for j in range(n_spokes):
        spoke = [(_eq_paper(link + j), "EC", 12.0)]
        spoke += [(_eq_paper(spoke_fill + 20 * j + k), "EC", 8.0) for k in range(11)]
        rows += _rows_for(f"{tag}{j:02d}", spoke)
    return rows


def _eq_disjoint(tag: str, n_funds: int, base: int) -> list[Row]:
    """Funds whose books touch nothing else in the universe: isolated nodes."""
    rows: list[Row] = []
    for f in range(n_funds):
        holdings = [(_eq_paper(base + 20 * f + k), "EC",
                     10.0 if k < 4 else 6.0) for k in range(12)]
        rows += _rows_for(f"{tag}{f:02d}", holdings)
    return rows


def _mixed_group(tag: str, n_funds: int, bonds: int, equities: int,
                 private: int) -> list[Row]:
    """48% fixed income, 52% equity — neither side reaches the 0.70 block threshold."""
    rows: list[Row] = []
    for f in range(n_funds):
        holdings = [(_fi_paper(bonds + i), "DBT", 8.0) for i in range(6)]
        holdings += [(_eq_paper(equities + i), "EC", 8.0) for i in range(6)]
        holdings += [(_eq_paper(private + 10 * f), "EC", 4.0)]
        rows += _rows_for(f"{tag}{f:02d}", holdings)
    return rows


def anchor_rows() -> list[Row]:
    """Every holding row of the synthetic anchor, in a deterministic order.

    The identifier ranges below are DISJOINT by construction. That is not cosmetic: an
    accidental collision silently bridges two groups that the fixture claims are
    unrelated, and the golden would then lock in a structure nobody designed. The
    ranges are asserted in ``tests/test_fund_peer_groups.py``."""
    rows: list[Row] = []
    #                            core   private
    rows += _fi_group("FIA", 8, 1001, 1100, alternate_lines=False)
    rows += _fi_group("FIB", 8, 2001, 2100, alternate_lines=True)
    rows += _eq_clique("EQC", 10, 1001, 1100)
    rows += _eq_clique("EQD", 6, 2001, 2100)
    #                        spokes  link  hub_fill  spoke_fill
    rows += _eq_star("EQG", 6, 3001, 3101, 3200)
    rows += _eq_disjoint("EQN", 4, 4001)
    #                            bonds  equities  private
    rows += _mixed_group("MXF", 6, 3001, 5001, 5101)

    # too few positions: rejected by the >= 10 identified positions floor
    rows += _rows_for("REJ00", [(_eq_paper(6001 + k), "EC", 16.0) for k in range(6)])
    # a report outside the lag window: never enters the universe
    rows += _rows_for("OLD00", [(_eq_paper(6101 + k), "EC", 8.0) for k in range(12)],
                      report_date=STALE_REPORT_DATE)
    return rows


def served_universe() -> list[str]:
    """Every series the fake product serves, including two that never partition."""
    series = sorted({row[0] for row in anchor_rows()})
    return sorted(series + ["NEVERFILED00"])


def eligible_series() -> list[str]:
    """The series that survive the eligibility floors, sorted as the worker sorts."""
    return sorted({row[0] for row in anchor_rows()
                   if row[0] not in ("REJ00", "OLD00")})


def unidentified_rows(fraction_of_rows: int = 3) -> list[Row]:
    """The same anchor with every ``fraction_of_rows``-th row's identifier stripped.

    ``LE:`` is the legal-entity stand-in the ingest writes when no security identifier
    was filed: it normalises to None and is exactly what the 2024-11/2025-01 hole
    looked like."""
    out: list[Row] = []
    for i, (sid, rdate, cusip, isin, asset_class, pct) in enumerate(anchor_rows()):
        if i % fraction_of_rows == 0:
            out.append((sid, rdate, "LE:UNIDENTIFIED ENTITY", None, asset_class, pct))
        else:
            out.append((sid, rdate, cusip, isin, asset_class, pct))
    return out


# --------------------------------------------------------------------------- #
# The fake database
# --------------------------------------------------------------------------- #
def catalog_rows(module) -> list[tuple]:
    """``information_schema.columns`` as the committed DDL would produce it."""
    return [(name, *signature) for name, signature in module.EXPECTED_COLUMNS.items()]


class FakeCursor:
    def __init__(self, conn, name: str | None = None) -> None:
        self.conn = conn
        self.name = name
        self.itersize = 0
        self.rowcount = -1
        self._rows: list[tuple] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def __iter__(self):
        return iter(self._rows)

    def execute(self, sql, params=None) -> None:
        self.conn.executed.append((sql, params))
        response = self.conn.responder(sql, params)
        self.rowcount = response.get("rowcount", len(response.get("rows", [])))
        self._rows = response.get("rows", [])

    def executemany(self, sql, seq_of_params) -> None:
        """psycopg3 pipelines these; the fake replays them one by one so the
        recorded statement log stays row-accurate."""
        total = 0
        for params in seq_of_params:
            self.execute(sql, params)
            total += self.rowcount
        self.rowcount = total
        self._rows = []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    def __init__(self, responder) -> None:
        self.executed: list = []
        self.commits = 0
        self.closed = False
        self.responder = responder

    def cursor(self, name: str | None = None) -> FakeCursor:
        return FakeCursor(self, name)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class FakeDatabase:
    """Serves the synthetic anchor and records every published row."""

    def __init__(self, module, *, rows: Iterable[Row] | None = None,
                 universe: list[str] | None = None,
                 catalog: list[tuple] | None = None,
                 lock_free: bool = True) -> None:
        self.module = module
        self.rows = list(anchor_rows() if rows is None else rows)
        self.universe = served_universe() if universe is None else universe
        self.catalog = catalog_rows(module) if catalog is None else catalog
        self.lock_free = lock_free
        self.published: list[dict[str, Any]] = []
        self.deleted_anchors: list[Any] = []
        self.conn: FakeConn | None = None

    # -- the served relations ---------------------------------------------- #
    def _holdings(self, params) -> list[tuple]:
        anchor = params["anchor"]
        wanted = set(params["series"])
        window_start = anchor - _dt.timedelta(days=137)     # 4 months 15 days
        eligible = [r for r in self.rows
                    if r[0] in wanted and window_start <= r[1] <= anchor]
        latest: dict[str, _dt.date] = {}
        for sid, rdate, *_ in eligible:
            if rdate > latest.get(sid, _dt.date.min):
                latest[sid] = rdate
        return [r for r in eligible if r[1] == latest[r[0]]]

    def __call__(self, sql, params=None):
        params = params or {}
        w = self.module
        if "pg_try_advisory_lock" in sql:
            return {"rows": [(self.lock_free,)]}
        if "pg_advisory_unlock" in sql:
            return {"rows": [(1,)]}
        if sql.startswith("SET search_path"):
            return {"rows": []}
        if sql.startswith("SHOW search_path"):
            return {"rows": [("public",)]}
        if sql is w.CATALOG_COLUMNS_SQL:
            return {"rows": list(self.catalog)}
        if sql is w.SERVED_UNIVERSE_SQL:
            return {"rows": [(s,) for s in self.universe]}
        if sql is w.HOLDINGS_SQL:
            return {"rows": self._holdings(params)}
        if sql is w.DELETE_ANCHOR_SQL:
            before = len(self.published)
            self.published = [r for r in self.published
                              if r["anchor_date"] != params["anchor"]]
            self.deleted_anchors.append(params["anchor"])
            return {"rowcount": before - len(self.published)}
        if sql is w.INSERT_SQL:
            self.published.append(dict(params))
            return {"rowcount": 1}
        if sql is w.VERIFY_COUNTS_SQL:
            rows = self._anchor_rows(params["anchor"])
            empirical = [r for r in rows if r["group_state"] == "empirical"]
            inconsistent = sum(
                1 for r in rows
                if (r["group_state"] == "empirical") != (r["group_id"] is not None))
            stamps = {r["computed_at"] for r in rows}
            sizes = [r["group_size"] for r in rows if r["group_size"] is not None]
            return {"rows": [(len(rows), len(empirical), len(stamps), inconsistent,
                              max(sizes, default=0))]}
        if sql is w.VERIFY_GROUP_SIZES_SQL:
            counts: dict[tuple, int] = {}
            for r in self._anchor_rows(params["anchor"]):
                if r["group_id"] is None:
                    continue
                key = (r["group_id"], r["group_size"])
                counts[key] = counts.get(key, 0) + 1
            return {"rows": [(gid, size, n) for (gid, size), n in counts.items()
                             if n != size][:5]}
        if sql is w.READBACK_SQL:
            for r in self._anchor_rows(params["anchor"]):
                if r["series_id"] == params["series_id"]:
                    return {"rows": [tuple(r[c] for c in w.ROW_COLUMNS)]}
            return {"rows": []}
        raise AssertionError(f"unexpected SQL in the worker: {sql[:160]!r}")

    def _anchor_rows(self, anchor) -> list[dict[str, Any]]:
        return [r for r in self.published if r["anchor_date"] == anchor]


def run_worker(module, monkeypatch, *, anchor: str | None = None,
               today: _dt.date | None = None, **database_kwargs):
    """Drive ``module.run`` against the fake database. No PostgreSQL anywhere."""
    database = FakeDatabase(module, **database_kwargs)
    conn = FakeConn(database)
    database.conn = conn
    monkeypatch.setattr(module, "connect", lambda dsn, **kw: conn)
    stats = module.run("postgresql://fake", anchor=anchor,
                       today=today or _dt.date(2026, 2, 15))
    return stats, database
