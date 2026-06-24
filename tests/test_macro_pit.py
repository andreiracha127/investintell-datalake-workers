# tests/test_macro_pit.py
import datetime as dt

from src import macro_pit


class _Cur:
    def __init__(self, rows): self._rows = rows
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=None): self._sql, self._params = sql, params
    def fetchall(self): return self._rows


class _Conn:
    def __init__(self, rows): self._rows = rows
    def cursor(self): return _Cur(self._rows)


def test_latest_vintage_as_of_picks_value_known_at_decision_time() -> None:
    # DB returns DISTINCT ON (series, period) latest available_at<=cutoff, already filtered.
    rows = [
        ("PAYEMS", dt.date(2010, 3, 1), 129871.0),
        ("PAYEMS", dt.date(2010, 4, 1), 130161.0),
    ]
    conn = _Conn(rows)
    out = macro_pit.latest_vintage_as_of(
        conn, ["PAYEMS"], dt.datetime(2010, 6, 1, tzinfo=dt.timezone.utc)
    )
    assert out == {"PAYEMS": {dt.date(2010, 3, 1): 129871.0, dt.date(2010, 4, 1): 130161.0}}


def test_latest_vintage_as_of_passes_cutoff_and_series() -> None:
    conn = _Conn([])
    cur_holder = {}
    orig = _Conn.cursor

    def _spy(self):
        c = orig(self)
        cur_holder["c"] = c
        return c
    _Conn.cursor = _spy
    cutoff = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    macro_pit.latest_vintage_as_of(conn, ["A", "B"], cutoff)
    _Conn.cursor = orig
    assert "available_at <= " in cur_holder["c"]._sql
    assert cur_holder["c"]._params[0] == ["A", "B"]
    assert cur_holder["c"]._params[1] == cutoff
