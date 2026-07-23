"""Regression contracts for activation-worker source pinning and qualification."""

from __future__ import annotations

from datetime import date
from uuid import UUID

from src.workers import bond_security_master as bond_worker
from src.workers import mixed_quant_publication as mixed_quant_worker


class _Result:
    def __init__(self, *, row: tuple[bool] | None = None, rowcount: int = 0) -> None:
        self._row = row
        self.rowcount = rowcount

    def fetchone(self) -> tuple[bool] | None:
        return self._row


class _RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> _Result:
        self.calls.append((query, params))
        if "to_regclass" in query:
            return _Result(row=(True,))
        return _Result(rowcount=1)


def test_bond_observation_load_is_pinned_to_the_resolved_nport_publication() -> None:
    conn = _RecordingConnection()
    as_of = date(2026, 7, 23)
    publication_id = UUID("11111111-1111-4111-8111-111111111111")

    assert bond_worker._load_nport_observations(conn, as_of, publication_id) == 1

    statement, params = conn.calls[-1]
    assert "FROM sec_nport_holdings_v2_current h" in statement
    assert "h.publication_id=%s" in statement
    assert params == (publication_id, as_of, as_of)


def test_cusip_identities_require_the_same_qualified_map_row_as_tickers_and_returns() -> None:
    conn = _RecordingConnection()

    mixed_quant_worker._populate_native_observations(conn, date(2026, 7, 23))

    cusip_identity_sql = next(
        query for query, _ in conn.calls if "md5('equity-cusip|" in query
    )

    assert "JOIN sec_cusip_ticker_map m ON m.cusip=left(h.cusip,6)" in cusip_identity_sql
    assert "LEFT JOIN sec_cusip_ticker_map" not in cusip_identity_sql
    assert "m.is_tradeable" in cusip_identity_sql
    assert "m.security_type NOT IN ('ETP','Open-End Fund','Closed-End Fund')" in cusip_identity_sql
    assert "IN ('EC','EP')" in cusip_identity_sql
