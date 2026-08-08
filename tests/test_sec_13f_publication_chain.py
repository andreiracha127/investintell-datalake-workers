from contextlib import contextmanager

import pytest

from src.workers import sec_13f_publication_chain as chain


class _Conn:
    def __enter__(self): return self
    def __exit__(self, *_args): return False


@contextmanager
def _lock(_conn, _lock_id):
    yield True


def _wire(monkeypatch):
    monkeypatch.setattr(chain, "connect", lambda _dsn, autocommit=False: _Conn())
    monkeypatch.setattr(chain, "advisory_lock", _lock)


def test_refresh_order_and_affected_quarter_window(monkeypatch):
    _wire(monkeypatch)
    events = []
    monkeypatch.setattr(chain, "_refresh_mv", lambda _d, name, **_k: events.append(name))
    monkeypatch.setattr(
        chain, "_refresh_caggs",
        lambda _d, start, end: events.append(("caggs", start.isoformat(), end.isoformat())),
    )
    def ingestion(*_args, **_kwargs):
        return {
            "affected_report_date_start": "2025-02-14",
            "affected_report_date_end": "2025-05-15",
            "upserted": 3,
        }
    result = chain.run("db", ingestion_runner=ingestion)
    assert events == [
        "fund_reveal_13f_holdings_mv",
        "holding_reverse_lookup_mv",
        ("caggs", "2025-01-01", "2025-07-01"),
    ]
    assert result["published"] is True


def test_ingestion_failure_prevents_all_refreshes(monkeypatch):
    _wire(monkeypatch)
    monkeypatch.setattr(chain, "_refresh_mv", lambda *_a, **_k: pytest.fail("refresh called"))
    with pytest.raises(RuntimeError, match="did not complete"):
        chain.run("db", ingestion_runner=lambda *_a, **_k: {"failed_packages": 1})


def test_no_changed_filings_is_idempotent_noop(monkeypatch):
    _wire(monkeypatch)
    result = chain.run("db", ingestion_runner=lambda *_a, **_k: {"upserted": 0})
    assert result["refresh_skipped"] == "no_changed_filings"


def test_refresh_only_mode_uses_canonical_source_window_without_legacy_ingestion(monkeypatch):
    _wire(monkeypatch)
    monkeypatch.setenv("SEC_13F_REFRESH_ONLY", "1")
    monkeypatch.setattr(
        chain,
        "_canonical_source_window",
        lambda _dsn: {
            "source": "canonical_sec_13f_holdings",
            "affected_report_date_start": "2026-03-31",
            "affected_report_date_end": "2026-03-31",
        },
    )
    monkeypatch.setattr(
        chain,
        "_refresh_mv",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(chain, "_refresh_caggs", lambda *_args: None)

    def legacy_ingestion(*_args, **_kwargs):
        pytest.fail("legacy ingestion must not run against the canonical production schema")

    result = chain.run("db", ingestion_runner=legacy_ingestion)

    assert result["published"] is True
    assert result["stages"][0]["stats"]["source"] == "canonical_sec_13f_holdings"


def test_middle_step_failure_stops_caggs(monkeypatch):
    _wire(monkeypatch)
    calls = []

    def refresh(_dsn, name, **_kwargs):
        calls.append(name)
        if name == "holding_reverse_lookup_mv":
            raise RuntimeError("reverse lookup failed")

    monkeypatch.setattr(chain, "_refresh_mv", refresh)
    monkeypatch.setattr(chain, "_refresh_caggs", lambda *_a: pytest.fail("CAGG called"))
    def source(*_args, **_kwargs):
        return {
            "affected_report_date_start": "2025-03-31",
            "affected_report_date_end": "2025-03-31",
        }
    with pytest.raises(RuntimeError, match="reverse lookup failed"):
        chain.run("db", ingestion_runner=source)
    assert calls == ["fund_reveal_13f_holdings_mv", "holding_reverse_lookup_mv"]


def test_outer_lock_contention(monkeypatch):
    monkeypatch.setattr(chain, "connect", lambda _dsn: _Conn())

    @contextmanager
    def busy(_conn, _lock_id): yield False

    monkeypatch.setattr(chain, "advisory_lock", busy)
    assert chain.run("db")["skipped"] == "lock_busy"
