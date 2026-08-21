"""Focused contract tests for the official Federal Reserve SEP ingestion lane."""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
from decimal import Decimal
from pathlib import Path

import pytest

from src import db
from src.workers import fomc_sep_ingestion as sep

FIXTURES = Path("tests/fixtures/fomc_sep")


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _by_horizon(artifact: sep.ReleaseArtifact) -> dict[str, int]:
    totals: dict[str, int] = {}
    for row in artifact.distributions:
        totals[row.projection_horizon] = totals.get(row.projection_horizon, 0) + row.participant_count
    return totals


def test_quarter_point_fixture_is_normalized() -> None:
    artifact = sep.parse_release(
        _fixture("quarter_point.html"),
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm",
    )
    assert artifact.source_format == "quarter_point"
    assert artifact.release_date == dt.date(2012, 9, 13)
    assert _by_horizon(artifact) == {"2012": 19, "2013": 17, "2014": 18, "longer_run": 17}
    assert all(row.bin_kind == "point" for row in artifact.distributions)


def test_eighth_point_fixture_is_normalized() -> None:
    artifact = sep.parse_release(
        _fixture("eighth_point.html"),
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20161214.htm",
    )
    assert artifact.source_format == "eighth_point"
    assert _by_horizon(artifact) == {"2016": 19, "2017": 5, "2018": 4, "longer_run": 11}
    assert any(str(row.rate_bin_low) == "0.625" for row in artifact.distributions)


def test_range_bin_fixture_selects_only_current_release_columns() -> None:
    artifact = sep.parse_release(
        _fixture("range_bins.html"),
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
    )
    assert artifact.source_format == "range_bins"
    assert _by_horizon(artifact) == {"2023": 20, "2024": 17, "longer_run": 14}
    assert all(row.bin_kind == "range" for row in artifact.distributions)
    assert all(row.rate_bin_low < row.rate_bin_high for row in artifact.distributions)


def test_source_hash_is_over_exact_bytes() -> None:
    lf = b"<html>\n<body>SEP</body>\n</html>\n"
    crlf = lf.replace(b"\n", b"\r\n")
    assert sep.source_sha256(lf) == hashlib.sha256(lf).hexdigest()
    assert sep.source_sha256(crlf) == hashlib.sha256(crlf).hexdigest()
    assert sep.source_sha256(lf) != sep.source_sha256(crlf)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "The Committee decided to keep the target range for the federal "
            "funds rate at 0 to 1/4 percent.",
            (Decimal("0.000"), Decimal("0.250"), Decimal("0.125")),
        ),
        (
            "The Committee decided to maintain the target range for the federal "
            "funds rate at 4-1/4 to 4-1/2 percent.",
            (Decimal("4.250"), Decimal("4.500"), Decimal("4.375")),
        ),
    ],
)
def test_policy_statement_parser_preserves_official_target_range(
    text: str, expected: tuple[Decimal, Decimal, Decimal]
) -> None:
    content = f"<html><body>{text}</body></html>".encode()
    assert sep.parse_policy_rate(
        content,
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm",
    ) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://sec-api.io/monetarypolicy/fomcprojtabl20231213.htm",
        "https://www.federalreserve.gov.evil.test/monetarypolicy/fomcprojtabl20231213.htm",
        "http://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm?download=1",
    ],
)
def test_source_allowlist_fails_closed(url: str) -> None:
    with pytest.raises(sep.SepIngestionError, match="refusing"):
        sep.canonical_release_url(url)


def test_discovery_keeps_only_official_projection_links() -> None:
    index = b"""
        <html><body>
        <a href="/monetarypolicy/fomcprojtabl20120913.htm">Projection Materials HTML</a>
        <a href="/monetarypolicy/files/fomcprojtabl20120913.pdf">Projection Materials PDF</a>
        <a href="/monetarypolicy/fomcminutes20120913.htm">Minutes</a>
        </body></html>
    """
    urls = sep.discover_release_urls(
        [("https://www.federalreserve.gov/monetarypolicy/fomchistorical2012.htm", index)],
        dt.date(2012, 12, 31),
    )
    assert urls == [
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm"
    ]


def test_historical_backfill_urls_are_official_and_start_in_2012() -> None:
    urls = sep._historical_release_urls(dt.date(2020, 12, 31))
    assert urls[0].endswith("fomcprojtabl20120125.htm")
    assert urls[-1].endswith("fomcprojtabl20201216.htm")
    assert len(urls) == 35
    assert all(sep.canonical_release_url(url) == url for url in urls)


def test_parser_fails_closed_without_policy_distribution() -> None:
    content = b"<html><body>Board of Governors Federal Reserve federal funds rate</body></html>"
    with pytest.raises(sep.SepIngestionError, match="no recognized policy-rate table"):
        sep.parse_release(
            content,
            "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        )


def test_ddl_is_append_only_normalized_and_pointer_guarded() -> None:
    ddl = Path("schemas/fomc_sep_ingestion.sql").read_text(encoding="utf-8")
    for token in (
        "CREATE TABLE IF NOT EXISTS fomc_sep_releases",
        "CREATE TABLE IF NOT EXISTS fomc_sep_rate_distributions",
        "source_sha256",
        "policy_source_sha256",
        "policy_rate_midpoint_pct",
        "parser_version",
        "observed_at",
        "fetched_at",
        "fomc_sep_immutable_guard",
        "current SEP release requires a complete normalized distribution",
        "CREATE OR REPLACE VIEW fomc_sep_current_release",
        "CREATE OR REPLACE VIEW fomc_sep_current_distribution",
    ):
        assert token in ddl


def test_lock_and_dispatcher_registration_are_present() -> None:
    assert db.LOCK_FOMC_SEP_INGESTION == 900_359
    ids = [value for name, value in vars(db).items() if name.startswith("LOCK_")]
    assert ids.count(900_359) == 1
    dispatcher = Path("src/run_worker.py").read_text(encoding="utf-8")
    assert "|fomc_sep_ingestion)" in dispatcher


def test_worker_has_no_sec_api_runtime_dependency() -> None:
    source = Path("src/workers/fomc_sep_ingestion.py").read_text(encoding="utf-8")
    assert "SEC_API_IO_KEY" not in source
    assert "sec-api.io" not in source
    assert "httpx.Client" in source
    assert "ON CONFLICT (release_date, source_sha256, policy_source_sha256)" in source
    assert "ON CONFLICT (release_id, projection_horizon, rate_bin_low, rate_bin_high)" in source


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self.conn = conn

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        self.conn.sql.append(sql)

    def fetchall(self) -> list[tuple[dt.date, str, str]]:
        return list(self.conn.known)


class _FakeConnection:
    def __init__(
        self, known: set[tuple[dt.date, str, str]] | None = None
    ) -> None:
        self.sql: list[str] = []
        self.known = known or set()
        self.pointer = "prior-release"
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1
        self.pointer = "prior-release"


def test_bounded_runs_advance_unseen_then_poll_latest_after_catch_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    urls = [
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120125.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120620.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm",
    ]
    release_content = _fixture("quarter_point.html")
    policy_content = (
        b"<html><body>The Committee decided to keep the target range "
        b"for the federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    conn = _FakeConnection(
        {
            (
                dt.date(2012, 9, 13),
                sep.source_sha256(release_content),
                sep.source_sha256(policy_content),
            )
        }
    )
    latest_content = [release_content]
    fetched_release_urls: list[str] = []
    published_dates: list[list[dt.date]] = []

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    def fake_get(_client: object, url: str) -> bytes:
        if "/pressreleases/" in url:
            return policy_content
        fetched_release_urls.append(url)
        if url == urls[-1]:
            return latest_content[0]
        return release_content

    def publish(
        fake_conn: _FakeConnection, artifacts: list[sep.ReleaseArtifact]
    ) -> tuple[int, int]:
        published_dates.append([artifact.release_date for artifact in artifacts])
        for artifact in artifacts:
            assert artifact.policy_source_sha256 is not None
            fake_conn.known.add(
                (
                    artifact.release_date,
                    artifact.source_sha256,
                    artifact.policy_source_sha256,
                )
            )
        return len(artifacts), sum(len(item.distributions) for item in artifacts)

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [])
    monkeypatch.setattr(sep, "_historical_release_urls", lambda _as_of: urls)
    monkeypatch.setattr(sep, "_get_official_html", fake_get)
    monkeypatch.setattr(sep, "_publish_artifacts", publish)

    first = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[1]]
    assert first["fetched"] == 1
    assert first["releases"] == 1
    assert first["unchanged"] == 0
    assert published_dates == [[dt.date(2012, 6, 20)]]

    fetched_release_urls.clear()
    second = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[0]]
    assert second["fetched"] == 1
    assert second["releases"] == 1
    assert second["unchanged"] == 0
    assert published_dates[-1] == [dt.date(2012, 1, 25)]

    fetched_release_urls.clear()
    latest_content[0] += b"\n"
    third = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[2]]
    assert third["fetched"] == 1
    assert third["releases"] == 1
    assert third["unchanged"] == 0
    assert published_dates[-1] == [dt.date(2012, 9, 13)]


def test_partial_publication_rolls_back_and_preserves_prior_pointer(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConnection()

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    release_url = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm"
    index = f'<a href="{release_url}">Projection Materials HTML</a>'.encode()

    def fake_get(_client: object, url: str) -> bytes:
        if "historical" in url:
            return index
        if "/pressreleases/" in url:
            return (
                b"<html><body>The Committee decided to keep the target range "
                b"for the federal funds rate at 0 to 1/4 percent.</body></html>"
            )
        return _fixture("quarter_point.html")

    def fail_mid_publish(fake_conn: _FakeConnection, artifacts: list[sep.ReleaseArtifact]):
        assert len(artifacts) == 1
        fake_conn.pointer = "partial-release"
        raise RuntimeError("child insert failed")

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [
        "https://www.federalreserve.gov/monetarypolicy/fomchistorical2012.htm"
    ])
    monkeypatch.setattr(sep, "_historical_release_urls", lambda _as_of: [])
    monkeypatch.setattr(sep, "_get_official_html", fake_get)
    monkeypatch.setattr(sep, "_publish_artifacts", fail_mid_publish)

    with pytest.raises(RuntimeError, match="child insert failed"):
        sep.run("postgresql://unused", calc_date="2012-12-31")
    assert conn.pointer == "prior-release"
    assert conn.rollbacks == 1
    assert conn.commits == 0


def test_lock_busy_performs_no_schema_or_network_work(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConnection()

    @contextlib.contextmanager
    def busy(_conn: object, _lock: int):
        yield False

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", busy)
    result = sep.run("postgresql://unused", calc_date="2023-12-31")
    assert result == {"status": "lock_busy", "releases": 0, "distributions": 0}
    assert conn.sql == []
