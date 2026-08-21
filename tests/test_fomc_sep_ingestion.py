"""Focused contract tests for the official Federal Reserve SEP ingestion lane."""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import uuid
from decimal import Decimal
from pathlib import Path

import pytest
import httpx

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


def test_policy_statement_url_uses_the_official_route_for_each_era() -> None:
    assert sep.policy_statement_url(dt.date(2012, 1, 25)) == (
        "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"
    )
    assert sep.policy_statement_url(dt.date(2017, 3, 15)) == (
        "https://www.federalreserve.gov/newsevents/press/monetary/20170315a.htm"
    )
    assert sep.policy_statement_url(dt.date(2017, 6, 14)) == (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20170614a.htm"
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm",
    ],
)
def test_policy_statement_parser_accepts_both_official_route_shapes(url: str) -> None:
    content = (
        b"<html><body>The Committee decided to keep the target range for the "
        b"federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    assert sep.parse_policy_rate(content, url) == (
        Decimal("0.000"),
        Decimal("0.250"),
        Decimal("0.125"),
    )


@pytest.mark.parametrize(
    "url",
    [
        "http://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm",
        "https://www.federalreserve.gov.evil.test/newsevents/press/monetary/20120125a.htm",
        "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm?download=1",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a.htm#article",
        "https://www.federalreserve.gov/newsevents/press/monetary20120125a.htm",
        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20250618a1.htm",
    ],
)
def test_policy_statement_allowlist_fails_closed(url: str) -> None:
    with pytest.raises(sep.SepIngestionError, match="not a canonical"):
        sep.parse_policy_rate(b"<html></html>", url)


def test_policy_fetch_follows_only_the_same_date_legacy_migration_redirect() -> None:
    legacy = "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"
    current = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20120125a.htm"
    )
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if str(request.url) == legacy:
            return httpx.Response(
                302,
                headers={"location": current.replace("https://", "http://")},
            )
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"<html>statement</html>",
        )

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        assert sep._get_official_html(client, legacy) == b"<html>statement</html>"
    assert requested == [legacy, current]


def test_policy_fetch_redirect_gets_a_fresh_target_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"
    current = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20120125a.htm"
    )
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if str(request.url) == legacy and requested.count(legacy) < 3:
            return httpx.Response(503)
        if str(request.url) == legacy:
            return httpx.Response(302, headers={"location": current})
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"<html>statement</html>",
        )

    monkeypatch.setattr(sep.time, "sleep", lambda _seconds: None)
    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        assert sep._get_official_html(client, legacy) == b"<html>statement</html>"
    assert requested == [legacy, legacy, legacy, current]


def test_policy_fetch_retries_target_without_following_another_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"
    current = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20120125a.htm"
    )
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if str(request.url) == legacy:
            return httpx.Response(302, headers={"location": current})
        if requested.count(current) < 3:
            return httpx.Response(503)
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"<html>statement</html>",
        )

    monkeypatch.setattr(sep.time, "sleep", lambda _seconds: None)
    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        assert sep._get_official_html(client, legacy) == b"<html>statement</html>"
    assert requested == [legacy, current, current, current]


def test_policy_fetch_rejects_a_redirect_to_another_release_date() -> None:
    legacy = "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            302,
            headers={
                "location": (
                    "https://www.federalreserve.gov/newsevents/pressreleases/"
                    "monetary20120126a.htm"
                )
            },
        )

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        with pytest.raises(sep.SepIngestionError, match="refusing non-canonical"):
            sep._get_official_html(client, legacy)


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
    assert "press/monetary/[0-9]{8}a[.]htm" in ddl
    assert "pressreleases/monetary[0-9]{8}a[.]htm" in ddl
    assert "CONSTRAINT fomc_sep_releases_observation_key UNIQUE" in ddl
    assert "legacy_constraint" in ddl
    assert "DROP CONSTRAINT %I" in ddl


def test_pointer_guard_does_not_declare_reserved_current_date() -> None:
    ddl = Path("schemas/fomc_sep_ingestion.sql").read_text(encoding="utf-8")
    guard = ddl.split("fomc_sep_current_pointer_guard()", 1)[1].split("$$;", 1)[0]

    assert "current_date date;" not in guard
    assert "prior_release_date date;" in guard


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
    assert (
        "ON CONFLICT (release_date, source_sha256, policy_source_sha256, parser_version)"
        in source
    )
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
        if sql.startswith("SELECT release_id"):
            self.conn.events.append("known")
        elif "CREATE TABLE IF NOT EXISTS fomc_sep_releases" in sql:
            self.conn.events.append("ddl")
        if self.conn.fail_execute:
            raise RuntimeError("schema install failed")
        if "INSERT INTO fomc_sep_current_pointer" in sql:
            assert isinstance(params, tuple)
            release_id = params[0]
            release_date = self.conn.release_dates[release_id]
            if release_date >= self.conn.release_dates[self.conn.pointer]:
                self.conn.pointer = release_id

    def fetchall(self) -> list[tuple[uuid.UUID, dt.date, str, str, str]]:
        return [
            (release_id, release_date, source_hash, policy_hash, parser_version)
            for (
                release_date,
                source_hash,
                policy_hash,
                parser_version,
            ), release_id
            in self.conn.known.items()
        ]


class _FakeConnection:
    def __init__(
        self,
        known: dict[tuple[dt.date, str, str, str], uuid.UUID] | None = None,
        *,
        pointer: uuid.UUID | None = None,
        fail_execute: bool = False,
    ) -> None:
        self.sql: list[str] = []
        self.known = known or {}
        self.pointer = pointer or uuid.uuid4()
        self.prior_pointer = self.pointer
        self.release_dates = {
            release_id: key[0] for key, release_id in self.known.items()
        }
        self.release_dates.setdefault(self.pointer, dt.date.min)
        self.commits = 0
        self.rollbacks = 0
        self.fail_execute = fail_execute
        self.events: list[str] = []

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.events.append("commit")

    def rollback(self) -> None:
        self.rollbacks += 1
        self.events.append("rollback")
        self.pointer = self.prior_pointer


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
    known_release_id = uuid.uuid4()
    conn = _FakeConnection(
        {
            (
                dt.date(2012, 9, 13),
                sep.source_sha256(release_content),
                sep.source_sha256(policy_content),
                sep.PARSER_VERSION,
            ): known_release_id
        }
    )
    latest_content = [release_content]
    fetched_release_urls: list[str] = []
    fetched_policy_urls: list[str] = []
    published_dates: list[list[dt.date]] = []
    published_ids: list[list[object]] = []

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    def fake_get(_client: object, url: str) -> bytes:
        if "/newsevents/" in url:
            fetched_policy_urls.append(url)
            return policy_content
        fetched_release_urls.append(url)
        if url == urls[-1]:
            return latest_content[0]
        return release_content

    def publish(
        fake_conn: _FakeConnection, artifacts: list[sep.ReleaseArtifact]
    ) -> tuple[int, int]:
        published_dates.append([artifact.release_date for artifact in artifacts])
        published_ids.append([artifact.release_id for artifact in artifacts])
        for artifact in artifacts:
            assert artifact.policy_source_sha256 is not None
            fake_conn.known[
                (
                    artifact.release_date,
                    artifact.source_sha256,
                    artifact.policy_source_sha256,
                    artifact.parser_version,
                )
            ] = artifact.release_id
            fake_conn.release_dates[artifact.release_id] = artifact.release_date
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
    assert fetched_policy_urls == [
        "https://www.federalreserve.gov/newsevents/press/monetary/20120620a.htm"
    ]

    fetched_release_urls.clear()
    fetched_policy_urls.clear()
    second = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[0]]
    assert second["fetched"] == 1
    assert second["releases"] == 1
    assert second["unchanged"] == 0
    assert published_dates[-1] == [dt.date(2012, 1, 25)]
    assert fetched_policy_urls == [
        "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"
    ]

    fetched_release_urls.clear()
    fetched_policy_urls.clear()
    latest_content[0] += b"\n"
    third = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[2]]
    assert third["fetched"] == 1
    assert third["releases"] == 1
    assert third["unchanged"] == 0
    assert published_dates[-1] == [dt.date(2012, 9, 13)]
    assert fetched_policy_urls == [
        "https://www.federalreserve.gov/newsevents/press/monetary/20120913a.htm"
    ]

    parser_v1_release_id = published_ids[-1][0]
    fetched_release_urls.clear()
    fetched_policy_urls.clear()
    same_parser = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[2]]
    assert same_parser["unchanged"] == 1
    assert same_parser["releases"] == 0
    assert len(published_ids[-1]) == 0

    monkeypatch.setattr(sep, "PARSER_VERSION", "fomc_sep_html_v2")
    fetched_release_urls.clear()
    fetched_policy_urls.clear()
    new_parser = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[2]]
    assert new_parser["unchanged"] == 0
    assert new_parser["releases"] == 1
    assert published_ids[-1][0] != parser_v1_release_id
    latest_identity = (
        dt.date(2012, 9, 13),
        sep.source_sha256(latest_content[0]),
        sep.source_sha256(policy_content),
    )
    assert {row[3] for row in conn.known if row[:3] == latest_identity} == {
        "fomc_sep_html_v1",
        "fomc_sep_html_v2",
    }


def test_bounded_polling_rotates_without_starvation_and_is_replay_deterministic() -> None:
    urls = [
        f"https://www.federalreserve.gov/monetarypolicy/fomcprojtabl2012{month:02d}01.htm"
        for month in range(1, 6)
    ]
    known_dates = {sep._release_date(url) for url in urls}
    selected_by_day = [
        sep._bounded_release_urls(
            urls,
            known_dates,
            dt.date(2012, 1, 1) + dt.timedelta(days=day),
            2,
        )
        for day in range(len(urls))
    ]

    assert all(len(selected) == 2 for selected in selected_by_day)
    assert set().union(*(set(selected) for selected in selected_by_day)) == set(urls)
    assert urls[-1] in selected_by_day[3]
    assert selected_by_day[2] == sep._bounded_release_urls(
        urls, known_dates, dt.date(2012, 1, 3), 2
    )


def test_bounded_polling_spends_the_strict_cap_on_unseen_dates_first() -> None:
    urls = [
        f"https://www.federalreserve.gov/monetarypolicy/fomcprojtabl2012{month:02d}01.htm"
        for month in range(1, 6)
    ]
    known_dates = {sep._release_date(url) for url in urls[:2]}

    selected = sep._bounded_release_urls(
        urls, known_dates, dt.date(2012, 2, 1), 2
    )

    assert selected == urls[-2:]
    assert len(selected) == 2


def test_reverted_latest_release_repoints_to_existing_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_url = (
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm"
    )
    policy_content = (
        b"<html><body>The Committee decided to keep the target range "
        b"for the federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    release_a = _fixture("quarter_point.html")
    release_b = release_a + b"\n"
    release_a_id = uuid.uuid4()
    release_b_id = uuid.uuid4()
    release_date = dt.date(2012, 9, 13)
    policy_hash = sep.source_sha256(policy_content)
    conn = _FakeConnection(
        {
            (
                release_date,
                sep.source_sha256(release_a),
                policy_hash,
                sep.PARSER_VERSION,
            ): release_a_id,
            (
                release_date,
                sep.source_sha256(release_b),
                policy_hash,
                sep.PARSER_VERSION,
            ): release_b_id,
        },
        pointer=release_b_id,
    )

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    def fake_get(_client: object, url: str) -> bytes:
        return policy_content if "/newsevents/" in url else release_a

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [])
    monkeypatch.setattr(sep, "_historical_release_urls", lambda _as_of: [release_url])
    monkeypatch.setattr(sep, "_get_official_html", fake_get)

    result = sep.run("postgresql://unused", calc_date="2012-12-31")

    assert result["unchanged"] == 1
    assert result["releases"] == 0
    assert conn.pointer == release_a_id
    assert len(conn.known) == 2


def test_matching_older_polled_release_does_not_regress_pointer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    older_url = (
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120620.htm"
    )
    policy_content = (
        b"<html><body>The Committee decided to keep the target range "
        b"for the federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    release_content = _fixture("quarter_point.html")
    older_id = uuid.uuid4()
    current_id = uuid.uuid4()
    policy_hash = sep.source_sha256(policy_content)
    conn = _FakeConnection(
        {
            (
                dt.date(2012, 6, 20),
                sep.source_sha256(release_content),
                policy_hash,
                sep.PARSER_VERSION,
            ): older_id,
            (
                dt.date(2012, 9, 13),
                sep.source_sha256(release_content + b"\n"),
                policy_hash,
                sep.PARSER_VERSION,
            ): current_id,
        },
        pointer=current_id,
    )

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    def fake_get(_client: object, url: str) -> bytes:
        return policy_content if "/newsevents/" in url else release_content

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [])
    monkeypatch.setattr(sep, "_historical_release_urls", lambda _as_of: [older_url])
    monkeypatch.setattr(sep, "_get_official_html", fake_get)

    result = sep.run("postgresql://unused", calc_date="2012-12-31")

    assert result["unchanged"] == 1
    assert result["releases"] == 0
    assert conn.pointer == current_id
    assert len(conn.known) == 2


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
        if "/newsevents/" in url:
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
    assert conn.pointer == conn.prior_pointer
    assert conn.rollbacks == 1
    assert conn.commits == 2


def test_schema_failure_rolls_back_before_network(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConnection(fail_execute=True)

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(
        sep,
        "_get_official_html",
        lambda *_args: pytest.fail("network I/O must not run after DDL failure"),
    )

    with pytest.raises(RuntimeError, match="schema install failed"):
        sep.run("postgresql://unused", calc_date="2012-12-31")
    assert conn.events == ["ddl", "rollback"]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_ddl_is_committed_before_network_while_session_lock_is_held(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _FakeConnection()
    lock_held = False
    publication_called = False

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        nonlocal lock_held
        lock_held = True
        try:
            yield True
        finally:
            lock_held = False

    def fail_network(_client: object, _url: str) -> bytes:
        assert lock_held
        assert conn.events == ["ddl", "commit", "known", "commit"]
        conn.events.append("network")
        raise RuntimeError("network failed")

    def publish(_conn: object, _artifacts: object) -> tuple[int, int]:
        nonlocal publication_called
        publication_called = True
        return 0, 0

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [sep.CALENDAR_URL])
    monkeypatch.setattr(sep, "_get_official_html", fail_network)
    monkeypatch.setattr(sep, "_publish_artifacts", publish)

    with pytest.raises(RuntimeError, match="network failed"):
        sep.run("postgresql://unused", calc_date="2012-12-31")
    assert conn.events == ["ddl", "commit", "known", "commit", "network"]
    assert not lock_held
    assert not publication_called
    assert conn.pointer == conn.prior_pointer


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
