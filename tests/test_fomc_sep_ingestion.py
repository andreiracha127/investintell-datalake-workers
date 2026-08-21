"""Focused contract tests for the official Federal Reserve SEP ingestion lane."""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import os
import uuid
from collections.abc import Iterator
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest
import httpx

from src import db
from src.workers import fomc_sep_ingestion as sep

FIXTURES = Path("tests/fixtures/fomc_sep")


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _with_resolved_url(fetch):
    def wrapped(client, url, *, return_url=False):
        content = fetch(client, url)
        return (content, url) if return_url else content

    return wrapped


def _by_horizon(artifact: sep.ReleaseArtifact) -> dict[str, int]:
    totals: dict[str, int] = {}
    for row in artifact.distributions:
        totals[row.projection_horizon] = totals.get(row.projection_horizon, 0) + row.participant_count
    return totals


def _distribution_count(
    artifact: sep.ReleaseArtifact, horizon: str, low: str, high: str
) -> int | None:
    for row in artifact.distributions:
        if (
            row.projection_horizon == horizon
            and row.rate_bin_low == Decimal(low)
            and row.rate_bin_high == Decimal(high)
        ):
            return row.participant_count
    return None

class _TrackingStream(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]) -> None:
        self.chunks = chunks
        self.read_count = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk


def test_official_two_row_quarter_point_header_is_normalized() -> None:
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
    assert _distribution_count(artifact, "2023", "5.13", "5.37") == 1  # September: 4
    assert _distribution_count(artifact, "2024", "4.38", "4.62") == 4  # September: 2
    assert _distribution_count(artifact, "longer_run", "2.38", "2.62") == 11  # September: 12


def test_december_2012_compilation_range_bins_are_normalized() -> None:
    artifact = sep.parse_release(
        _fixture("december_2012_range_bins.html"),
        "https://www.federalreserve.gov/monetarypolicy/files/"
        "FOMC20121212SEPcompilation.htm",
    )
    assert artifact.source_format == "range_bins"
    assert artifact.release_date == dt.date(2012, 12, 12)
    assert _by_horizon(artifact) == {
        "2012": 19,
        "2013": 19,
        "2014": 19,
        "2015": 19,
        "longer_run": 19,
    }
    assert all(row.bin_kind == "range" for row in artifact.distributions)
    assert _distribution_count(artifact, "2012", "0", "0.37") == 19  # September: 18
    assert _distribution_count(artifact, "2013", "0.38", "0.62") == 1  # September: 2
    assert _distribution_count(artifact, "2014", "0.38", "0.62") == 1  # September: 0
    assert _distribution_count(artifact, "2015", "0.38", "0.62") == 5  # September: 2
    assert _distribution_count(artifact, "longer_run", "3.63", "3.87") == 3  # September: 2


@pytest.mark.parametrize("dash", ["-", "\u2010", "\u2011", "\u2013", "\u2014", "\u2212"])
def test_standalone_dash_count_placeholders_are_zero(dash: str) -> None:
    assert sep._parse_count(f" {dash} ") == 0
    assert sep._horizon(f"Longer{dash}Run") == "longer_run"


@pytest.mark.parametrize("value", ["1-2", "1\u20142", "word", "--", "1.5"])
def test_malformed_participant_counts_are_rejected(value: str) -> None:
    content = _fixture("range_bins.html").replace(
        b"<td>19</td>", f"<td>{value}</td>".encode(), 1
    )
    with pytest.raises(sep.SepIngestionError, match="invalid SEP participant count"):
        sep.parse_release(
            content,
            "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        )


@pytest.mark.parametrize("extra_cell", [False, True], ids=["short", "long"])
def test_recognized_distribution_rows_must_match_header_width(extra_cell: bool) -> None:
    original = (
        b"<tr><td>5.38 - 5.62</td><td>7</td><td>19</td><td></td>"
        b"<td>2</td><td></td><td>1</td></tr>"
    )
    replacement = (
        original.replace(b"<td></td><td>1</td></tr>", b"<td></td></tr>")
        if not extra_cell
        else original.replace(b"</tr>", b"<td></td></tr>")
    )
    content = _fixture("range_bins.html").replace(original, replacement, 1)
    with pytest.raises(
        sep.SepIngestionError,
        match=r"distribution row has \d+ cells; expected 6",
    ):
        sep.parse_release(
            content,
            "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        )


@pytest.mark.parametrize("invalid_range", ["2.38 - 2.38", "2.62 - 2.38"])
def test_invalid_range_bins_are_rejected(invalid_range: str) -> None:
    content = _fixture("range_bins.html").replace(
        b"2.38 - 2.62", invalid_range.encode(), 1
    )
    with pytest.raises(sep.SepIngestionError, match="invalid SEP range bin"):
        sep.parse_release(
            content,
            "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        )


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
        (
            "The Committee reaffirmed its expectation that the current target "
            "range for the federal funds rate of 0 to 1/4 percent will be appropriate.",
            (Decimal("0.000"), Decimal("0.250"), Decimal("0.125")),
        ),
        (
            "The Committee reaffirmed its view that the current 0 to 1/4 percent "
            "target range for the federal funds rate remains appropriate.",
            (Decimal("0.000"), Decimal("0.250"), Decimal("0.125")),
        ),
        (
            "The Committee decided to raise the target range for the federal funds "
            "rate to 1-1/4 to 1‑1/2 percent.",
            (Decimal("1.250"), Decimal("1.500"), Decimal("1.375")),
        ),
        (
            "The Committee decided to lower the target range for the federal funds "
            "rate by 1/2 percentage point to 4-3/4 to 5 percent.",
            (Decimal("4.750"), Decimal("5.000"), Decimal("4.875")),
        ),
        (
            "The Committee maintained the target range for the federal funds rate "
            "at 0 to 1⁄4 percent.",
            (Decimal("0.000"), Decimal("0.250"), Decimal("0.125")),
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


def test_parse_release_rejects_policy_source_from_another_release_date() -> None:
    policy = (
        b"<html><body>The Committee decided to keep the target range for the "
        b"federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    with pytest.raises(sep.SepIngestionError, match="does not match SEP release date"):
        sep.parse_release(
            _fixture("quarter_point.html"),
            "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm",
            policy_content=policy,
            policy_url=(
                "https://www.federalreserve.gov/newsevents/press/monetary/"
                "20120914a.htm"
            ),
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


def test_policy_fetch_can_return_the_resolved_canonical_url() -> None:
    legacy = "https://www.federalreserve.gov/newsevents/press/monetary/20120125a.htm"
    current = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20120125a.htm"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if str(request.url) == legacy:
            return httpx.Response(302, headers={"location": current})
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=b"<html>statement</html>",
        )

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        assert sep._get_official_html(client, legacy, return_url=True) == (
            b"<html>statement</html>",
            current,
        )


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
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtable20220316.htm",
        sep.CALENDAR_URL,
    ],
)
def test_official_html_fetches_release_exceptions_and_calendar(url: str) -> None:
    content = b"<html>official</html>"

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == url
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=content,
        )

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        assert sep._get_official_html(client, url) == content


@pytest.mark.parametrize(
    ("url", "error"),
    [
        (
            "https://www.federalreserve.gov/monetarypolicy/"
            "fomcprojtable20220316.htm",
            "SEP release redirect",
        ),
        (sep.CALENDAR_URL, "calendar redirect"),
    ],
)
def test_release_and_calendar_redirects_never_use_policy_validation(
    url: str, error: str
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": url})

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        with pytest.raises(sep.SepIngestionError, match=error):
            sep._get_official_html(client, url)


def test_non_html_is_rejected_before_streaming_the_body() -> None:
    url = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm"
    stream = _TrackingStream([b"not html"])

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "application/pdf"},
            stream=stream,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(sep.SepIngestionError, match="not HTML"):
            sep._get_official_html(client, url)
    assert stream.read_count == 0


def test_declared_oversized_html_is_rejected_before_streaming_the_body() -> None:
    url = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm"
    stream = _TrackingStream([b"<html></html>"])

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={
                "content-type": "text/html",
                "content-length": str(sep.MAX_HTML_BYTES + 1),
            },
            stream=stream,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(sep.SepIngestionError, match="too large"):
            sep._get_official_html(client, url)
    assert stream.read_count == 0


def test_negative_content_length_is_rejected_before_streaming_the_body() -> None:
    url = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm"
    stream = _TrackingStream([b"<html></html>"])

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html", "content-length": "-1"},
            stream=stream,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(sep.SepIngestionError, match="size is invalid"):
            sep._get_official_html(client, url)
    assert stream.read_count == 0


def test_html_stream_has_a_total_read_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm"
    stream = _TrackingStream([b"<html>", b"must not be read"])
    timestamps = iter([0.0, 1.0])
    monkeypatch.setattr(sep, "HTTP_TOTAL_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(sep.time, "monotonic", lambda: next(timestamps))

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            stream=stream,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(sep.SepIngestionError, match="exceeded total timeout"):
            sep._get_official_html(client, url)
    assert stream.read_count == 1


def test_html_stream_is_capped_while_reading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm"
    stream = _TrackingStream([b"123", b"456", b"must not be read"])
    monkeypatch.setattr(sep, "MAX_HTML_BYTES", 5)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            stream=stream,
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(sep.SepIngestionError, match="too large"):
            sep._get_official_html(client, url)
    assert stream.read_count == 2

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


def test_december_2012_compilation_url_is_canonical_and_date_bearing() -> None:
    url = (
        "https://www.federalreserve.gov/monetarypolicy/files/"
        "FOMC20121212SEPcompilation.htm"
    )
    assert sep.canonical_release_url(url) == url
    assert sep._release_date(url) == dt.date(2012, 12, 12)


def test_march_2022_exact_projection_route_is_canonical_and_date_bearing() -> None:
    url = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtable20220316.htm"
    )
    assert sep.canonical_release_url(url) == url
    assert sep._release_date(url) == dt.date(2022, 3, 16)


@pytest.mark.parametrize(
    "url",
    [
        "https://www.federalreserve.gov/monetarypolicy/files/FOMC20121211SEPcompilation.htm",
        "https://www.federalreserve.gov/monetarypolicy/files/fomc20121212SEPcompilation.htm",
        "https://www.federalreserve.gov/monetarypolicy/FOMC20121212SEPcompilation.htm",
        "https://www.federalreserve.gov/monetarypolicy/files/FOMC20121212SEPcompilation.html",
        "https://www.federalreserve.gov/monetarypolicy/files/FOMC20121212SEPcompilation.htm?download=1",
    ],
)
def test_december_2012_compilation_route_rejects_malformed_variants(url: str) -> None:
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


def test_march_2022_table_url_is_canonical_and_date_bearing() -> None:
    url = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtable20220316.htm"
    )
    assert sep.canonical_release_url(url) == url
    assert sep._release_date(url) == dt.date(2022, 3, 16)


@pytest.mark.parametrize(
    "url",
    [
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtable20220315.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtable20220317.htm",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtable20220316.html",
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtable20220316.htm?x=1",
        "https://www.federalreserve.gov/monetarypolicy/files/fomcprojtable20220316.htm",
    ],
)
def test_march_2022_table_route_rejects_malformed_variants(url: str) -> None:
    with pytest.raises(sep.SepIngestionError, match="refusing"):
        sep.canonical_release_url(url)


def test_calendar_discovery_accepts_only_the_exact_march_2022_table_route() -> None:
    exact = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtable20220316.htm"
    )
    index = f"""
        <html><body>
        <a href="{exact}">March projection materials</a>
        <a href="/monetarypolicy/fomcprojtable20220315.htm">Wrong date</a>
        <a href="/monetarypolicy/fomcprojtable20220317.htm">Wrong date</a>
        <a href="/monetarypolicy/fomcprojtable20220316.html">Wrong suffix</a>
        </body></html>
    """.encode()

    assert sep.discover_release_urls(
        [(sep.CALENDAR_URL, index)], dt.date(2022, 3, 16)
    ) == [exact]

def test_historical_backfill_urls_are_official_and_start_in_2012() -> None:
    urls = sep._historical_release_urls(dt.date(2020, 12, 31))
    assert urls[0].endswith("fomcprojtabl20120125.htm")
    assert urls[-1].endswith("fomcprojtabl20201216.htm")
    assert len(urls) == 36
    assert all(sep.canonical_release_url(url) == url for url in urls)


@pytest.mark.parametrize(
    ("as_of", "included"),
    [
        (dt.date(2012, 12, 11), False),
        (dt.date(2012, 12, 12), True),
        (dt.date(2012, 12, 13), True),
    ],
)
def test_historical_backfill_includes_december_2012_sep_from_release_date(
    as_of: dt.date,
    included: bool,
) -> None:
    url = (
        "https://www.federalreserve.gov/monetarypolicy/files/"
        "FOMC20121212SEPcompilation.htm"
    )
    assert (url in sep._historical_release_urls(as_of)) is included


def test_parser_fails_closed_without_policy_distribution() -> None:
    content = b"<html><body>Board of Governors Federal Reserve federal funds rate</body></html>"
    with pytest.raises(sep.SepIngestionError, match="no recognized policy-rate table"):
        sep.parse_release(
            content,
            "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20231213.htm",
        )


def test_release_identity_includes_both_canonical_provenance_routes() -> None:
    release_date = dt.date(2012, 12, 12)
    generic_url = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtabl20121212.htm"
    )
    compilation_url = (
        "https://www.federalreserve.gov/monetarypolicy/files/"
        "FOMC20121212SEPcompilation.htm"
    )
    legacy_policy_url = (
        "https://www.federalreserve.gov/newsevents/press/monetary/"
        "20121212a.htm"
    )
    current_policy_url = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20121212a.htm"
    )
    release_content = _fixture("december_2012_range_bins.html")
    policy_content = (
        b"<html><body>The Committee decided to keep the target range for the "
        b"federal funds rate at 0 to 1/4 percent.</body></html>"
    )

    generic = sep.parse_release(
        release_content,
        generic_url,
        policy_content=policy_content,
        policy_url=legacy_policy_url,
    )
    corrected_release_route = sep.parse_release(
        release_content,
        compilation_url,
        policy_content=policy_content,
        policy_url=legacy_policy_url,
    )
    corrected_policy_route = sep.parse_release(
        release_content,
        compilation_url,
        policy_content=policy_content,
        policy_url=current_policy_url,
    )

    assert generic.release_date == corrected_release_route.release_date == release_date
    assert generic.source_sha256 == corrected_release_route.source_sha256
    assert (
        generic.policy_source_sha256
        == corrected_release_route.policy_source_sha256
        == corrected_policy_route.policy_source_sha256
    )
    assert len(
        {
            generic.release_id,
            corrected_release_route.release_id,
            corrected_policy_route.release_id,
        }
    ) == 3


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
        "FOMC20121212SEPcompilation.htm",
        "fomcprojtable20220316.htm",
        "fomc_sep_releases_source_url_release_date_v2_check",
        "fomc_sep_releases_policy_source_url_release_date_v2_check",
        "horizon_total BETWEEN 1 AND 25",
        "horizon_count < 4",
    ):
        assert token in ddl
    assert (
        "CONSTRAINT fomc_sep_releases_provenance_observation_key UNIQUE" in ddl
    )
    assert (
        "DROP CONSTRAINT IF EXISTS fomc_sep_releases_observation_key" in ddl
    )


def test_ddl_migrates_legacy_route_checks_only_after_v2_validation() -> None:
    ddl = Path("schemas/fomc_sep_ingestion.sql").read_text(encoding="utf-8")
    legacy_checks = {
        "fomc_sep_releases_source_url_release_date_v2_check": (
            "fomc_sep_releases_source_url_official_routes_check",
            "fomc_sep_releases_source_url_check",
        ),
        "fomc_sep_releases_policy_source_url_release_date_v2_check": (
            "fomc_sep_releases_policy_source_url_official_routes_check",
            "fomc_sep_releases_policy_source_url_check",
        ),
    }

    for v2_constraint, old_constraints in legacy_checks.items():
        assert f"ADD CONSTRAINT {v2_constraint}" in ddl
        validation = ddl.index(f"VALIDATE CONSTRAINT {v2_constraint}")
        for old_constraint in old_constraints:
            drop = f"DROP CONSTRAINT IF EXISTS {old_constraint}"
            assert drop in ddl
            assert validation < ddl.index(drop, validation)


def test_ddl_preserves_the_parser_identity_migration() -> None:
    ddl = Path("schemas/fomc_sep_ingestion.sql").read_text(encoding="utf-8")
    new_key = "fomc_sep_releases_provenance_observation_key"
    legacy_drop = "DROP CONSTRAINT IF EXISTS fomc_sep_releases_observation_key"

    assert f"CONSTRAINT {new_key} UNIQUE" in ddl
    assert f"ADD CONSTRAINT {new_key} UNIQUE" in ddl
    assert legacy_drop in ddl
    assert ddl.index(f"ADD CONSTRAINT {new_key}") < ddl.index(legacy_drop)
    assert "legacy_three_column_constraint text;" in ddl
    assert "DROP CONSTRAINT %I" in ddl
    assert ddl.index(f"ADD CONSTRAINT {new_key}") < ddl.index(
        "DROP CONSTRAINT %I"
    )


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
        "release_date, source_url, source_sha256,"
        "\n                    policy_source_url, policy_source_sha256, parser_version"
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

    def fetchall(
        self,
    ) -> list[tuple[uuid.UUID, dt.date, str, str, str, str, str]]:
        return [
            (
                release_id,
                release_date,
                source_url,
                source_hash,
                policy_url,
                policy_hash,
                parser_version,
            )
            for (
                release_date,
                source_url,
                source_hash,
                policy_url,
                policy_hash,
                parser_version,
            ), release_id
            in self.conn.known.items()
        ]


class _FakeConnection:
    def __init__(
        self,
        known: dict[tuple[dt.date, str, str, str, str, str], uuid.UUID] | None = None,
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


@pytest.mark.parametrize("limit", [0, -1])
def test_invalid_limit_fails_before_connection_or_network(
    monkeypatch: pytest.MonkeyPatch, limit: int
) -> None:
    monkeypatch.setattr(
        sep,
        "connect",
        lambda *_args: pytest.fail("database connection must not open"),
    )
    monkeypatch.setattr(
        sep,
        "_get_official_html",
        lambda *_args: pytest.fail("network I/O must not run"),
    )

    with pytest.raises(sep.SepIngestionError, match="limit must be at least 1"):
        sep.run("postgresql://unused", limit=limit)


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
                urls[-1],
                sep.source_sha256(release_content),
                sep.policy_statement_url(dt.date(2012, 9, 13)),
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
            assert artifact.policy_source_url is not None
            assert artifact.policy_source_sha256 is not None
            fake_conn.known[
                (
                    artifact.release_date,
                    artifact.source_url,
                    artifact.source_sha256,
                    artifact.policy_source_url,
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
    monkeypatch.setattr(sep, "_get_official_html", _with_resolved_url(fake_get))
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
    latest_identity = (
        dt.date(2012, 9, 13),
        urls[-1],
        sep.source_sha256(latest_content[0]),
        sep.policy_statement_url(dt.date(2012, 9, 13)),
        sep.source_sha256(policy_content),
    )
    legacy_v1_release_id = uuid.uuid4()
    conn.known[(*latest_identity, "fomc_sep_html_v1")] = legacy_v1_release_id
    conn.release_dates[legacy_v1_release_id] = latest_identity[0]

    third = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[2]]
    assert third["fetched"] == 1
    assert third["releases"] == 1
    assert third["unchanged"] == 0
    assert published_dates[-1] == [dt.date(2012, 9, 13)]
    assert fetched_policy_urls == [
        "https://www.federalreserve.gov/newsevents/press/monetary/20120913a.htm"
    ]

    parser_v2_release_id = published_ids[-1][0]
    fetched_release_urls.clear()
    fetched_policy_urls.clear()
    same_parser = sep.run("postgresql://unused", calc_date="2012-12-31", limit=1)
    assert fetched_release_urls == [urls[2]]
    assert same_parser["unchanged"] == 1
    assert same_parser["releases"] == 0
    assert len(published_ids[-1]) == 0
    assert {row[5] for row in conn.known if row[:5] == latest_identity} == {
        "fomc_sep_html_v1",
        "fomc_sep_html_v2",
    }
    assert parser_v2_release_id != legacy_v1_release_id


def test_bounded_polling_rotates_without_starvation_and_is_replay_deterministic() -> None:
    urls = [
        f"https://www.federalreserve.gov/monetarypolicy/fomcprojtabl2012{month:02d}01.htm"
        for month in range(1, 6)
    ]
    known_routes = set(urls)
    selected_by_day = [
        sep._bounded_release_urls(
            urls,
            known_routes,
            dt.date(2012, 1, 1) + dt.timedelta(days=day),
            2,
        )
        for day in range(len(urls))
    ]

    assert all(len(selected) == 2 for selected in selected_by_day)
    assert set().union(*(set(selected) for selected in selected_by_day)) == set(urls)
    assert urls[-1] in selected_by_day[3]
    assert selected_by_day[2] == sep._bounded_release_urls(
        urls, known_routes, dt.date(2012, 1, 3), 2
    )


def test_bounded_polling_spends_the_strict_cap_on_unseen_routes_first() -> None:
    urls = [
        f"https://www.federalreserve.gov/monetarypolicy/fomcprojtabl2012{month:02d}01.htm"
        for month in range(1, 6)
    ]
    known_routes = set(urls[:2])

    selected = sep._bounded_release_urls(
        urls, known_routes, dt.date(2012, 2, 1), 2
    )

    assert selected == urls[-2:]
    assert len(selected) == 2


def test_bounded_run_selects_and_publishes_exact_december_2012_route_when_generic_is_known(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generic_url = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtabl20121212.htm"
    )
    compilation_url = (
        "https://www.federalreserve.gov/monetarypolicy/files/"
        "FOMC20121212SEPcompilation.htm"
    )
    policy_url = (
        "https://www.federalreserve.gov/newsevents/press/monetary/"
        "20121212a.htm"
    )
    policy_content = (
        b"<html><body>The Committee decided to keep the target range for the "
        b"federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    known_id = uuid.uuid4()
    known_content = _fixture("quarter_point.html")
    policy_hash = sep.source_sha256(policy_content)
    conn = _FakeConnection(
        {
            (
                dt.date(2012, 12, 12),
                generic_url,
                sep.source_sha256(known_content),
                policy_url,
                policy_hash,
                sep.PARSER_VERSION,
            ): known_id
        },
    )
    published: list[sep.ReleaseArtifact] = []
    fetched: list[str] = []

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    def fake_get(_client: object, url: str) -> bytes:
        fetched.append(url)
        if url == policy_url:
            return policy_content
        assert url == compilation_url
        return _fixture("december_2012_range_bins.html")

    def publish(_conn: object, artifacts: list[sep.ReleaseArtifact]) -> tuple[int, int]:
        published.extend(artifacts)
        return len(artifacts), sum(len(item.distributions) for item in artifacts)

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [])
    monkeypatch.setattr(sep, "_historical_release_urls", lambda _as_of: [compilation_url])
    monkeypatch.setattr(sep, "_get_official_html", _with_resolved_url(fake_get))
    monkeypatch.setattr(sep, "_publish_artifacts", publish)

    result = sep.run("postgresql://unused", calc_date="2012-12-12", limit=1)

    assert fetched == [compilation_url, policy_url]
    assert result["releases"] == 1
    assert result["unchanged"] == 0
    assert [artifact.source_url for artifact in published] == [compilation_url]


def test_run_persists_resolved_policy_redirect_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_url = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtabl20120913.htm"
    )
    legacy_policy_url = (
        "https://www.federalreserve.gov/newsevents/press/monetary/"
        "20120913a.htm"
    )
    current_policy_url = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20120913a.htm"
    )
    release_content = _fixture("quarter_point.html")
    policy_content = (
        b"<html><body>The Committee decided to keep the target range "
        b"for the federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    conn = _FakeConnection()
    published: list[sep.ReleaseArtifact] = []
    requested: list[str] = []

    @contextlib.contextmanager
    def acquired(_conn: object, _lock: int):
        yield True

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        requested.append(url)
        if url == release_url:
            return httpx.Response(
                200,
                headers={"content-type": "text/html"},
                content=release_content,
            )
        if url == legacy_policy_url:
            return httpx.Response(302, headers={"location": current_policy_url})
        assert url == current_policy_url
        return httpx.Response(
            200,
            headers={"content-type": "text/html"},
            content=policy_content,
        )

    client_class = httpx.Client

    def client_factory(**kwargs: object) -> httpx.Client:
        return client_class(
            transport=httpx.MockTransport(handler),
            headers=kwargs.get("headers"),
            follow_redirects=False,
        )

    def publish(
        fake_conn: _FakeConnection, artifacts: list[sep.ReleaseArtifact]
    ) -> tuple[int, int]:
        published.extend(artifacts)
        for artifact in artifacts:
            assert artifact.policy_source_url == current_policy_url
            fake_conn.known[
                (
                    artifact.release_date,
                    artifact.source_url,
                    artifact.source_sha256,
                    artifact.policy_source_url,
                    artifact.policy_source_sha256 or "",
                    artifact.parser_version,
                )
            ] = artifact.release_id
            fake_conn.release_dates[artifact.release_id] = artifact.release_date
        return len(artifacts), sum(len(item.distributions) for item in artifacts)

    monkeypatch.setattr(sep, "connect", lambda _dsn: conn)
    monkeypatch.setattr(sep, "advisory_lock", acquired)
    monkeypatch.setattr(sep, "_index_urls", lambda _as_of: [])
    monkeypatch.setattr(sep, "_historical_release_urls", lambda _as_of: [release_url])
    monkeypatch.setattr(sep.httpx, "Client", client_factory)
    monkeypatch.setattr(sep, "_publish_artifacts", publish)

    first = sep.run("postgresql://unused", calc_date="2012-12-31")
    second = sep.run("postgresql://unused", calc_date="2012-12-31")

    assert first["releases"] == 1
    assert second["unchanged"] == 1
    assert second["releases"] == 0
    assert [artifact.policy_source_url for artifact in published] == [current_policy_url]
    assert requested == [
        release_url,
        legacy_policy_url,
        current_policy_url,
        release_url,
        legacy_policy_url,
        current_policy_url,
    ]


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
                release_url,
                sep.source_sha256(release_a),
                sep.policy_statement_url(release_date),
                policy_hash,
                sep.PARSER_VERSION,
            ): release_a_id,
            (
                release_date,
                release_url,
                sep.source_sha256(release_b),
                sep.policy_statement_url(release_date),
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
    monkeypatch.setattr(sep, "_get_official_html", _with_resolved_url(fake_get))

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
                older_url,
                sep.source_sha256(release_content),
                sep.policy_statement_url(dt.date(2012, 6, 20)),
                policy_hash,
                sep.PARSER_VERSION,
            ): older_id,
            (
                dt.date(2012, 9, 13),
                "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120913.htm",
                sep.source_sha256(release_content + b"\n"),
                sep.policy_statement_url(dt.date(2012, 9, 13)),
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
    monkeypatch.setattr(sep, "_get_official_html", _with_resolved_url(fake_get))

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
    monkeypatch.setattr(sep, "_get_official_html", _with_resolved_url(fake_get))
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


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL unavailable"
)
def test_postgres_legacy_migration_and_pointer_invariants() -> None:
    import psycopg
    from psycopg import sql

    schema = f"test_fomc_sep_{uuid.uuid4().hex}"
    ddl = Path("schemas/fomc_sep_ingestion.sql").read_text(encoding="utf-8")
    now = dt.datetime(2026, 8, 21, tzinfo=dt.timezone.utc)
    release_sql = """
        INSERT INTO fomc_sep_releases(
            release_id, release_date, source_url, source_sha256, parser_version,
            source_format, policy_source_url, policy_source_sha256,
            policy_rate_lower_pct, policy_rate_upper_pct, policy_rate_midpoint_pct,
            observed_at, fetched_at
        ) VALUES (%s, %s, %s, %s, %s, 'range_bins', %s, %s,
                  0.000, 0.250, 0.125, %s, %s)
    """
    distribution_sql = """
        INSERT INTO fomc_sep_rate_distributions(
            release_id, projection_horizon, rate_bin_low,
            rate_bin_high, bin_kind, participant_count
        ) VALUES (%s, %s, %s, %s, 'point', %s)
    """

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        try:
            conn.execute(ddl)
            conn.execute(
                "ALTER TABLE fomc_sep_releases DROP CONSTRAINT "
                "fomc_sep_releases_provenance_observation_key"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases ADD CONSTRAINT "
                "fomc_sep_releases_observation_key UNIQUE ("
                "release_date, source_sha256, policy_source_sha256, parser_version)"
            )
            legacy_release_id = uuid.uuid4()
            conn.execute(
                release_sql,
                (
                    legacy_release_id,
                    dt.date(2012, 1, 25),
                    "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20120125.htm",
                    "e" * 64,
                    sep.PARSER_VERSION,
                    "https://www.federalreserve.gov/newsevents/pressreleases/monetary20120125a.htm",
                    "f" * 64,
                    now,
                    now,
                ),
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases DROP CONSTRAINT "
                "fomc_sep_releases_source_url_release_date_v2_check"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases ADD CONSTRAINT "
                "fomc_sep_releases_source_url_check CHECK ("
                "source_url ~ '^https://www[.]federalreserve[.]gov/monetarypolicy/"
                "fomcprojtabl[0-9]{8}[.]htm$')"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases ADD CONSTRAINT "
                "fomc_sep_releases_source_url_official_routes_check CHECK ("
                "source_url ~ '^https://www[.]federalreserve[.]gov/monetarypolicy/"
                "(fomcprojtabl[0-9]{8}[.]htm|files/FOMC20121212SEPcompilation[.]htm)$')"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases DROP CONSTRAINT "
                "fomc_sep_releases_policy_source_url_release_date_v2_check"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases ADD CONSTRAINT "
                "fomc_sep_releases_policy_source_url_check CHECK ("
                "policy_source_url ~ '^https://www[.]federalreserve[.]gov/newsevents/"
                "pressreleases/monetary[0-9]{8}a[.]htm$')"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases ADD CONSTRAINT "
                "fomc_sep_releases_policy_source_url_official_routes_check CHECK ("
                "policy_source_url ~ '^https://www[.]federalreserve[.]gov/newsevents/"
                "(press/monetary/[0-9]{8}a[.]htm|pressreleases/monetary[0-9]{8}a[.]htm)$')"
            )
            legacy_route_checks = {
                row[0]
                for row in conn.execute(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = 'fomc_sep_releases'::regclass "
                    "AND conname LIKE '%source_url%check'"
                ).fetchall()
            }
            assert legacy_route_checks == {
                "fomc_sep_releases_source_url_check",
                "fomc_sep_releases_source_url_official_routes_check",
                "fomc_sep_releases_policy_source_url_check",
                "fomc_sep_releases_policy_source_url_official_routes_check",
            }

            conn.execute(ddl)
            conn.execute(ddl)
            constraints = dict(
                conn.execute(
                    "SELECT conname, convalidated FROM pg_constraint "
                    "WHERE conrelid = 'fomc_sep_releases'::regclass "
                    "AND conname LIKE '%source_url%check'"
                ).fetchall()
            )
            assert constraints == {
                "fomc_sep_releases_policy_source_url_release_date_v2_check": True,
                "fomc_sep_releases_source_url_release_date_v2_check": True,
            }
            observation_keys = conn.execute(
                "SELECT conname FROM pg_constraint "
                "WHERE conrelid = 'fomc_sep_releases'::regclass "
                "AND conname LIKE 'fomc_sep_releases%observation_key'"
            ).fetchall()
            assert observation_keys == [
                ("fomc_sep_releases_provenance_observation_key",)
            ]
            assert conn.execute(
                "SELECT count(*) FROM fomc_sep_releases WHERE release_id = %s",
                (legacy_release_id,),
            ).fetchone()[0] == 1

            conn.execute(
                "ALTER TABLE fomc_sep_releases DROP CONSTRAINT "
                "fomc_sep_releases_provenance_observation_key"
            )
            conn.execute(
                "ALTER TABLE fomc_sep_releases ADD CONSTRAINT "
                "fomc_sep_releases_release_date_source_sha256_policy_source_sha256_key "
                "UNIQUE (release_date, source_sha256, policy_source_sha256)"
            )
            conn.execute(ddl)
            assert conn.execute(
                "SELECT conname FROM pg_constraint "
                "WHERE conrelid = 'fomc_sep_releases'::regclass "
                "AND contype = 'u' ORDER BY conname"
            ).fetchall() == [("fomc_sep_releases_provenance_observation_key",)]

            release_date = dt.date(2012, 12, 12)
            generic_url = (
                "https://www.federalreserve.gov/monetarypolicy/"
                "fomcprojtabl20121212.htm"
            )
            compilation_url = (
                "https://www.federalreserve.gov/monetarypolicy/files/"
                "FOMC20121212SEPcompilation.htm"
            )
            legacy_policy_url = (
                "https://www.federalreserve.gov/newsevents/press/monetary/"
                "20121212a.htm"
            )
            current_policy_url = (
                "https://www.federalreserve.gov/newsevents/pressreleases/"
                "monetary20121212a.htm"
            )
            release_content = _fixture("december_2012_range_bins.html")
            policy_content = (
                b"<html><body>The Committee decided to keep the target range for the "
                b"federal funds rate at 0 to 1/4 percent.</body></html>"
            )
            route_artifacts = [
                sep.parse_release(
                    release_content,
                    generic_url,
                    policy_content=policy_content,
                    policy_url=legacy_policy_url,
                ),
                sep.parse_release(
                    release_content,
                    compilation_url,
                    policy_content=policy_content,
                    policy_url=legacy_policy_url,
                ),
                sep.parse_release(
                    release_content,
                    compilation_url,
                    policy_content=policy_content,
                    policy_url=current_policy_url,
                ),
            ]
            assert len({artifact.release_id for artifact in route_artifacts}) == 3
            for artifact in route_artifacts:
                assert sep._publish_artifacts(conn, [artifact])[0] == 1
                assert conn.execute(
                    "SELECT release_id FROM fomc_sep_current_pointer WHERE singleton"
                ).fetchone()[0] == artifact.release_id
            assert conn.execute(
                "SELECT count(*) FROM fomc_sep_releases "
                "WHERE release_date = %s AND source_sha256 = %s "
                "AND policy_source_sha256 = %s AND parser_version = %s",
                (
                    release_date,
                    route_artifacts[0].source_sha256,
                    route_artifacts[0].policy_source_sha256,
                    sep.PARSER_VERSION,
                ),
            ).fetchone()[0] == 3

            special_ids = []
            for release_date, source_url, policy_url, marker in (
                (
                    dt.date(2012, 12, 12),
                    "https://www.federalreserve.gov/monetarypolicy/files/FOMC20121212SEPcompilation.htm",
                    "https://www.federalreserve.gov/newsevents/press/monetary/20121212a.htm",
                    "3",
                ),
                (
                    dt.date(2022, 3, 16),
                    "https://www.federalreserve.gov/monetarypolicy/fomcprojtable20220316.htm",
                    "https://www.federalreserve.gov/newsevents/pressreleases/monetary20220316a.htm",
                    "4",
                ),
            ):
                release_id = uuid.uuid4()
                special_ids.append(release_id)
                conn.execute(
                    release_sql,
                    (
                        release_id, release_date, source_url, marker * 64,
                        sep.PARSER_VERSION, policy_url, chr(ord(marker) + 4) * 64,
                        now, now,
                    ),
                )

            with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
                conn.execute(
                    release_sql,
                    (
                        uuid.uuid4(), dt.date(2021, 3, 17),
                        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20210318.htm",
                        "a" * 64, sep.PARSER_VERSION,
                        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20210317a.htm",
                        "b" * 64, now, now,
                    ),
                )
            with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
                conn.execute(
                    release_sql,
                    (
                        uuid.uuid4(), dt.date(2021, 6, 16),
                        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20210616.htm",
                        "c" * 64, sep.PARSER_VERSION,
                        "https://www.federalreserve.gov/newsevents/pressreleases/monetary20210617a.htm",
                        "d" * 64, now, now,
                    ),
                )

            complete_id = special_ids[1]
            for horizon in ("2022", "2023", "2024"):
                conn.execute(
                    distribution_sql,
                    (complete_id, horizon, Decimal("0.375"), Decimal("0.375"), 19),
                )
            with pytest.raises(psycopg.errors.RaiseException), conn.transaction():
                conn.execute(
                    "INSERT INTO fomc_sep_current_pointer(singleton, release_id) "
                    "VALUES (true, %s)",
                    (complete_id,),
                )
            conn.execute(
                distribution_sql,
                (complete_id, "longer_run", Decimal("2.500"), Decimal("2.500"), 19),
            )
            conn.execute(
                "UPDATE fomc_sep_current_pointer SET release_id = %s WHERE singleton",
                (complete_id,),
            )

            invalid_total_id = uuid.uuid4()
            conn.execute(
                release_sql,
                (
                    invalid_total_id, dt.date(2025, 9, 17),
                    "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20250917.htm",
                    "0" * 64, sep.PARSER_VERSION,
                    "https://www.federalreserve.gov/newsevents/pressreleases/monetary20250917a.htm",
                    "9" * 64, now, now,
                ),
            )
            for horizon in ("2026", "2027", "longer_run"):
                conn.execute(
                    distribution_sql,
                    (invalid_total_id, horizon, Decimal("4.375"), Decimal("4.375"), 19),
                )
            conn.execute(
                distribution_sql,
                (invalid_total_id, "2025", Decimal("4.250"), Decimal("4.250"), 20),
            )
            conn.execute(
                distribution_sql,
                (invalid_total_id, "2025", Decimal("4.500"), Decimal("4.500"), 10),
            )
            with pytest.raises(psycopg.errors.RaiseException), conn.transaction():
                conn.execute(
                    "UPDATE fomc_sep_current_pointer SET release_id = %s WHERE singleton",
                    (invalid_total_id,),
                )
            assert conn.execute(
                "SELECT release_id FROM fomc_sep_current_pointer WHERE singleton"
            ).fetchone()[0] == complete_id
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.skipif(
    not os.getenv("SEC_TEST_DATABASE_URL"), reason="SEC_TEST_DATABASE_URL unavailable"
)
def test_postgres_failed_pointer_promotion_rolls_back_publication() -> None:
    import psycopg
    from psycopg import sql

    schema = f"test_fomc_sep_rollback_{uuid.uuid4().hex}"
    ddl = Path("schemas/fomc_sep_ingestion.sql").read_text(encoding="utf-8")
    release_url = (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomcprojtabl20120913.htm"
    )
    policy_url = (
        "https://www.federalreserve.gov/newsevents/pressreleases/"
        "monetary20120913a.htm"
    )
    policy_content = (
        b"<html><body>The Committee decided to keep the target range "
        b"for the federal funds rate at 0 to 1/4 percent.</body></html>"
    )
    complete = sep.parse_release(
        _fixture("quarter_point.html"),
        release_url,
        policy_content=policy_content,
        policy_url=policy_url,
    )
    incomplete = replace(
        complete,
        distributions=tuple(
            row
            for row in complete.distributions
            if row.projection_horizon != "longer_run"
        ),
    )
    assert len({row.projection_horizon for row in incomplete.distributions}) == 3

    with psycopg.connect(os.environ["SEC_TEST_DATABASE_URL"]) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
        try:
            conn.execute(ddl)
            conn.commit()

            def publish_like_run() -> None:
                try:
                    sep._publish_artifacts(conn, [incomplete])
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise

            with pytest.raises(
                psycopg.errors.RaiseException,
                match="current SEP release requires a complete normalized distribution",
            ):
                publish_like_run()

            assert conn.execute("SELECT count(*) FROM fomc_sep_releases").fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM fomc_sep_rate_distributions"
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM fomc_sep_current_pointer"
            ).fetchone()[0] == 0
        finally:
            conn.execute("SET search_path TO public")
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def test_equal_date_routes_have_a_deterministic_secondary_order() -> None:
    routes = [
        "https://www.federalreserve.gov/monetarypolicy/fomcprojtabl20121212.htm",
        "https://www.federalreserve.gov/monetarypolicy/files/FOMC20121212SEPcompilation.htm",
    ]
    selected = sep._bounded_release_urls(
        list(reversed(routes)), set(), dt.date(2012, 12, 12), 2
    )
    assert selected == sorted(routes)
    assert len(selected) == 2
