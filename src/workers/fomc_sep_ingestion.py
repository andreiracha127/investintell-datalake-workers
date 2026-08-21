"""Ingest official Federal Reserve FOMC SEP policy-rate distributions.

The Federal Reserve accessible projection pages are the only runtime source.
Each response is hashed over its exact bytes and stored as an immutable release
observation with normalized participant counts by horizon and rate bin. Replays
are no-ops; a retroactive source edit creates a new observation instead of
rewriting history.

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict``.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import re
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx

from src.db import LOCK_FOMC_SEP_INGESTION, advisory_lock, connect

BASE_URL = "https://www.federalreserve.gov"
CALENDAR_URL = f"{BASE_URL}/monetarypolicy/fomccalendars.htm"
PARSER_VERSION = "fomc_sep_html_v1"
BACKFILL_START_YEAR = 2012
MAX_HTML_BYTES = 5_000_000
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "fomc_sep_ingestion.sql"
_RELEASE_PATH = re.compile(r"/monetarypolicy/fomcprojtabl(\d{8})[.]htm")
_RATE_RANGE = re.compile(r"^(-?\d+(?:[.]\d+)?)\s*[-\u2013\u2014]\s*(-?\d+(?:[.]\d+)?)$")
_RATE_POINT = re.compile(r"^-?\d+(?:[.]\d+)?$")
_COUNT = re.compile(r"^\d+$")
_LEGACY_POLICY_PATH = re.compile(r"/newsevents/press/monetary/\d{8}a[.]htm")
_CURRENT_POLICY_PATH = re.compile(
    r"/newsevents/pressreleases/monetary\d{8}a[.]htm"
)
# The March 2017 SEP statement was published on the legacy route; June 2017 was
# the first SEP after the Federal Reserve's spring 2017 website migration.
_POLICY_ROUTE_CUTOVER = dt.date(2017, 6, 14)
_POLICY_RANGE = re.compile(
    r"target range for the federal funds rate (?:at|to) "
    r"(?P<low>\d+(?:-\d+/\d+|/\d+|[.]\d+)?) to "
    r"(?P<high>\d+(?:-\d+/\d+|/\d+|[.]\d+)?) percent",
    re.IGNORECASE,
)
_NAMESPACE = uuid.UUID("3ab0f348-9661-5cee-8b36-d79e66c21025")

# The Federal Reserve historical-year indexes no longer link these accessible
# HTML pages, although the official pages remain live. Dates are therefore
# pinned from the verified official URL identifiers through 2020; the current
# calendar discovers every release from 2021 onward.
_HISTORICAL_RELEASE_DATES = (
    "20120125", "20120425", "20120620", "20120913",
    "20130320", "20130619", "20130918", "20131218",
    "20140319", "20140618", "20140917", "20141217",
    "20150318", "20150617", "20150917", "20151216",
    "20160316", "20160615", "20160921", "20161214",
    "20170315", "20170614", "20170920", "20171213",
    "20180321", "20180613", "20180926", "20181219",
    "20190320", "20190619", "20190918", "20191211",
    "20200610", "20200916", "20201216",
)


class SepIngestionError(RuntimeError):
    """An official source or parser invariant failed; publish nothing."""


@dataclass(frozen=True)
class Cell:
    text: str
    colspan: int = 1


@dataclass(frozen=True)
class HtmlTable:
    heading: str
    rows: tuple[tuple[Cell, ...], ...]


@dataclass(frozen=True)
class DistributionRow:
    projection_horizon: str
    rate_bin_low: Decimal
    rate_bin_high: Decimal
    bin_kind: str
    participant_count: int


@dataclass(frozen=True)
class ReleaseArtifact:
    release_id: uuid.UUID
    release_date: dt.date
    source_url: str
    source_sha256: str
    parser_version: str
    source_format: str
    observed_at: dt.datetime
    fetched_at: dt.datetime
    distributions: tuple[DistributionRow, ...]
    policy_source_url: str | None = None
    policy_source_sha256: str | None = None
    policy_rate_lower_pct: Decimal | None = None
    policy_rate_upper_pct: Decimal | None = None
    policy_rate_midpoint_pct: Decimal | None = None


def _clean_text(parts: list[str]) -> str:
    return " ".join("".join(parts).replace("\xa0", " ").split())


class _PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.tables: list[HtmlTable] = []
        self.text_parts: list[str] = []
        self._heading = ""
        self._heading_parts: list[str] | None = None
        self._table_rows: list[tuple[Cell, ...]] | None = None
        self._row: list[Cell] | None = None
        self._cell_parts: list[str] | None = None
        self._cell_colspan = 1

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        if tag == "a" and values.get("href"):
            self.links.append(str(values["href"]))
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._heading_parts = []
        elif tag == "table":
            self._table_rows = []
        elif tag == "tr" and self._table_rows is not None:
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell_parts = []
            try:
                self._cell_colspan = max(1, int(values.get("colspan") or 1))
            except ValueError:
                self._cell_colspan = 1

    def handle_data(self, data: str) -> None:
        self.text_parts.append(data)
        if self._heading_parts is not None:
            self._heading_parts.append(data)
        if self._cell_parts is not None:
            self._cell_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._cell_parts is not None and self._row is not None:
            self._row.append(Cell(_clean_text(self._cell_parts), self._cell_colspan))
            self._cell_parts = None
            self._cell_colspan = 1
        elif tag == "tr" and self._row is not None and self._table_rows is not None:
            if self._row:
                self._table_rows.append(tuple(self._row))
            self._row = None
        elif tag == "table" and self._table_rows is not None:
            self.tables.append(HtmlTable(self._heading, tuple(self._table_rows)))
            self._table_rows = None
        elif tag in {"h1", "h2", "h3", "h4", "h5", "h6"} and self._heading_parts is not None:
            self._heading = _clean_text(self._heading_parts)
            self._heading_parts = None

    @property
    def page_text(self) -> str:
        return _clean_text(self.text_parts)


def source_sha256(content: bytes) -> str:
    """SHA-256 over untouched response bytes (no decoding or newline changes)."""
    return hashlib.sha256(content).hexdigest()


def policy_statement_url(release_date: dt.date) -> str:
    if release_date < _POLICY_ROUTE_CUTOVER:
        return (
            f"{BASE_URL}/newsevents/press/monetary/"
            f"{release_date:%Y%m%d}a.htm"
        )
    return (
        f"{BASE_URL}/newsevents/pressreleases/"
        f"monetary{release_date:%Y%m%d}a.htm"
    )


def canonical_policy_statement_url(source_url: str) -> str:
    """Return one of the two exact official statement URL shapes or fail closed."""
    parsed = urlsplit(source_url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise SepIngestionError(
            f"not a canonical FOMC statement URL: {source_url}"
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.federalreserve.gov"
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not (
            _LEGACY_POLICY_PATH.fullmatch(parsed.path)
            or _CURRENT_POLICY_PATH.fullmatch(parsed.path)
        )
    ):
        raise SepIngestionError(f"not a canonical FOMC statement URL: {source_url}")
    return f"{BASE_URL}{parsed.path}"


def _canonical_policy_redirect(source_url: str, location: str) -> str:
    source = urlsplit(canonical_policy_statement_url(source_url))
    candidate = urlsplit(urljoin(source_url, location))
    if candidate.scheme == "http" and candidate.hostname == "www.federalreserve.gov":
        candidate = candidate._replace(scheme="https")
    target = urlsplit(canonical_policy_statement_url(candidate.geturl()))
    source_date = re.search(r"(\d{8})a[.]htm$", source.path)
    target_date = re.search(r"(\d{8})a[.]htm$", target.path)
    if (
        not _LEGACY_POLICY_PATH.fullmatch(source.path)
        or not _CURRENT_POLICY_PATH.fullmatch(target.path)
        or source_date is None
        or target_date is None
        or source_date.group(1) != target_date.group(1)
    ):
        raise SepIngestionError(
            f"refusing non-canonical Federal Reserve redirect: {source_url}"
        )
    return target.geturl()


def _mixed_number(value: str) -> Decimal:
    if "-" in value:
        whole, fraction = value.split("-", 1)
        numerator, denominator = fraction.split("/", 1)
        return Decimal(whole) + Decimal(numerator) / Decimal(denominator)
    if "/" in value:
        numerator, denominator = value.split("/", 1)
        return Decimal(numerator) / Decimal(denominator)
    return Decimal(value)


def parse_policy_rate(
    content: bytes, source_url: str
) -> tuple[Decimal, Decimal, Decimal]:
    source_url = canonical_policy_statement_url(source_url)
    text = _parse_page(content, source_url).page_text
    match = _POLICY_RANGE.search(text.replace("\u2013", "-").replace("\u2014", "-"))
    if match is None:
        raise SepIngestionError(
            f"FOMC statement carries no target federal-funds range: {source_url}"
        )
    lower = _mixed_number(match.group("low")).quantize(Decimal("0.001"))
    upper = _mixed_number(match.group("high")).quantize(Decimal("0.001"))
    if lower > upper:
        raise SepIngestionError("FOMC target range is inverted")
    midpoint = ((lower + upper) / 2).quantize(Decimal("0.001"))
    return lower, upper, midpoint


def _release_date(source_url: str) -> dt.date:
    match = _RELEASE_PATH.fullmatch(urlsplit(source_url).path)
    if not match:
        raise SepIngestionError(f"not a canonical SEP release URL: {source_url}")
    return dt.datetime.strptime(match.group(1), "%Y%m%d").date()


def canonical_release_url(href: str) -> str:
    """Return a canonical official SEP URL or fail closed."""
    candidate = urljoin(BASE_URL, href)
    parsed = urlsplit(candidate)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.federalreserve.gov"
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not _RELEASE_PATH.fullmatch(parsed.path)
    ):
        raise SepIngestionError(f"refusing non-Federal-Reserve SEP URL: {candidate}")
    canonical = f"{BASE_URL}{parsed.path}"
    _release_date(canonical)
    return canonical


def _decode_html(content: bytes, source_url: str) -> str:
    try:
        return content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SepIngestionError(f"official SEP page is not UTF-8: {source_url}") from exc


def _parse_page(content: bytes, source_url: str) -> _PageParser:
    parser = _PageParser()
    try:
        parser.feed(_decode_html(content, source_url))
        parser.close()
    except Exception as exc:
        raise SepIngestionError(f"invalid SEP HTML from {source_url}") from exc
    return parser


def discover_release_urls(index_pages: list[tuple[str, bytes]], as_of: dt.date) -> list[str]:
    """Extract canonical release links from official calendar/archive HTML."""
    urls: set[str] = set()
    for index_url, content in index_pages:
        parser = _parse_page(content, index_url)
        for href in parser.links:
            if "fomcprojtabl" not in href.lower():
                continue
            if not _RELEASE_PATH.fullmatch(urlsplit(urljoin(BASE_URL, href)).path):
                continue
            url = canonical_release_url(href)
            release_date = _release_date(url)
            if dt.date(BACKFILL_START_YEAR, 1, 1) <= release_date <= as_of:
                urls.add(url)
    return sorted(urls, key=_release_date)


def _expand(cells: tuple[Cell, ...]) -> list[str]:
    expanded: list[str] = []
    for cell in cells:
        expanded.extend([cell.text] * cell.colspan)
    return expanded


def _horizon(value: str) -> str | None:
    normalized = value.strip().lower().replace("-", " ")
    if normalized in {"longer run", "long run"}:
        return "longer_run"
    match = re.search(r"\b(20\d{2})\b", normalized)
    return match.group(1) if match else None


def _parse_count(value: str) -> int:
    normalized = value.strip().replace("\u2014", "").replace("-", "")
    if not normalized:
        return 0
    if not _COUNT.fullmatch(normalized):
        raise SepIngestionError(f"invalid SEP participant count: {value!r}")
    return int(normalized)


def _aligned_values(cells: tuple[Cell, ...], width: int) -> list[str]:
    values = _expand(cells)
    if len(values) > width:
        raise SepIngestionError("SEP distribution row has more cells than its header")
    return values + [""] * (width - len(values))


def _parse_range_table(table: HtmlTable, release_date: dt.date) -> list[DistributionRow]:
    header_index = next(
        (i for i, row in enumerate(table.rows) if row and "percent range" in row[0].text.lower()),
        None,
    )
    if header_index is None or header_index + 1 >= len(table.rows):
        raise SepIngestionError("range-bin SEP table is missing headers")
    years = _expand(table.rows[header_index][1:])
    labels = _expand(table.rows[header_index + 1])
    if len(labels) == len(years) + 1:
        labels = labels[1:]
    if len(labels) != len(years):
        raise SepIngestionError("range-bin SEP year/projection headers do not align")
    release_month = release_date.strftime("%B").lower()
    selected = [i for i, label in enumerate(labels) if release_month in label.lower()]
    if not selected:
        raise SepIngestionError("range-bin SEP table has no current-release columns")

    rows: list[DistributionRow] = []
    for raw_row in table.rows[header_index + 2 :]:
        if not raw_row:
            continue
        match = _RATE_RANGE.fullmatch(raw_row[0].text.strip())
        if not match:
            continue
        low, high = Decimal(match.group(1)), Decimal(match.group(2))
        values = _aligned_values(raw_row[1:], len(years))
        for index in selected:
            horizon = _horizon(years[index])
            if horizon is None:
                raise SepIngestionError(f"invalid SEP projection horizon: {years[index]!r}")
            count = _parse_count(values[index])
            if count:
                rows.append(DistributionRow(horizon, low, high, "range", count))
    return rows


def _point_format(page_text: str, release_date: dt.date, rates: set[Decimal]) -> str:
    text = page_text.lower()
    if any(token in text for token in ("1/4 percentage point", "1\u20444 percentage point", "\u00bc percentage point")):
        return "quarter_point"
    if any(token in text for token in ("1/8 percentage point", "1\u20448 percentage point", "\u215b percentage point")):
        return "eighth_point"
    try:
        if any(rate * 4 != (rate * 4).to_integral_value() for rate in rates):
            return "eighth_point"
    except InvalidOperation as exc:
        raise SepIngestionError("invalid policy-rate precision") from exc
    return "quarter_point" if release_date.year <= 2014 else "eighth_point"


def _parse_point_table(table: HtmlTable) -> tuple[list[DistributionRow], set[Decimal]]:
    header_index = next(
        (
            i
            for i, row in enumerate(table.rows)
            if row
            and (
                "target federal funds rate" in row[0].text.lower()
                or "midpoint of target range" in row[0].text.lower()
            )
        ),
        None,
    )
    if header_index is None:
        raise SepIngestionError("point-bin SEP table is missing its rate header")
    horizons = [_horizon(value) for value in _expand(table.rows[header_index][1:])]
    if not horizons or any(value is None for value in horizons):
        raise SepIngestionError("point-bin SEP projection horizons are invalid")

    rows: list[DistributionRow] = []
    rates: set[Decimal] = set()
    for raw_row in table.rows[header_index + 1 :]:
        if not raw_row or not _RATE_POINT.fullmatch(raw_row[0].text.strip()):
            continue
        rate = Decimal(raw_row[0].text.strip())
        rates.add(rate)
        values = _aligned_values(raw_row[1:], len(horizons))
        for index, horizon in enumerate(horizons):
            count = _parse_count(values[index])
            if count:
                rows.append(DistributionRow(str(horizon), rate, rate, "point", count))
    return rows, rates


def _validate_distribution(rows: list[DistributionRow]) -> tuple[DistributionRow, ...]:
    if not rows:
        raise SepIngestionError("SEP page contains no policy-rate distribution")
    keys: set[tuple[str, Decimal, Decimal]] = set()
    totals: dict[str, int] = {}
    for row in rows:
        key = (row.projection_horizon, row.rate_bin_low, row.rate_bin_high)
        if key in keys:
            raise SepIngestionError(f"duplicate SEP distribution bin: {key}")
        keys.add(key)
        totals[row.projection_horizon] = totals.get(row.projection_horizon, 0) + row.participant_count
    if len(totals) < 2:
        raise SepIngestionError("SEP distribution must contain at least two horizons")
    if any(total < 1 or total > 25 for total in totals.values()):
        raise SepIngestionError(f"implausible SEP participant totals: {totals}")
    return tuple(sorted(rows, key=lambda row: (row.projection_horizon, row.rate_bin_low)))


def parse_release(
    content: bytes,
    source_url: str,
    fetched_at: dt.datetime | None = None,
    *,
    policy_content: bytes | None = None,
    policy_url: str | None = None,
) -> ReleaseArtifact:
    """Parse one exact official HTML response into an immutable release artifact."""
    source_url = canonical_release_url(source_url)
    release_date = _release_date(source_url)
    parser = _parse_page(content, source_url)
    page_text = parser.page_text
    lower_text = page_text.lower()
    if "federal reserve" not in lower_text or "federal funds rate" not in lower_text:
        raise SepIngestionError("page is not an official FOMC projection release")

    range_tables = [
        table
        for table in parser.tables
        if "federal funds rate" in table.heading.lower()
        and any(row and "percent range" in row[0].text.lower() for row in table.rows)
    ]
    if range_tables:
        distributions = _parse_range_table(range_tables[0], release_date)
        source_format = "range_bins"
    else:
        point_tables = [
            table
            for table in parser.tables
            if any(
                row
                and (
                    "target federal funds rate" in row[0].text.lower()
                    or "midpoint of target range" in row[0].text.lower()
                )
                for row in table.rows
            )
        ]
        if not point_tables:
            raise SepIngestionError("SEP page has no recognized policy-rate table")
        distributions, rates = _parse_point_table(point_tables[0])
        source_format = _point_format(page_text, release_date, rates)

    digest = source_sha256(content)
    fetched = fetched_at or dt.datetime.now(dt.timezone.utc)
    if fetched.tzinfo is None:
        raise SepIngestionError("fetched_at must be timezone-aware")
    policy_digest = source_sha256(policy_content) if policy_content is not None else None
    policy_rates = (
        parse_policy_rate(policy_content, policy_url)
        if policy_content is not None and policy_url is not None
        else (None, None, None)
    )
    release_id = uuid.uuid5(
        _NAMESPACE,
        f"{source_url}|{digest}|{policy_digest or 'policy-unavailable'}|{PARSER_VERSION}",
    )
    return ReleaseArtifact(
        release_id=release_id,
        release_date=release_date,
        source_url=source_url,
        source_sha256=digest,
        parser_version=PARSER_VERSION,
        source_format=source_format,
        observed_at=fetched,
        fetched_at=fetched,
        distributions=_validate_distribution(distributions),
        policy_source_url=policy_url,
        policy_source_sha256=policy_digest,
        policy_rate_lower_pct=policy_rates[0],
        policy_rate_upper_pct=policy_rates[1],
        policy_rate_midpoint_pct=policy_rates[2],
    )


def _get_official_html(client: httpx.Client, url: str) -> bytes:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname != "www.federalreserve.gov":
        raise SepIngestionError(f"refusing non-Federal-Reserve source: {url}")
    current_url = url
    redirected = False
    while True:
        last_status: int | None = None
        for attempt in range(3):
            try:
                response = client.get(current_url)
            except httpx.HTTPError as exc:
                if attempt == 2:
                    raise SepIngestionError(
                        f"Federal Reserve request failed: {current_url}"
                    ) from exc
                time.sleep(2**attempt)
                continue
            last_status = response.status_code
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "").lower()
                if "text/html" not in content_type:
                    raise SepIngestionError(f"Federal Reserve source is not HTML: {url}")
                content = response.content
                if not content or len(content) > MAX_HTML_BYTES:
                    raise SepIngestionError(
                        f"Federal Reserve HTML size is invalid: {current_url}"
                    )
                return content
            if response.status_code in {301, 302, 307, 308} and not redirected:
                location = response.headers.get("location")
                if location is None:
                    raise SepIngestionError(
                        f"Federal Reserve redirect omitted its target: {current_url}"
                    )
                current_url = _canonical_policy_redirect(current_url, location)
                redirected = True
                break
            if response.status_code not in {429, 500, 502, 503, 504}:
                raise SepIngestionError(
                    f"Federal Reserve request returned {response.status_code}: {current_url}"
                )
            time.sleep(2**attempt)
        else:
            raise SepIngestionError(
                f"Federal Reserve request exhausted retries ({last_status}): {current_url}"
            )


def _index_urls(as_of: dt.date) -> list[str]:
    return [CALENDAR_URL] if as_of.year >= 2021 else []


def _historical_release_urls(as_of: dt.date) -> list[str]:
    return [
        f"{BASE_URL}/monetarypolicy/fomcprojtabl{value}.htm"
        for value in _HISTORICAL_RELEASE_DATES
        if dt.datetime.strptime(value, "%Y%m%d").date() <= as_of
    ]


def _known_release_hashes(
    conn: Any,
) -> dict[tuple[dt.date, str, str, str], uuid.UUID]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT release_id, release_date, source_sha256, "
            "policy_source_sha256, parser_version "
            "FROM fomc_sep_releases"
        )
        return {
            (
                row[1],
                str(row[2]).strip(),
                str(row[3]).strip(),
                str(row[4]),
            ): row[0]
            for row in cur.fetchall()
        }


def _bounded_release_urls(
    urls: list[str], known_dates: set[dt.date], as_of: dt.date, limit: int
) -> list[str]:
    unseen_urls = [url for url in urls if _release_date(url) not in known_dates][
        -limit:
    ]
    remaining = limit - len(unseen_urls)
    if not remaining:
        return unseen_urls

    selected = set(unseen_urls)
    polling_urls = [url for url in urls if url not in selected]
    if polling_urls:
        # Daily runs advance the ring; an identical calc_date replay is stable.
        offset = (
            as_of - dt.date(BACKFILL_START_YEAR, 1, 1)
        ).days % len(polling_urls)
        polling_urls = (polling_urls[offset:] + polling_urls[:offset])[:remaining]
    return sorted([*unseen_urls, *polling_urls], key=_release_date)


def _repoint_current_release(conn: Any, release_id: uuid.UUID) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fomc_sep_current_pointer(singleton, release_id)
            VALUES (true, %s)
            ON CONFLICT (singleton) DO UPDATE SET release_id=EXCLUDED.release_id
            WHERE (SELECT release_date FROM fomc_sep_releases WHERE release_id=EXCLUDED.release_id)
                  >=
                  (SELECT release_date FROM fomc_sep_releases
                   WHERE release_id=fomc_sep_current_pointer.release_id)
            """,
            (release_id,),
        )


def _publish_artifacts(conn: Any, artifacts: list[ReleaseArtifact]) -> tuple[int, int]:
    release_count = 0
    distribution_count = 0
    with conn.cursor() as cur:
        for artifact in sorted(artifacts, key=lambda item: item.release_date):
            if (
                artifact.policy_source_url is None
                or artifact.policy_source_sha256 is None
                or artifact.policy_rate_lower_pct is None
                or artifact.policy_rate_upper_pct is None
                or artifact.policy_rate_midpoint_pct is None
            ):
                raise SepIngestionError(
                    "SEP publication requires official policy-rate context"
                )
            cur.execute(
                """
                INSERT INTO fomc_sep_releases(
                    release_id, release_date, source_url, source_sha256,
                    parser_version, source_format, policy_source_url,
                    policy_source_sha256, policy_rate_lower_pct,
                    policy_rate_upper_pct, policy_rate_midpoint_pct,
                    observed_at, fetched_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (release_date, source_sha256, policy_source_sha256, parser_version)
                DO NOTHING
                """,
                (
                    artifact.release_id,
                    artifact.release_date,
                    artifact.source_url,
                    artifact.source_sha256,
                    artifact.parser_version,
                    artifact.source_format,
                    artifact.policy_source_url,
                    artifact.policy_source_sha256,
                    artifact.policy_rate_lower_pct,
                    artifact.policy_rate_upper_pct,
                    artifact.policy_rate_midpoint_pct,
                    artifact.observed_at,
                    artifact.fetched_at,
                ),
            )
            release_count += max(cur.rowcount, 0)
            cur.executemany(
                """
                INSERT INTO fomc_sep_rate_distributions(
                    release_id, projection_horizon, rate_bin_low, rate_bin_high,
                    bin_kind, participant_count
                ) VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (release_id, projection_horizon, rate_bin_low, rate_bin_high)
                DO NOTHING
                """,
                [
                    (
                        artifact.release_id,
                        row.projection_horizon,
                        row.rate_bin_low,
                        row.rate_bin_high,
                        row.bin_kind,
                        row.participant_count,
                    )
                    for row in artifact.distributions
                ],
            )
            distribution_count += max(cur.rowcount, 0)
            _repoint_current_release(conn, artifact.release_id)
    return release_count, distribution_count


def run(
    dsn: str,
    *,
    calc_date: str | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Backfill from 2012 and poll official SEP pages idempotently."""
    as_of = dt.date.fromisoformat(calc_date) if calc_date else dt.date.today()
    if as_of < dt.date(BACKFILL_START_YEAR, 1, 1):
        raise SepIngestionError("calc_date predates the supported 2012 SEP history")

    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FOMC_SEP_INGESTION) as acquired:
            if not acquired:
                return {"status": "lock_busy", "releases": 0, "distributions": 0}
            try:
                with conn.cursor() as cur:
                    cur.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
                known = _known_release_hashes(conn)
                headers = {
                    "Accept": "text/html,application/xhtml+xml",
                    "User-Agent": "InvestIntell-SEP-Ingestion/1.0 (+https://hub.investintell.com)",
                }
                with httpx.Client(timeout=45.0, headers=headers, follow_redirects=False) as client:
                    index_pages = [(url, _get_official_html(client, url)) for url in _index_urls(as_of)]
                    urls = sorted(
                        set(discover_release_urls(index_pages, as_of))
                        | set(_historical_release_urls(as_of)),
                        key=_release_date,
                    )
                    if not urls:
                        raise SepIngestionError("official Federal Reserve indexes exposed no SEP releases")
                    if limit is not None:
                        known_dates = {
                            release_date
                            for release_date, _, _, parser_version in known
                            if parser_version == PARSER_VERSION
                        }
                        urls = _bounded_release_urls(urls, known_dates, as_of, limit)
                    artifacts: list[ReleaseArtifact] = []
                    fetched = 0
                    unchanged = 0
                    for url in urls:
                        content = _get_official_html(client, url)
                        statement_url = policy_statement_url(_release_date(url))
                        statement = _get_official_html(client, statement_url)
                        fetched += 1
                        digest = source_sha256(content)
                        statement_digest = source_sha256(statement)
                        known_release_id = known.get(
                            (
                                _release_date(url),
                                digest,
                                statement_digest,
                                PARSER_VERSION,
                            )
                        )
                        if known_release_id is not None:
                            unchanged += 1
                            _repoint_current_release(conn, known_release_id)
                            continue
                        artifacts.append(
                            parse_release(
                                content,
                                url,
                                policy_content=statement,
                                policy_url=statement_url,
                            )
                        )
                releases, distributions = _publish_artifacts(conn, artifacts)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    return {
        "status": "ok",
        "discovered": len(urls),
        "fetched": fetched,
        "unchanged": unchanged,
        "releases": releases,
        "distributions": distributions,
        "parser_version": PARSER_VERSION,
        "as_of": as_of.isoformat(),
    }
