"""Real SEC source slices and point-in-time insider admission regressions.

Database tests are opt-in through ``SEC_INSIDER_TEST_DSN`` and install the exact
schema in a unique schema of that disposable local database. They never connect
through the application's default connection settings.
"""
from __future__ import annotations

import csv
import datetime as dt
import gzip
import hashlib
import io
import json
import os
import uuid
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import load_sec_insider_filings as insider

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sec_insider_ticker_evidence"

# These are the real raw field values, after TSV quote escaping is decoded.
DERA_SYMBOLS = {
    "0001193125-26-409183": [],
    "0001225208-26-007725": ["SIRI"],
    "0000025475-26-000080": ["CRDA", "CRDB"],
    "0001628280-26-062934": ["Z", "ZG"],
    "0000099780-26-000121": ["TRN"],
    "0002151730-26-000002": ["WELPP"],
    "0001314152-26-000191": [],
    "0001728451-26-000004": ["BRK-A"],
    # TrueCar's own Form 4 (issuer CIK 1327318). sec_insider_v3 read TRUE as a
    # boolean placeholder; every whole-field TRUE in DERA is an issuer's symbol.
    "0001327318-26-000010": ["TRUE"],
    "0001193125-26-106222": ["WSO", "WSOB"],
    "0001181431-06-063496": ["TRUE"],  # Centrue Financial
    "0001140361-06-016251": ["OB"],  # OneBeacon, "NYSE: OB"
    "0001454938-25-000115": ["OB"],  # Outbrain
    "0001467481-15-000014": [],  # OTCBB alone names no symbol
    "0001493152-22-016490": [],
    "0000107140-22-000040": ["JWA", "JWB"],
    "0001033012-22-000101": ["FBC"],
    "0000933136-08-000234": ["WM"],
    "0000757011-17-000056": ["USG"],
    "0001225208-15-015903": ["BF-B"],
    "0001225208-19-013728": ["ISCA", "ISCB"],
    "0001258105-24-000007": ["AMNB"],
}


def _real_dera_rows():
    for path in sorted((FIXTURE / "dera").glob("*/SUBMISSION.tsv")):
        with path.open(encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream, delimiter="\t"):
                yield row


@pytest.mark.parametrize(
    "row", list(_real_dera_rows()), ids=lambda row: row["ACCESSION_NUMBER"]
)
def test_normalization_of_real_section_five_filings(row):
    assert insider.normalize_symbols(row["ISSUERTRADINGSYMBOL"]) == DERA_SYMBOLS[
        row["ACCESSION_NUMBER"]
    ]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("?SIRI?", ["SIRI"]),
        ("?X?", ["X"]),
        ("X", ["X"]),
        ("XX", ["XX"]),
        ("NS", ["NS"]),
        ('"""WM"""', ["WM"]),
        ("CBS, CBS.A", ["CBS", "CBS-A"]),
        ("WSO & WSOB", ["WSO", "WSOB"]),
        ("JWA AND JWB", ["JWA", "JWB"]),
        ("BRK/A", ["BRK-A"]),
        ("BRK A", ["BRK-A"]),
        ("NYSE TRN", ["TRN"]),
        ("NASDAQ: AMNB", ["AMNB"]),
        ("EDLG, OB", ["EDLG"]),
        ("EDLG,OB", ["EDLG"]),
        ("EDLG OB", ["EDLG"]),
        ("EDLG.PK", ["EDLG"]),
        ("OTCBB", []),
        ("XXXXXXXXXX", []),
        ("none", []),
        ("No Symbol", []),
        ("N/A", []),
        ("1314152", []),
        ("ISCA, ISCA", ["ISCA"]),
    ],
)
def test_additional_symbol_rules(raw, expected):
    assert insider.normalize_symbols(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # TrueCar and Centrue write TRUE, OneBeacon and Outbrain OB (1,941 filings).
        ("TRUE", ["TRUE"]),
        ("(TRUE)", ["TRUE"]),
        ("OB", ["OB"]),
        ("NYSE: OB", ["OB"]),
        ("NYSE:OB", ["OB"]),
        # A boolean typed as a symbol, as W1 reads it, and the OTC marks stay placeholders.
        ("true", []),
        ("True", []),
        ("FALSE", []),
        ("false", []),
        ("OTCBB", []),
        ("OTC BB", []),
        ("OTCBB-__", []),
        # Beside a symbol OB still qualifies it.
        ("EDLG, OB", ["EDLG"]),
        ("fmbh.ob", ["FMBH"]),
    ],
)
def test_v4_reads_true_and_ob_as_symbols_but_keeps_placeholder_senses(raw, expected):
    assert insider.PARSER_VERSION == "sec_insider_v4"
    assert insider.normalize_symbols(raw) == expected


# Real DERA ISSUERTRADINGSYMBOL values (2006q1-2026q3). Splitting the prose
# ones used to emit fake symbols, some of them other issuers' real tickers.
@pytest.mark.parametrize(
    "raw",
    [
        "NOT LISTED", "not listed", "NOT TRADED", "NOT PUBLIC", "NO TICKER", "No Ticker",
        "SEE REMARK", "IN REMARKS", "(to come)", "None Yet", "app. for", "PENDING",
        "unknown", "PRIVATE", "NOT APPLICABLE", "NO SYMBOL", "[ N/A ]",
        # Issuer names: SEE, FB, CNB and CO are other issuers' tickers.
        "LEE ENT", "EAST FORK", "BAAP FB", "CNB CORP", "HCA INC.", "XPEL, INC.",
        "DEERE & CO", "OWL ROCK T", "hcsb finan", "MSO II", "OTCM PRKA",
    ],
)
def test_placeholder_and_prose_fields_are_rejected_before_splitting(raw):
    assert insider.normalize_symbols(raw) == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Real multi-symbol fields keep every symbol.
        ("JWA/JWB", ["JWA", "JWB"]),
        ("jwa/jwb", ["JWA", "JWB"]),
        ("CRDA CRDB", ["CRDA", "CRDB"]),
        ("TAP.A TAP", ["TAP-A", "TAP"]),
        ("Z AND ZG", ["Z", "ZG"]),
        ("ABI/CRA", ["ABI", "CRA"]),
        ("LTR;CG", ["LTR", "CG"]),
        ("PHC - PIHC", ["PHC", "PIHC"]),
        ("N O G", ["N-O-G"]),
        # Exchange, country and when-issued words qualify the symbol.
        ("FF US", ["FF"]),
        ("CARR WI", ["CARR"]),
        ("FLL AMEX", ["FLL"]),
        ("ASX: HTW", ["HTW"]),
        ("BDGV: OTC", ["BDGV"]),
        ("OTC;RAKR", ["RAKR"]),
        ("/DE/CHD", ["CHD"]),
        # Preferred and line suffixes attach; a lone listed class is the sibling line.
        ("HFC PrB", ["HFC-PB"]),
        ("HBC PR A", ["HBC-PA"]),
        ("REN/REN WS", ["REN", "REN-WS"]),
        ("PDAC - UN", ["PDAC-UN"]),
        ("BWINA / B", ["BWINA", "BWINB"]),
        ("CTMMA,B", ["CTMMA", "CTMMB"]),
        ("SEAL-PA/PB", ["SEAL-PA", "SEAL-PB"]),
        # Single words that are real tickers (Sealed Air, Thoma Bravo Advantage).
        ("SEE", ["SEE"]),
        ("TBA", ["TBA"]),
        ("NYSE: CO", ["CO"]),
    ],
)
def test_real_symbol_lists_survive_prose_rejection(raw, expected):
    assert insider.normalize_symbols(raw) == expected


def _dera_zip(tmp_path, quarter="2026q3", *, replacement_rows=None):
    source = FIXTURE / "dera" / quarter / "SUBMISSION.tsv"
    package = tmp_path / f"{quarter}_form345.zip"
    if replacement_rows is None:
        data = source.read_bytes()
    else:
        stream = io.StringIO(newline="\n")
        fields = source.read_text(encoding="utf-8").splitlines()[0].split("\t")
        writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        writer.writerows(replacement_rows)
        data = stream.getvalue().encode()
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("SUBMISSION.tsv", data)
        # The insider loader has no reason to parse any of the transaction tables.
        archive.writestr("NONDERIV_TRANS.tsv", b"unneeded table")
    return package


def test_dera_parse_retains_raw_filings_and_date_only_knowledge(tmp_path):
    package = _dera_zip(tmp_path)
    filings = {filing.accession: filing for filing in insider.iter_dera_filings(package)}
    assert len(filings) == 8
    siri = filings["0001225208-26-007725"]
    assert siri.cik == 908937
    assert siri.raw_symbol == "(SIRI)"
    assert siri.normalized_symbols == ("SIRI",)
    assert siri.form == "4"
    assert siri.filed == dt.date(2026, 9, 10)
    assert siri.accepted is None
    assert siri.available_on == dt.date(2026, 9, 11)
    assert len(siri.fact_hash) == 64
    # Placeholder evidence remains auditable in the raw table but never resolves.
    assert filings["0001193125-26-409183"].normalized_symbols == ()
    assert filings["0001314152-26-000191"].normalized_symbols == ()


@pytest.mark.parametrize(
    ("accession", "cik", "raw_symbol", "symbols", "accepted"),
    [
        (
            "0000700565-03-000110", 700565, "fmbh.ob", ("FMBH",),
            "2003-05-30T16:36:35-04:00",
        ),
        (
            "0001181431-03-009430", 1103313, "EDLG, OB", ("EDLG",),
            "2003-05-30T15:02:36-04:00",
        ),
    ],
)
def test_real_may_2003_ownership_xml(accession, cik, raw_symbol, symbols, accepted):
    fixture = FIXTURE / "secapi" / accession
    metadata = json.loads((fixture / "metadata.json").read_text(encoding="utf-8"))
    filing = insider.parse_ownership_xml(
        (fixture / "ownership.xml").read_bytes(), metadata,
        source_package="form-5-files/2003/2003-05" if metadata["formType"] == "5"
        else "form-3-files/2003/2003-05",
    )
    assert filing is not None
    assert filing.accession == accession
    assert filing.cik == cik  # issuerCik, not the reporting owner's CIK in metadata.
    assert filing.raw_symbol == raw_symbol
    assert filing.normalized_symbols == symbols
    assert filing.form == metadata["formType"]
    assert filing.filed == dt.date(2003, 5, 30)
    assert filing.accepted == dt.datetime.fromisoformat(accepted)
    assert filing.available_on == dt.date(2003, 5, 30)
    assert filing.filed != dt.date.fromisoformat(metadata["effectivenessDate"])


def test_secapi_archive_ignores_html_rendering_and_counts_each_accession_once(tmp_path):
    accession = "0001181431-03-009430"
    fixture = FIXTURE / "secapi" / accession
    package = tmp_path / "form-5-files" / "2003" / "2003-05.zip"
    package.parent.mkdir(parents=True)
    folder = f"2003-05/{accession.replace('-', '')}"
    with zipfile.ZipFile(package, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(fixture / "metadata.json", f"{folder}/metadata.json")
        archive.write(fixture / "ownership.xml", f"{folder}/rrd10184.xml")
        archive.writestr(f"{folder}/xslF345X01/rrd10184.xml", b"<html>rendering</html>")
        archive.writestr("../../outside.txt", b"untrusted archive member")
    filings = list(insider.iter_secapi_filings(package))
    assert len(filings) == 1
    assert filings[0].accession == accession
    assert filings[0].normalized_symbols == ("EDLG",)
    assert not (tmp_path.parent / "outside.txt").exists()


@pytest.mark.parametrize(
    ("accession", "form", "cik", "ticker", "day"),
    [
        ("0001209191-03-003774", "3", 829224, "SBUX", "2003-05-09"),
        ("0001100885-03-000018", "3/A", 1100885, "DTAS", "2003-05-06"),
    ],
)
def test_edgar_metadata_form_handles_real_malformed_early_xml(accession, form, cik, ticker, day):
    import xml.etree.ElementTree as ET

    fixture = FIXTURE / "secapi" / accession
    raw = (fixture / "ownership.xml").read_bytes()
    # These real early XML specimens incorrectly nest noSecuritiesOwned inside
    # documentType. Its text is whitespace while EDGAR metadata has the form.
    assert not ET.fromstring(raw).findtext("documentType").strip()
    metadata = json.loads((fixture / "metadata.json").read_text(encoding="utf-8"))
    filing = insider.parse_ownership_xml(raw, metadata, source_package="form-3-files/2003/2003-05")
    assert filing.form == form
    assert filing.cik == cik
    assert filing.normalized_symbols == (ticker,)
    assert filing.filed == dt.date.fromisoformat(day)


def test_dera_listing_requires_real_sec_quarterly_links():
    html = '''<a href="/files/dera/data/insider-transactions-data-sets/2006q1_form345.zip">q1</a>
        <a href="https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2006q1_form345.zip">same</a>
        <a href="https://example.invalid/2006q2_form345.zip">unrelated host</a>
        <a href="/files/2026q3_notes.zip">different dataset</a>'''
    assert insider.listed_dera_urls(html) == [
        "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2006q1_form345.zip",
    ]
    with pytest.raises(ValueError, match="no quarterly packages"):
        insider.listed_dera_urls("<html>access denied</html>")


class _ArchiveCatalogue:
    def __init__(self):
        self.downloads = []
        self.payload = b"sample monthly archive"
        self.catalogues = {
            dataset: [
                {"key": f"2003/{month}.zip", "size": len(self.payload),
                 "updatedAt": "2026-10-08T00:00:00Z", "records": 1}
                for month in ("2003-05", "2003-06")
            ]
            for dataset in insider.DATASETS
        }

    def json(self, url):
        dataset = url.rsplit("/", 1)[1].removesuffix(".json")
        return {"containers": self.catalogues[dataset]}

    def download(self, url, path, *, expected_size=None, authenticated=False):
        assert authenticated
        assert "?" not in url
        assert expected_size == len(self.payload)
        self.downloads.append(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload)
        return {"sha256": hashlib.sha256(self.payload).hexdigest(), "size": len(self.payload)}


def test_secapi_monthly_catalogue_cache_detects_same_size_republication_and_corruption(tmp_path):
    client = _ArchiveCatalogue()
    result = insider.sync_secapi(tmp_path, first="2003-05", last="2003-06", client=client)
    assert len(result) == len(client.downloads) == 6
    client.downloads.clear()
    assert insider.sync_secapi(tmp_path, first="2003-05", last="2003-06", client=client) == result
    assert client.downloads == []
    client.catalogues[insider.DATASETS[0]][0]["updatedAt"] = "2026-10-09T00:00:00Z"
    insider.sync_secapi(tmp_path, first="2003-05", last="2003-06", client=client)
    assert client.downloads == [result[0]]
    client.downloads.clear()
    result[-1].write_bytes(b"x" * len(client.payload))
    insider.sync_secapi(tmp_path, first="2003-05", last="2003-06", client=client)
    assert client.downloads == [result[-1]]


@pytest.mark.parametrize("catalogue_defect", ["missing", "duplicate"])
def test_secapi_rejects_incomplete_or_duplicate_monthly_catalogue(tmp_path, catalogue_defect):
    client = _ArchiveCatalogue()
    entries = client.catalogues[insider.DATASETS[0]]
    if catalogue_defect == "missing":
        entries.pop()
    else:
        entries.append(dict(entries[0]))
    with pytest.raises(ValueError, match="incomplete or duplicate"):
        insider.sync_secapi(tmp_path, first="2003-05", last="2003-06", client=client)
    assert client.downloads == []


def test_secapi_download_window_bounds_the_load_from_a_persistent_cache(tmp_path, monkeypatch, capsys):
    accession = "0001181431-03-009430"
    fixture = FIXTURE / "secapi" / accession
    secapi = tmp_path / "secapi"
    archives = {}
    for month in ("2003-05", "2003-06", "2004-01"):
        package = secapi / "form-5-files" / month[:4] / f"{month}.zip"
        package.parent.mkdir(parents=True, exist_ok=True)
        folder = f"{month}/{accession.replace('-', '')}"
        with zipfile.ZipFile(package, "w") as archive:
            archive.write(fixture / "metadata.json", f"{folder}/metadata.json")
            archive.write(fixture / "ownership.xml", f"{folder}/rrd10184.xml")
        archives[month] = package
    windows = []

    def sync(out_dir, *, first, last, api_key):
        windows.append((first, last))
        return [archives["2003-05"], archives["2003-06"]]

    monkeypatch.setattr(insider, "sync_secapi", sync)
    monkeypatch.setattr(insider, "load_api_key", lambda dotenv: "test-key")
    base = ["--packages-dir", str(tmp_path / "dera"), "--secapi-dir", str(secapi), "--dry-run"]

    def loaded(argv):
        assert insider.main(argv) == 0
        return [json.loads(line)["package"] for line in capsys.readouterr().out.splitlines()
                if '"package"' in line]

    window = ["--download-secapi", "--from", "2003-05", "--to", "2003-06"]
    assert loaded(base + window) == ["form-5-files/2003/2003-05.zip", "form-5-files/2003/2003-06.zip"]
    assert windows == [("2003-05", "2003-06")]
    # Explicit packages override the window; without a download the cache loads whole.
    assert loaded(base + window + [str(archives["2004-01"])]) == ["form-5-files/2004/2004-01.zip"]
    assert len(loaded(base)) == 3


def test_http_client_preserves_user_agent_and_request_spacing_without_network(monkeypatch):
    requests = []
    sleeps = []

    def fake_open(request, timeout):
        requests.append(request)
        assert timeout == 90
        return object()

    monkeypatch.setattr(insider.urllib.request, "urlopen", fake_open)
    monkeypatch.setattr(insider.urllib.request, "build_opener", lambda *args: SimpleNamespace(open=fake_open))
    monkeypatch.setattr(insider.time, "sleep", sleeps.append)
    monkeypatch.setattr(insider.time, "monotonic", lambda: 0.0)
    client = insider.HttpClient("fixture-test-key", spacing=0)
    client.request("https://www.sec.gov/files/2006q1_form345.zip")
    client.request("https://api.sec-api.io/datasets/form-3-files/2003/2003-05.zip", authenticated=True)
    assert all(request.get_header("User-agent") == insider.USER_AGENT for request in requests)
    assert requests[0].get_header("Authorization") is None
    assert requests[1].get_header("Authorization") == "fixture-test-key"
    assert all(wait >= 0.1 for wait in sleeps)
    assert sleeps[-1] >= 0.5
    with pytest.raises(ValueError, match="another host"):
        client.request("https://example.invalid/archive.zip", authenticated=True)


def test_secapi_archive_streaming_400_is_retried_but_sec_400_is_not(tmp_path, monkeypatch):
    archive_bytes = io.BytesIO()
    with zipfile.ZipFile(archive_bytes, "w") as archive:
        archive.writestr("2003-05/metadata.json", b"{}")
    data = archive_bytes.getvalue()
    hosts = []

    class Response(io.BytesIO):
        headers = {"Content-Length": str(len(data))}

    def open_url(request, timeout):
        hosts.append(insider.urllib.parse.urlsplit(request.full_url).hostname)
        if len(hosts) < 3:  # sec-api's documented archive streaming failure
            raise insider.urllib.error.HTTPError(request.full_url, 400, "Bad Request", {}, None)
        return Response(data)

    monkeypatch.setattr(insider.urllib.request, "build_opener", lambda *args: SimpleNamespace(open=open_url))
    monkeypatch.setattr(insider.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(insider.time, "sleep", lambda seconds: None)
    client = insider.HttpClient("fixture-test-key", spacing=0)
    target = tmp_path / "form-4-files" / "2003" / "2003-05.zip"
    client.download("https://api.sec-api.io/datasets/form-4-files/2003/2003-05.zip", target,
                    expected_size=len(data), authenticated=True)
    assert hosts == ["api.sec-api.io"] * 3
    assert target.read_bytes() == data
    hosts.clear()
    with pytest.raises(RuntimeError, match="HTTP request failed"):
        client.download("https://www.sec.gov/files/2006q1_form345.zip", tmp_path / "2006q1_form345.zip")
    assert hosts == ["www.sec.gov"]


def test_safe_error_output_redacts_registered_key_and_postgres_password():
    insider.HttpClient("fixture-sensitive-api-key")
    text = insider.scrub(
        "fixture-sensitive-api-key postgresql://user:fixture-password@127.0.0.1/db "
        "password=fixture-password"
    )
    assert "fixture-sensitive-api-key" not in text
    assert "fixture-password" not in text


class _DeraResponse(io.BytesIO):
    def __init__(self, payload, headers=None):
        super().__init__(payload)
        self.headers = headers or {}


class _DeraHttp:
    def __init__(self, *, etag='"original"', last_modified="Mon, 05 Oct 2026 00:00:00 GMT",
                 listing_gzip=False):
        self.downloads = []
        self.payload = b"original package content"
        self.etag = etag
        self.last_modified = last_modified
        self.listing_gzip = listing_gzip
        self.url = "https://www.sec.gov/files/dera/data/insider-transactions-data-sets/2006q1_form345.zip"

    def request(self, url, *, method="GET"):
        if method == "HEAD":
            assert url == self.url
            return _DeraResponse(b"", {
                "Content-Length": str(len(self.payload)),
                "ETag": self.etag, "Last-Modified": self.last_modified,
            })
        assert url == insider.DERA_LISTING_URL
        listing = f'<a href="{self.url}">package</a>'.encode()
        if self.listing_gzip:
            return _DeraResponse(gzip.compress(listing), {"Content-Encoding": "gzip"})
        return _DeraResponse(listing)

    def download(self, url, path, *, expected_size=None):
        assert url == self.url
        assert expected_size == len(self.payload)
        self.downloads.append(path)
        path.write_bytes(self.payload)
        return {"size": len(self.payload), "sha256": hashlib.sha256(self.payload).hexdigest(),
                "etag": self.etag, "last_modified": self.last_modified}


def test_dera_verified_cache_reuses_unchanged_validator_but_checks_local_hash(tmp_path):
    client = _DeraHttp()
    (path,) = insider.sync_dera(tmp_path, verify_cache=True, client=client)
    assert client.downloads == [path]
    client.downloads.clear()
    assert insider.sync_dera(tmp_path, verify_cache=True, client=client) == [path]
    assert client.downloads == []
    path.write_bytes(b"x" * len(client.payload))
    insider.sync_dera(tmp_path, verify_cache=True, client=client)
    assert client.downloads == [path]


def test_dera_remote_modified_validator_change_refetches_even_with_newer_local_mtime(tmp_path):
    client = _DeraHttp(etag=None)
    (path,) = insider.sync_dera(tmp_path, verify_cache=True, client=client)
    client.downloads.clear()
    local_mtime = dt.datetime(2026, 10, 9, tzinfo=dt.timezone.utc).timestamp()
    os.utime(path, (local_mtime, local_mtime))
    # Both remote versions predate the local copy. Comparing only remote date
    # to local mtime would silently retain a superseded package.
    client.last_modified = "Tue, 06 Oct 2026 00:00:00 GMT"
    client.payload = b"modified package content"
    insider.sync_dera(tmp_path, verify_cache=True, client=client)
    assert client.downloads == [path]
    assert path.read_bytes() == client.payload


def test_dera_without_remote_validators_refetches_before_content_hash_noop(tmp_path):
    client = _DeraHttp(etag=None, last_modified=None)
    (path,) = insider.sync_dera(tmp_path, verify_cache=True, client=client)
    client.downloads.clear()
    insider.sync_dera(tmp_path, verify_cache=True, client=client)
    assert client.downloads == [path]


@pytest.mark.parametrize("content_encoding", [None, "gzip", "x-gzip"])
def test_http_catalogue_decodes_compressed_json_response(monkeypatch, content_encoding):
    expected = {"containers": [{"key": "2003/2003-05.zip", "size": 1024,
                                "updatedAt": "2026-10-08T00:00:00Z"}]}
    raw = json.dumps(expected).encode("utf-8")
    headers = {}
    if content_encoding:
        raw = gzip.compress(raw)
        headers["Content-Encoding"] = content_encoding
    client = insider.HttpClient()
    monkeypatch.setattr(client, "request", lambda url: _DeraResponse(raw, headers))
    assert client.json("https://api.sec-api.io/datasets/form-3-files.json") == expected


def test_dera_listing_handles_compressed_http_response(tmp_path):
    client = _DeraHttp(listing_gzip=True)
    (path,) = insider.sync_dera(tmp_path, verify_cache=True, client=client)
    assert client.downloads == [path]
    assert path.read_bytes() == client.payload


def test_resumed_production_export_reexports_interrupted_files(tmp_path, monkeypatch):
    from scripts import validate_sec_insider_ticker_evidence as validation

    monkeypatch.setattr(validation, "_check_query_window", lambda: None)
    exported = []

    def psql(args, **kwargs):
        query = args[-1]
        if "current_user" in query:
            return SimpleNamespace(returncode=0, stdout="mcp_ro,on,30s\n", stderr="")
        exported.append(query)
        body = ("ticker,first_price\nAAPL,1980-12-31\n" if "first_price" in query
                else "ticker,status,cover_cik\nAAPL,resolved,320193\n")
        return SimpleNamespace(returncode=0, stdout=body, stderr="")

    monkeypatch.setattr(validation.subprocess, "run", psql)
    # An interrupted write: the header and part of a row parse as a shorter CSV.
    (tmp_path / "first_prices.csv").write_bytes(b"ticker,first_price\nAAPL,19")
    (tmp_path / "cover_2007.csv").write_bytes(b"")
    complete = b"ticker,status,cover_cik\nMSFT,resolved,789019\n"
    (tmp_path / "cover_2008.csv").write_bytes(complete)
    validation.export_production("psql", tmp_path)
    assert len(exported) == len(validation.YEARS)  # every file except complete cover_2008
    assert (tmp_path / "first_prices.csv").read_bytes() == b"ticker,first_price\nAAPL,1980-12-31\n"
    assert (tmp_path / "cover_2007.csv").read_bytes() == b"ticker,status,cover_cik\nAAPL,resolved,320193\n"
    assert (tmp_path / "cover_2008.csv").read_bytes() == complete
    assert not list(tmp_path.glob("*.tmp"))
    manifest = json.loads((tmp_path / "production_snapshot.json").read_text(encoding="utf-8"))
    assert manifest["sha256"]["first_prices.csv"] == hashlib.sha256(
        (tmp_path / "first_prices.csv").read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def db():
    dsn = os.environ.get("SEC_INSIDER_TEST_DSN")
    if not dsn:
        pytest.skip("set SEC_INSIDER_TEST_DSN to a disposable local PostgreSQL 16 database")
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql

    schema = "sec_insider_test_" + uuid.uuid4().hex
    conn = psycopg.connect(dsn, autocommit=True)
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
    schema_sql = (ROOT / "schemas" / "sec_insider_ticker_evidence.sql").read_text(encoding="utf-8")
    conn.execute(schema_sql)
    try:
        yield conn
    finally:
        conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
        conn.close()


@pytest.fixture
def clean_db(db):
    # Exercise the loader's real package commit/rollback boundary. An outer
    # transaction would retain its ON COMMIT DROP stage across package loads.
    truncate = (
        "TRUNCATE sec_insider_filings, sec_insider_packages, "
        "sec_insider_package_members, sec_insider_package_facts RESTART IDENTITY"
    )
    db.execute(truncate)
    try:
        yield db
    finally:
        db.execute(truncate)


def _insert(conn, ticker, cik, filed_dates, *, accession_start=1, available_on=None,
            retired_on=None, package="test", source_version="a" * 64, accession_cik=None):
    for offset, filed in enumerate(filed_dates):
        if isinstance(filed, str):
            filed = dt.date.fromisoformat(filed)
        accession = f"{accession_cik or cik:010d}-{filed.year % 100:02d}-{accession_start + offset:06d}"
        fact_hash = hashlib.sha256(f"{ticker}/{accession}/{source_version}".encode()).hexdigest()
        conn.execute(
            "INSERT INTO sec_insider_filings "
            "(fact_hash, accession, cik, raw_symbol, normalized_symbols, form, filed, "
            "available_on, retired_on, loaded_on, source, source_package, source_version) "
            "VALUES (%s, %s, %s, %s, %s, '4', %s, %s, %s, %s, 'dera', %s, %s)",
            (fact_hash, accession, cik, ticker, [ticker], filed,
             available_on or filed + dt.timedelta(days=1), retired_on,
             available_on or filed + dt.timedelta(days=1), package, source_version),
        )


def _resolve(conn, ticker, day):
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SELECT * FROM sec_insider_ticker_issuer_at(%s, %s::date)", (ticker, day))
        return cursor.fetchone()


def test_two_filings_on_two_dates_are_required(clean_db):
    _insert(clean_db, "ONE", 100, ["2010-02-01"])
    _insert(clean_db, "SAME", 101, ["2010-02-01", "2010-02-01"])
    _insert(clean_db, "TWOD", 102, ["2010-02-01", "2010-02-02"])
    one = _resolve(clean_db, "ONE", "2010-12-31")
    same = _resolve(clean_db, "SAME", "2010-12-31")
    admitted = _resolve(clean_db, "TWOD", "2010-12-31")
    assert (one["cik"], one["status"], one["filing_count"], one["distinct_dates"]) == (
        None, "none", 1, 1,
    )
    assert (same["cik"], same["status"], same["filing_count"], same["distinct_dates"]) == (
        None, "none", 2, 1,
    )
    assert (admitted["cik"], admitted["status"], admitted["filing_count"],
            admitted["distinct_dates"]) == (102, "resolved", 2, 2)


def test_three_times_dominance_includes_single_filing_minority(clean_db):
    _insert(clean_db, "DOM", 200, ["2010-02-01", "2010-02-02", "2010-02-03"])
    _insert(clean_db, "DOM", 201, ["2010-03-01"])
    row = _resolve(clean_db, "DOM", "2010-12-31")
    assert row["status"] == "resolved"
    assert row["cik"] == 200
    assert row["filing_count"] == 3
    assert row["runner_up_filings"] == 1
    assert row["candidate_count"] == 2
    assert row["total_filings"] == 4


def test_three_times_is_against_runner_up_not_combined_minority(clean_db):
    _insert(clean_db, "THREE", 210, ["2010-02-01", "2010-02-02", "2010-02-03"])
    _insert(clean_db, "THREE", 211, ["2010-03-01"])
    _insert(clean_db, "THREE", 212, ["2010-03-02"])
    row = _resolve(clean_db, "THREE", "2010-12-31")
    assert (row["status"], row["cik"], row["candidate_count"], row["total_filings"]) == (
        "resolved", 210, 3, 5,
    )


def test_plain_majority_and_latest_filing_do_not_resolve_conflict(clean_db):
    _insert(clean_db, "MAJ", 300, ["2010-02-01", "2010-02-02", "2010-03-01"])
    _insert(clean_db, "MAJ", 301, ["2010-02-03", "2010-04-01"])
    row = _resolve(clean_db, "MAJ", "2010-12-31")
    assert (row["status"], row["cik"]) == ("ambiguous", None)
    assert (row["filing_count"], row["runner_up_filings"], row["total_filings"]) == (3, 2, 5)


def test_conflicting_one_filing_cik_still_blocks_unanimous_admission(clean_db):
    _insert(clean_db, "STRAY", 310, ["2010-02-01", "2010-02-02"])
    _insert(clean_db, "STRAY", 311, ["2010-03-01"])
    row = _resolve(clean_db, "STRAY", "2010-12-31")
    assert (row["status"], row["cik"], row["candidate_count"]) == ("ambiguous", None, 2)


def test_dominant_cik_must_also_meet_distinct_date_admission(clean_db):
    _insert(clean_db, "BATCH", 320, ["2010-02-01"] * 3)
    _insert(clean_db, "BATCH", 321, ["2010-03-01"])
    row = _resolve(clean_db, "BATCH", "2010-12-31")
    assert (row["status"], row["cik"], row["distinct_dates"]) == ("ambiguous", None, 1)


def test_filed_before_d_and_exact_365_day_lower_bound(clean_db):
    day = dt.date(2010, 12, 31)
    start = day - dt.timedelta(days=365)
    _insert(clean_db, "BOUND", 400,
            [start - dt.timedelta(days=1), start, day - dt.timedelta(days=1), day])
    # XML can be known on its filing date. The filed < D fence must still
    # exclude this same-day fact independently of the availability check.
    clean_db.execute(
        "UPDATE sec_insider_filings SET accepted = %s, available_on = %s WHERE filed = %s",
        (dt.datetime(2010, 12, 31, 15, tzinfo=dt.timezone.utc), day, day),
    )
    row = _resolve(clean_db, "BOUND", day)
    assert (row["cik"], row["status"], row["filing_count"], row["distinct_dates"]) == (
        400, "resolved", 2, 2,
    )
    assert row["window_start"] == start
    assert row["window_end"] == day
    tomorrow = _resolve(clean_db, "BOUND", day + dt.timedelta(days=1))
    assert tomorrow["filing_count"] == 2


def test_newly_learned_version_does_not_backfill_past_and_retirement_is_exclusive(clean_db):
    _insert(clean_db, "CORR", 500, ["2010-02-01", "2010-02-02"],
            retired_on=dt.date(2010, 10, 1), source_version="b" * 64)
    _insert(clean_db, "CORR", 501, ["2010-02-01", "2010-02-02"],
            available_on=dt.date(2010, 10, 1), source_version="c" * 64, accession_cik=500)
    before = _resolve(clean_db, "CORR", "2010-09-30")
    after = _resolve(clean_db, "CORR", "2010-10-01")
    assert (before["status"], before["cik"]) == ("resolved", 500)
    assert (after["status"], after["cik"], after["candidate_count"]) == ("resolved", 501, 1)
    assert clean_db.execute("SELECT count(*) FROM sec_insider_filings").fetchone()[0] == 4


def test_no_evidence_returns_counts_and_window(clean_db):
    row = _resolve(clean_db, "NOEXIST", "2010-12-31")
    assert row["status"] == "none"
    assert row["cik"] is None
    assert all(row[name] == 0 for name in (
        "filing_count", "distinct_dates", "candidate_count", "total_filings", "runner_up_filings",
    ))
    assert row["window_start"] == dt.date(2009, 12, 31)
    assert row["window_end"] == dt.date(2010, 12, 31)


def test_match_key_is_separator_free_and_case_insensitive(clean_db):
    _insert(clean_db, "BRK-A", 1067983, ["2010-02-01", "2010-02-02"])
    for ticker in ("BRK-A", "brk.a", "BRK/A"):
        row = _resolve(clean_db, ticker, "2010-12-31")
        assert (row["status"], row["cik"], row["filing_count"]) == ("resolved", 1067983, 2)


def test_newer_accession_content_supersedes_an_older_package_carrier(clean_db):
    _insert(clean_db, "OLD", 600, ["2010-02-01", "2010-02-02"],
            package="old-carrier", source_version="d" * 64)
    _insert(clean_db, "NEW", 600, ["2010-02-01", "2010-02-02"],
            package="new-carrier", available_on=dt.date(2010, 10, 1), source_version="e" * 64)
    assert _resolve(clean_db, "OLD", "2010-09-30")["status"] == "resolved"
    assert _resolve(clean_db, "NEW", "2010-09-30")["status"] == "none"
    assert _resolve(clean_db, "OLD", "2010-10-01")["status"] == "none"
    assert _resolve(clean_db, "NEW", "2010-10-01")["status"] == "resolved"


def _table_snapshot(conn):
    return {
        name: conn.execute(f"SELECT to_jsonb(t) FROM {name} t ORDER BY t.id").fetchall()
        for name in ("sec_insider_filings", "sec_insider_package_members", "sec_insider_package_facts")
    } | {
        "sec_insider_packages": conn.execute(
            "SELECT to_jsonb(t) FROM sec_insider_packages t ORDER BY source_package"
        ).fetchall(),
    }


def test_incremental_package_keeps_validators_and_does_not_add_versions(clean_db, tmp_path):
    package = _dera_zip(tmp_path)
    insider.load_package(
        clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 8),
        validators={"etag": '"fixture-v1"', "last_modified": "Thu, 08 Oct 2026 00:00:00 GMT"},
    )
    before = _table_snapshot(clean_db)
    insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 9))
    assert _table_snapshot(clean_db) == before
    row = clean_db.execute(
        "SELECT filings, remote_etag, remote_last_modified FROM sec_insider_packages"
    ).fetchone()
    assert row == (8, '"fixture-v1"', "Thu, 08 Oct 2026 00:00:00 GMT")
    assert clean_db.execute("SELECT count(*) FROM sec_insider_filings").fetchone()[0] == 8


def test_republished_package_preserves_retired_facts_and_correction_knowledge(clean_db, tmp_path):
    package = _dera_zip(tmp_path)
    insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 8))
    rows = [row for row in _real_dera_rows() if row["ACCESSION_NUMBER"] in DERA_SYMBOLS
            and row["FILING_DATE"].endswith("2026") and row["ACCESSION_NUMBER"] not in (
                "0001327318-26-000010", "0001193125-26-106222",
            )]
    assert len(rows) == 8
    rows = [row for row in rows if row["ACCESSION_NUMBER"] != "0000099780-26-000121"]
    for row in rows:
        if row["ACCESSION_NUMBER"] == "0001225208-26-007725":
            row["ISSUERTRADINGSYMBOL"] = "NEW"
    _dera_zip(tmp_path, replacement_rows=rows)
    insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 9))
    versions = clean_db.execute(
        "SELECT normalized_symbols, available_on, retired_on FROM sec_insider_filings "
        "WHERE accession = '0001225208-26-007725' ORDER BY id"
    ).fetchall()
    assert versions == [
        (["SIRI"], dt.date(2026, 9, 11), dt.date(2026, 10, 9)),
        (["NEW"], dt.date(2026, 10, 9), None),
    ]
    assert clean_db.execute(
        "SELECT retired_on FROM sec_insider_filings WHERE accession = '0000099780-26-000121'"
    ).fetchone() == (dt.date(2026, 10, 9),)
    assert clean_db.execute("SELECT count(*) FROM sec_insider_filings").fetchone()[0] == 9
    # One SIRI filing remains insufficient for admission but proves the old fact
    # remains historically queryable until the correction's knowledge date.
    assert _resolve(clean_db, "SIRI", "2026-10-08")["filing_count"] == 1
    assert _resolve(clean_db, "SIRI", "2026-10-09")["filing_count"] == 0
    assert _resolve(clean_db, "NEW", "2026-10-08")["filing_count"] == 0
    assert _resolve(clean_db, "NEW", "2026-10-09")["filing_count"] == 1


def test_bad_republication_cannot_retire_last_good_package(clean_db, tmp_path):
    package = _dera_zip(tmp_path)
    insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 8))
    before = _table_snapshot(clean_db)
    with zipfile.ZipFile(package) as archive:
        rows = list(csv.DictReader(io.StringIO(archive.read("SUBMISSION.tsv").decode()), delimiter="\t"))
    rows[0]["ISSUERTRADINGSYMBOL"] = "CHANGED"
    rows[-1]["ACCESSION_NUMBER"] = "invalid-accession"
    _dera_zip(tmp_path, replacement_rows=rows)
    with pytest.raises((ValueError, RuntimeError)):
        insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 9))
    assert _table_snapshot(clean_db) == before


def test_metadata_only_accession_learned_later_is_dated_by_reconciliation(clean_db, tmp_path):
    known, late = "0000700565-03-000110", "0001181431-03-009430"
    package = tmp_path / "form-4-files" / "2003" / "2003-05.zip"
    package.parent.mkdir(parents=True)

    def publish(late_has_xml):
        with zipfile.ZipFile(package, "w") as archive:
            for accession in (known, late):
                fixture = FIXTURE / "secapi" / accession
                folder = f"2003-05/{accession.replace('-', '')}"
                archive.write(fixture / "metadata.json", f"{folder}/metadata.json")
                if accession == known or late_has_xml:
                    archive.write(fixture / "ownership.xml", f"{folder}/ownership.xml")
                else:  # metadata with only an HTML rendering, no ownership XML
                    archive.writestr(f"{folder}/rendering.xml", b"<html>rendering</html>")

    publish(late_has_xml=False)
    first = insider.load_package(clean_db, package, reconciled_on=dt.date(2026, 10, 8))
    assert (first["filings"], first["non_xml_filings"]) == (1, 1)
    publish(late_has_xml=True)
    insider.load_package(clean_db, package, reconciled_on=dt.date(2026, 10, 9))
    available = dict(clean_db.execute("SELECT accession, available_on FROM sec_insider_filings").fetchall())
    # The republished XML is new knowledge, not a fact backdated to the 2003 filing.
    assert available == {known: dt.date(2003, 5, 30), late: dt.date(2026, 10, 9)}


def test_v4_reparse_retires_and_redates_only_changed_readings(clean_db, tmp_path, monkeypatch):
    package = _dera_zip(tmp_path, "2026q1")  # TrueCar's TRUE and Watsco's WSO; WSOB
    unchanged = "SELECT to_jsonb(f) FROM sec_insider_filings f WHERE accession = '0001193125-26-106222'"
    v4 = insider.normalize_symbols
    with monkeypatch.context() as v3:
        v3.setattr(insider, "PARSER_VERSION", "sec_insider_v3")
        v3.setattr(insider, "normalize_symbols",
                   lambda raw: [] if insider._key(raw) in ("TRUE", "OB") else v4(raw))
        insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 9))
    before = clean_db.execute(unchanged).fetchall()
    result = insider.load_package(clean_db, package, source="dera", reconciled_on=dt.date(2026, 10, 10))
    assert (result["inserted"], result["retired"]) == (1, 1)
    assert clean_db.execute(
        "SELECT normalized_symbols, available_on, retired_on FROM sec_insider_filings "
        "WHERE accession = '0001327318-26-000010' ORDER BY id"
    ).fetchall() == [
        ([], dt.date(2026, 1, 24), dt.date(2026, 10, 10)),
        # The new reading is new knowledge, dated by reconciliation, not 2026-01-24.
        (["TRUE"], dt.date(2026, 10, 10), None),
    ]
    # An unchanged reading keeps its row and its original availability.
    assert clean_db.execute(unchanged).fetchall() == before
    assert clean_db.execute(
        "SELECT count(*) FILTER (WHERE retired_on IS NULL), count(*) FROM sec_insider_package_members"
    ).fetchone() == (2, 2)
    assert clean_db.execute("SELECT parser_version FROM sec_insider_packages").fetchone() == ("sec_insider_v4",)
