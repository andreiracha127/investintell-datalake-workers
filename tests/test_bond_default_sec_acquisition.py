"""SEC acquisition: identified requests, shared <=4 req/s budget, 403/429 rules, cache, headers."""

from __future__ import annotations

import datetime as dt
import itertools
import threading
import time
from pathlib import Path

import pytest

from src.bonds.default_events import sec_acquisition as sa

FIXTURES = Path(__file__).parent / "fixtures" / "bond_default_events" / "edgar"
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.lock = threading.Lock()

    def clock(self) -> float:
        with self.lock:
            return self.now

    def sleep(self, seconds: float) -> None:
        with self.lock:
            self.now += max(0.0, seconds)


class FakeTransport:
    def __init__(self, responses: list[sa.HttpResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, str]]] = []

    def __call__(self, url: str, headers: dict[str, str], max_bytes: int) -> sa.HttpResponse:
        self.calls.append((url, dict(headers)))
        item = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(item, Exception):
            raise item
        return item


def ok(body: bytes = b"hello", url: str = "https://www.sec.gov/x") -> sa.HttpResponse:
    return sa.HttpResponse(200, {"content-type": "text/plain"}, body, url)


def status(code: int, **headers: str) -> sa.HttpResponse:
    return sa.HttpResponse(code, {k.replace("_", "-"): v for k, v in headers.items()}, b"", "https://www.sec.gov/x")


def client(tmp_path: Path, transport: FakeTransport, clock: FakeClock | None = None) -> sa.SecClient:
    clock = clock or FakeClock()
    budget = sa.RequestBudget(4, clock=clock.clock, sleep=clock.sleep)
    return sa.SecClient(tmp_path / "cache", transport=transport, budget=budget, sleep=clock.sleep, now=lambda: NOW)


def max_starts_in_window(starts: list[float], window: float = 1.0) -> int:
    ordered = sorted(starts)
    best = 0
    for i, first in enumerate(ordered):
        best = max(best, sum(1 for s in ordered[i:] if s < first + window))
    return best


def test_budget_spacing_never_exceeds_four_per_second() -> None:
    clock = FakeClock()
    budget = sa.RequestBudget(4, clock=clock.clock, sleep=clock.sleep)
    for _ in range(13):
        budget.acquire()
    assert max_starts_in_window(list(budget.starts)) <= 4
    assert all(b - a >= 0.25 - 1e-9 for a, b in zip(sorted(budget.starts), sorted(budget.starts)[1:]))


class StampingTransport:
    """Records the clock at each transport invocation (the real request start)."""

    def __init__(self, clock, responses: list[sa.HttpResponse] | None = None, latency: float = 0.0) -> None:
        self.clock = clock
        self.lock = threading.Lock()
        self.stamps: list[tuple[float, str]] = []
        self.received: list[tuple[float, int]] = []
        self.responses = responses or []
        self.latency = latency

    def __call__(self, url: str, headers: dict[str, str], max_bytes: int) -> sa.HttpResponse:
        with self.lock:
            self.stamps.append((self.clock(), url))
            response = self.responses.pop(0) if self.responses else ok()
        if self.latency:
            time.sleep(self.latency)  # response arrives while other callers are already waiting
        with self.lock:
            self.received.append((self.clock(), response.status))
        return response


def test_budget_grants_actual_starts_after_delayed_wakeups(tmp_path: Path) -> None:
    """A thread that wakes late must recheck: grants follow actual time, not reserved slots."""
    clock = FakeClock()
    oversleep = [0.4, 0.0, 0.0, 0.3, 0.0, 0.0]

    def late_sleep(seconds: float) -> None:
        clock.sleep(seconds + (oversleep.pop(0) if oversleep else 0.0))

    budget = sa.RequestBudget(4, clock=clock.clock, sleep=late_sleep)
    transport = StampingTransport(clock.clock)
    sec = sa.SecClient(tmp_path / "cache", transport=transport, budget=budget, sleep=late_sleep, now=lambda: NOW,
                       lock_cache_dir=False)
    for n in range(8):
        sec.get(f"https://www.sec.gov/{n}", use_cache=False)
    stamps = [s for s, _ in transport.stamps]
    assert all(b - a >= 0.25 - 1e-9 for a, b in itertools.pairwise(stamps))
    assert max_starts_in_window(stamps) <= 4


def test_threads_with_jittered_wakeups_never_exceed_budget(tmp_path: Path) -> None:
    import random

    rng = random.Random(7)
    budget = sa.RequestBudget(4, sleep=lambda s: time.sleep(s + rng.uniform(0, 0.12)))
    transport = StampingTransport(time.monotonic)
    sec = sa.SecClient(tmp_path / "c", transport=transport, budget=budget, now=lambda: NOW)

    def work(index: int) -> None:
        for n in range(3):
            sec.get(f"https://www.sec.gov/j{index}/{n}", use_cache=False)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stamps = sorted(s for s, _ in transport.stamps)
    assert len(stamps) == 12
    assert all(b - a >= 0.25 - 0.002 for a, b in itertools.pairwise(stamps)), stamps
    assert max_starts_in_window(stamps, 1.0 - 0.005) <= 4


def test_retry_after_defers_pending_waiters(tmp_path: Path) -> None:
    budget = sa.RequestBudget(4)
    transport = StampingTransport(time.monotonic, [status(429, retry_after="1")], latency=0.35)
    sec = sa.SecClient(tmp_path / "c", transport=transport, budget=budget, now=lambda: NOW)
    threads = [threading.Thread(target=sec.get, args=(f"https://www.sec.gov/r{i}",), kwargs={"use_cache": False})
               for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stamps = sorted(s for s, _ in transport.stamps)
    assert len(stamps) == 5  # one 429 then 4 successes
    received_429 = next(t for t, code in transport.received if code == 429)
    # Starts already in flight before the 429 arrived are legitimate; after its receipt no
    # waiting caller may start until Retry-After has elapsed.
    blocked = [s for s in stamps if received_429 < s < received_429 + 1.0 - 0.002]
    assert not blocked, (received_429, stamps)
    assert all(b - a >= 0.25 - 0.002 for a, b in itertools.pairwise(stamps))


def test_cache_directory_is_single_process(tmp_path: Path) -> None:
    import subprocess
    import sys

    cache = tmp_path / "shared"
    root = Path(__file__).resolve().parents[1]
    script = (
        "import sys\n"
        f"sys.path.insert(0, {str(root)!r})\n"
        "from pathlib import Path\n"
        "from src.bonds.default_events import sec_acquisition as sa\n"
        "try:\n"
        "    sa.SecClient(Path(sys.argv[1]), transport=lambda *a: None, budget=sa.RequestBudget(4))\n"
        "except sa.SecCacheLocked as exc:\n"
        "    print('LOCKED', exc)\n"
        "else:\n"
        "    print('ACQUIRED')\n"
    )
    with sa.SecClient(cache, transport=FakeTransport([ok()]), budget=sa.RequestBudget(4)) as holder:
        same_process = sa.SecClient(cache, transport=FakeTransport([ok()]), budget=sa.RequestBudget(4))
        same_process.close()
        done = subprocess.run([sys.executable, "-c", script, str(cache)], capture_output=True, text=True,
                              timeout=120, check=False)
        assert done.returncode == 0, done.stderr[-1500:]
        assert done.stdout.startswith("LOCKED"), done.stdout
        assert holder.cache_lock is not None
    after = subprocess.run([sys.executable, "-c", script, str(cache)], capture_output=True, text=True,
                           timeout=120, check=False)
    assert after.stdout.startswith("ACQUIRED"), after.stdout + after.stderr[-500:]


def test_budget_rejects_more_than_four_per_second() -> None:
    with pytest.raises(ValueError):
        sa.RequestBudget(5)


def test_two_clients_and_threads_share_one_budget(tmp_path: Path) -> None:
    budget = sa.RequestBudget(4)
    transports = [FakeTransport([ok()]), FakeTransport([ok()])]
    clients = [
        sa.SecClient(tmp_path / f"c{i}", transport=t, budget=budget, now=lambda: NOW) for i, t in enumerate(transports)
    ]

    def work(index: int) -> None:
        for n in range(3):
            clients[index % 2].get(f"https://www.sec.gov/t{index}/{n}", use_cache=False)

    threads = [threading.Thread(target=work, args=(i,)) for i in range(3)]
    started = time.monotonic()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(budget.starts) == 9
    assert max_starts_in_window(list(budget.starts)) <= 4
    assert time.monotonic() - started >= 1.9


def test_default_clients_use_the_process_wide_budget(tmp_path: Path) -> None:
    a = sa.SecClient(tmp_path / "a", transport=FakeTransport([ok()]))
    b = sa.SecClient(tmp_path / "b", transport=FakeTransport([ok()]))
    assert a.budget is b.budget is sa.shared_budget()


def test_identified_user_agent_and_non_sec_urls_refused(tmp_path: Path) -> None:
    transport = FakeTransport([ok()])
    sec = client(tmp_path, transport)
    sec.get("https://www.sec.gov/Archives/edgar/x")
    assert transport.calls[0][1]["User-Agent"] == "InvestIntell research admin@investintell.com"
    with pytest.raises(sa.SecResponseError):
        sec.get("https://example.com/x")
    with pytest.raises(sa.SecResponseError):
        sec.get("http://www.sec.gov/x")
    with pytest.raises(ValueError):
        sa.SecClient(tmp_path / "x", transport=transport, user_agent="anonymous")


def test_403_stops_every_further_request(tmp_path: Path) -> None:
    transport = FakeTransport([status(403), ok()])
    sec = client(tmp_path, transport)
    with pytest.raises(sa.SecAccessDenied):
        sec.get("https://www.sec.gov/a")
    with pytest.raises(sa.SecAccessDenied):
        sec.get("https://www.sec.gov/b")
    assert len(transport.calls) == 1


def test_429_honours_retry_after_then_succeeds(tmp_path: Path) -> None:
    clock = FakeClock()
    transport = FakeTransport([status(429, retry_after="7"), status(429, retry_after="3"), ok()])
    sec = client(tmp_path, transport, clock)
    result = sec.get("https://www.sec.gov/a")
    assert result.body == b"hello"
    starts = sorted(sec.budget.starts)
    assert starts[1] - starts[0] >= 7 and starts[2] - starts[1] >= 3


def test_429_http_date_retry_after() -> None:
    assert sa.parse_retry_after("Fri, 25 Sep 2026 12:00:30 GMT", NOW) == 30.0
    assert sa.parse_retry_after("garbage", NOW) is None


def test_429_at_most_three_retries(tmp_path: Path) -> None:
    transport = FakeTransport([status(429, retry_after="1")])
    sec = client(tmp_path, transport)
    with pytest.raises(sa.SecRateLimited):
        sec.get("https://www.sec.gov/a")
    assert len(transport.calls) == 4


def test_429_unbounded_retry_after_stops(tmp_path: Path) -> None:
    transport = FakeTransport([status(429, retry_after="3600")])
    sec = client(tmp_path, transport)
    with pytest.raises(sa.SecRateLimited):
        sec.get("https://www.sec.gov/a")
    assert len(transport.calls) == 1


def test_transient_5xx_and_transport_failures_are_bounded(tmp_path: Path) -> None:
    transport = FakeTransport([status(503), sa.TransportFailure("reset"), ok()])
    assert client(tmp_path, transport).get("https://www.sec.gov/a").body == b"hello"
    failing = FakeTransport([status(503)])
    with pytest.raises(sa.SecResponseError):
        client(tmp_path / "f", failing).get("https://www.sec.gov/a")
    assert len(failing.calls) == 4


def test_redirects_only_within_sec(tmp_path: Path) -> None:
    transport = FakeTransport([sa.HttpResponse(301, {"location": "/b"}, b"", "x"), ok()])
    assert client(tmp_path, transport).get("https://www.sec.gov/a").body == b"hello"
    assert transport.calls[1][0] == "https://www.sec.gov/b"
    evil = FakeTransport([sa.HttpResponse(302, {"location": "https://evil.example/x"}, b"", "x")])
    with pytest.raises(sa.SecResponseError):
        client(tmp_path / "e", evil).get("https://www.sec.gov/a")


def test_content_addressed_cache(tmp_path: Path) -> None:
    transport = FakeTransport([ok(b"payload")])
    sec = client(tmp_path, transport)
    first = sec.get("https://www.sec.gov/a")
    second = sec.get("https://www.sec.gov/a")
    assert not first.from_cache and second.from_cache
    assert first.sha256 == second.sha256 and len(transport.calls) == 1
    obj = tmp_path / "cache" / "objects" / first.sha256[:2] / first.sha256
    assert obj.read_bytes() == b"payload"
    obj.write_bytes(b"tampered")
    with pytest.raises(sa.SecResponseError):
        sec.get("https://www.sec.gov/a")


def test_oversized_body_refused(tmp_path: Path) -> None:
    transport = FakeTransport([ok(b"x" * 100)])
    sec = sa.SecClient(tmp_path, transport=transport, budget=sa.RequestBudget(4), max_bytes=10, now=lambda: NOW)
    with pytest.raises(sa.SecResponseError):
        sec.get("https://www.sec.gov/a")


# --- acceptance headers ----------------------------------------------------------
def header_doc(accession: str, acceptance: str, *, form: str = "NPORT-P", cik: str = "0000917469",
               extra: str = "") -> bytes:
    return (
        f"<HTML><!--\n<SEC-HEADER>{accession}.hdr.sgml : 20260529\n<ACCEPTANCE-DATETIME>{acceptance}\n"
        f"<ACCESSION-NUMBER>{accession}\n<TYPE>{form}\n<PERIOD>20260331\n{extra}<FILING-DATE>20260529\n"
        f"<FILER>\n<COMPANY-DATA>\n<CIK>{cik}\n</COMPANY-DATA>\n</FILER>\n</SEC-HEADER>\n--></HTML>"
    ).encode()


def parse(doc: bytes, accession: str) -> sa.AcceptanceHeader:
    return sa.parse_acceptance_header(doc, accession_number=accession, url="u", document_sha256="0" * 64,
                                      retrieved_at=NOW)


def test_real_stage1a_nport_headers_eastern_to_utc() -> None:
    winter = parse((FIXTURES / "0001752724-21-029444-index-headers.html").read_bytes(), "0001752724-21-029444")
    assert winter.acceptance_raw == "20210219114416"
    assert winter.acceptance_at == dt.datetime(2021, 2, 19, 16, 44, 16, tzinfo=UTC)  # EST, UTC-5
    assert winter.submission_type == "NPORT-P" and winter.is_public_nport
    assert winter.filer_ciks == ("0001302624",)
    summer = parse((FIXTURES / "0001410368-26-056143-index-headers.html").read_bytes(), "0001410368-26-056143")
    assert summer.acceptance_at == dt.datetime(2026, 5, 29, 17, 28, 33, tzinfo=UTC)  # EDT, UTC-4
    assert summer.header_text.startswith("<SEC-HEADER>") and summer.header_text.endswith("</SEC-HEADER>")
    assert len(summer.header_sha256) == 64


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("20210314015959", dt.datetime(2021, 3, 14, 6, 59, 59, tzinfo=UTC)),
        ("20210314030000", dt.datetime(2021, 3, 14, 7, 0, 0, tzinfo=UTC)),
        ("20211107013000", dt.datetime(2021, 11, 7, 6, 30, 0, tzinfo=UTC)),  # ambiguous -> later
        ("20211107020000", dt.datetime(2021, 11, 7, 7, 0, 0, tzinfo=UTC)),
    ],
)
def test_acceptance_dst_boundaries(raw: str, expected: dt.datetime) -> None:
    accession = "0000000001-21-000001"
    assert parse(header_doc(accession, raw), accession).acceptance_at == expected


def test_nonexistent_spring_forward_time_rejected() -> None:
    accession = "0000000001-21-000001"
    with pytest.raises(sa.SecHeaderError):
        parse(header_doc(accession, "20210314023000"), accession)


def test_header_accession_mismatch_and_duplicate_acceptance_rejected() -> None:
    with pytest.raises(sa.SecHeaderError):
        parse(header_doc("0000000001-21-000001", "20210219114416"), "0000000001-21-000002")
    doc = header_doc("0000000001-21-000001", "20210219114416", extra="<ACCEPTANCE-DATETIME>20210219114417\n")
    with pytest.raises(sa.SecHeaderError):
        parse(doc, "0000000001-21-000001")
    with pytest.raises(sa.SecHeaderError):
        parse(b"<html>no header</html>", "0000000001-21-000001")


def test_nonpublic_submission_type_is_not_public_nport() -> None:
    accession = "0000000001-21-000001"
    header = parse(header_doc(accession, "20210219114416", form="NPORT-NP"), accession)
    assert not header.is_public_nport


def test_items_are_kept_as_tokens() -> None:
    accession = "0000000001-23-000001"
    doc = header_doc(accession, "20230110165959", form="8-K", extra="<ITEMS>1.03\n<ITEMS>2.04\n")
    assert parse(doc, accession).items == ("1.03", "2.04")


def test_header_store_fetches_once_per_accession(tmp_path: Path) -> None:
    accession = "0001410368-26-056143"
    body = (FIXTURES / f"{accession}-index-headers.html").read_bytes()
    transport = FakeTransport([ok(body)])
    store = sa.AcceptanceHeaderStore(client(tmp_path, transport), tmp_path / "headers")
    first = store.get(accession, "917469")
    second = store.get(accession, "0000917469")
    assert first == second and len(transport.calls) == 1
    assert transport.calls[0][0] == (
        "https://www.sec.gov/Archives/edgar/data/917469/000141036826056143/0001410368-26-056143-index-headers.html"
    )
    fresh = sa.AcceptanceHeaderStore(client(tmp_path / "other", FakeTransport([status(500)])), tmp_path / "headers")
    assert fresh.get(accession, "917469") == first
    assert sa.AcceptanceHeader.from_record(first.to_record()) == first


def test_header_url_validation() -> None:
    with pytest.raises(sa.SecHeaderError):
        sa.accession_header_url("917469", "bad")
    with pytest.raises(sa.SecHeaderError):
        sa.accession_header_url("0", "0001410368-26-056143")


# --- indexes ---------------------------------------------------------------------
INDEX = (
    "Description:           Master Index of EDGAR Dissemination Feed by Form Type\n"
    "Last Data Received:    March 31, 2023\n\n"
    "Form Type   Company Name                                                  CIK         Date Filed  File Name\n"
    + "-" * 141 + "\n"
    "8-K              ACME CORP                                                     1084869     2023-02-02  "
    "edgar/data/1084869/0001157523-23-000149.txt         \n"
    "8-K/A            ACME  HOLDINGS  INC                                           22          2023-02-03  "
    "edgar/data/22/0000000022-23-000001.txt\n"
    "SC 13D           SOMEONE LLC                                                   33          2023-02-04  "
    "edgar/data/33/0000000033-23-000001.txt\n"
)


def test_form_index_parsing() -> None:
    entries = sa.parse_form_index(INDEX.encode("latin-1"))
    assert [e.form_type for e in entries] == ["8-K", "8-K/A", "SC 13D"]
    assert entries[0].cik == "0001084869" and entries[0].accession_number == "0001157523-23-000149"
    assert entries[1].company_name == "ACME  HOLDINGS  INC"
    assert [e.form_type for e in sa.parse_form_index(INDEX.encode(), forms=("8-K",))] == ["8-K"]
    with pytest.raises(sa.SecIndexError):
        sa.parse_form_index((INDEX + "garbage line without columns\n").encode())
    with pytest.raises(sa.SecIndexError):
        sa.parse_form_index(b"no separator here\n")


def test_date_only_index_evidence_is_next_eastern_day_boundary() -> None:
    entry = sa.parse_form_index(INDEX.encode())[0]
    assert sa.index_date_public_available_at(entry) == dt.datetime(2023, 2, 3, 5, 0, tzinfo=UTC)
    summer = sa.FormIndexEntry("8-K", "X", "0000000001", dt.date(2023, 7, 3), "f", "0000000001-23-000001")
    assert sa.index_date_public_available_at(summer) == dt.datetime(2023, 7, 4, 4, 0, tzinfo=UTC)
    dst_eve = sa.FormIndexEntry("8-K", "X", "0000000001", dt.date(2023, 3, 11), "f", "0000000001-23-000002")
    assert sa.index_date_public_available_at(dst_eve) == dt.datetime(2023, 3, 12, 5, 0, tzinfo=UTC)


def test_index_urls() -> None:
    assert sa.full_index_url(2023, 1) == "https://www.sec.gov/Archives/edgar/full-index/2023/QTR1/form.idx"
    assert sa.daily_index_url(dt.date(2026, 5, 29)).endswith("/daily-index/2026/QTR2/form.20260529.idx")
    with pytest.raises(sa.SecIndexError):
        sa.full_index_url(2023, 5)
