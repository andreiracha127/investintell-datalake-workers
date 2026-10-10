"""Transport changes retain W1's exact raw bytes and parsed fixture facts."""

import io
import http.client
import json
import time
import urllib.error
import zipfile
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from scripts import load_sec_insider_filings as insider
from scripts import load_sec_ticker_cik_history as loader


FIXTURES = Path(__file__).parent / "fixtures" / "sec_ticker_cik_history"
ADSH = "0000876661-13-000657"


class Scheduler:
    def __init__(self):
        self.urls = []
        self.cooldowns = []

    def acquire(self, url, product=None):
        self.urls.append(str(url))

    def cooldown(self, url, retry_after=None, product=None, **kwargs):
        self.cooldowns.append((str(url), retry_after))

    def sleep(self, seconds):
        pass


class Body(io.BytesIO):
    def __init__(self, raw, headers=None):
        super().__init__(raw)
        self.headers = headers or {"Content-Length": str(len(raw))}


def test_mirror_preserves_canonical_cache_original_bytes_and_event_facts(tmp_path, monkeypatch):
    raw = (FIXTURES / "filings" / f"{ADSH}.txt").read_bytes()
    requests = []
    scheduler = Scheduler()

    def open_url(request, timeout):
        requests.append(request)
        return Body(raw)

    monkeypatch.setattr(insider.urllib.request, "build_opener", lambda *args: SimpleNamespace(open=open_url))
    client = SimpleNamespace(get=lambda url: pytest.fail("successful mirror must avoid government fallback"))
    documents = loader.EventDocuments(tmp_path / "docs", client, api_key="fixture-key", scheduler=scheduler)
    event = loader.RegistrationEvent(ADSH, 5133, "25-NSE", loader.dt.date(2013, 8, 12), "q")
    actual = loader.describe_events([event], documents, {5133})
    expected = loader.describe_events([event], loader.EventDocuments(FIXTURES / "filings", None), {5133})
    assert actual == expected
    assert (tmp_path / "docs" / f"{ADSH}.txt").read_bytes() == raw
    assert documents.fetched == 1
    assert requests[0].full_url.startswith("https://edgar-mirror.sec-api.io/")
    assert "fixture-key" not in requests[0].full_url
    assert requests[0].get_header("Authorization") == "fixture-key"
    assert scheduler.urls == [requests[0].full_url]


@pytest.mark.parametrize("failure", ["auth", "maintenance", "truncated", "incomplete-read"])
def test_paid_failure_falls_back_once_with_separate_government_permit(tmp_path, monkeypatch, failure):
    raw = (FIXTURES / "filings" / f"{ADSH}.txt").read_bytes()
    scheduler = Scheduler()
    requests = []

    def open_url(request, timeout):
        requests.append(request)
        if failure == "auth":
            raise urllib.error.HTTPError(request.full_url, 403, "fixture-key", {}, None)
        if failure == "maintenance":
            return Body(b"<html>maintenance</html>")
        if failure == "incomplete-read":
            class BrokenBody(Body):
                def read(self, *args):
                    raise http.client.IncompleteRead(b"partial", len(raw))
            return BrokenBody(raw)
        return Body(raw, {"Content-Length": str(len(raw) + 1)})

    monkeypatch.setattr(insider.urllib.request, "build_opener", lambda *args: SimpleNamespace(open=open_url))
    fallback = []

    def government(url):
        fallback.append(url)
        return SimpleNamespace(status_code=200, content=raw, headers={})

    documents = loader.EventDocuments(tmp_path, SimpleNamespace(get=government), api_key="fixture-key",
                                     scheduler=scheduler, spacing=0)
    assert documents.text(5133, ADSH) == raw.decode("latin-1")
    assert len(requests) == len(fallback) == 1
    assert scheduler.urls == [requests[0].full_url, fallback[0]]
    assert (tmp_path / f"{ADSH}.txt").read_bytes() == raw


def test_government_httpx_redirects_and_429_share_scheduler(monkeypatch):
    scheduler = Scheduler()

    def respond(request):
        if request.url.host == "www.sec.gov":
            return httpx.Response(302, headers={"Location": "https://archives.sec.gov/file.txt"})
        return httpx.Response(429, headers={"Retry-After": "7"})

    real_client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: real_client(transport=httpx.MockTransport(respond), **kwargs))
    with loader.sec_client(scheduler=scheduler) as client:
        assert client.get("https://www.sec.gov/file.txt").status_code == 429
    assert scheduler.urls == ["https://www.sec.gov/file.txt", "https://archives.sec.gov/file.txt"]
    assert scheduler.cooldowns == [("https://archives.sec.gov/file.txt", "7")]


def test_long_retry_after_is_shared_and_bounded_without_local_sleep(tmp_path, monkeypatch):
    sleeps = []
    monkeypatch.setattr(loader.time, "sleep", sleeps.append)
    requests = []

    class BoundedScheduler(Scheduler):
        def acquire(self, url, product=None):
            super().acquire(url, product)
            if self.cooldowns:
                raise RuntimeError("bounded wait exhausted fixture-sensitive-key")

    scheduler = BoundedScheduler()

    def government(url):
        requests.append(url)
        return SimpleNamespace(status_code=429, content=b"", headers={"retry-after": "86400"})

    documents = loader.EventDocuments(tmp_path, SimpleNamespace(get=government), api_key="",
                                     scheduler=scheduler, spacing=0)
    with pytest.raises(RuntimeError, match="SEC provider budget unavailable") as error:
        documents.text(5133, ADSH)
    assert "fixture-sensitive-key" not in str(error.value)
    assert scheduler.cooldowns == [(requests[0], "86400")]
    assert scheduler.urls == [requests[0]] * 2
    assert len(requests) == 1  # no request bypasses the shared cooldown
    assert sleeps == []
    assert (documents.fetched, documents.failed, documents.rejected) == (0, 1, 0)
    assert not (tmp_path / f"{ADSH}.txt").exists()


def test_5xx_remote_retry_after_cannot_extend_bounded_backoff(tmp_path, monkeypatch):
    sleeps = []
    monkeypatch.setattr(loader.time, "sleep", sleeps.append)
    raw = (FIXTURES / "filings" / f"{ADSH}.txt").read_bytes()
    responses = iter([SimpleNamespace(status_code=503, content=b"", headers={"retry-after": "86400"}),
                      SimpleNamespace(status_code=200, content=raw, headers={})])
    scheduler = Scheduler()
    documents = loader.EventDocuments(tmp_path, SimpleNamespace(get=lambda url: next(responses)),
                                     api_key="", scheduler=scheduler, spacing=0)
    assert documents.text(5133, ADSH) == raw.decode("latin-1")
    assert sleeps == [1.0]
    assert scheduler.cooldowns == []
    assert len(scheduler.urls) == 2


def test_single_and_multiple_download_workers_reproduce_fixture_bytes_and_exact_facts(tmp_path, monkeypatch, capsys):
    packages = {}
    for name, fixture in [("2026_08_notes.zip", "fsn_2026_08_voya"),
                          ("2026_09_notes.zip", "fsn_2026_09_citi")]:
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for member in sorted((FIXTURES / fixture).iterdir()):
                archive.writestr(member.name, member.read_bytes())
        packages[name] = output.getvalue()
    urls = ["https://www.sec.gov/files/" + name for name in packages]

    class Response:
        def __init__(self, raw):
            self.raw = raw
            self.headers = {"content-length": str(len(raw))}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        def iter_bytes(self, size):
            yield self.raw

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def stream(self, method, url):
            if "2026_08" in url:
                time.sleep(0.03)  # deliberately reverse completion order
            return Response(packages[url.rsplit("/", 1)[1]])

    monkeypatch.setattr(loader, "sec_client", Client)
    monkeypatch.setattr(loader, "list_package_urls", lambda client: urls)
    monkeypatch.setattr(loader.os, "cpu_count", lambda: 8)
    snapshots = []
    logs = []
    for workers in (1, 2):
        paths = loader.download_packages(tmp_path / str(workers), workers=workers)
        assert [path.name for path in paths] == list(packages)
        assert [path.read_bytes() for path in paths] == list(packages.values())
        facts = []
        for path in paths:
            result = asdict(loader.parse_package(path))
            result.pop("parse_seconds")  # runtime telemetry is not parser evidence
            facts.append(result)
        snapshots.append(json.dumps(facts, default=str, sort_keys=True))
        logs.append(capsys.readouterr().out)
    assert snapshots[0] == snapshots[1]
    assert logs[0] == logs[1]


def test_short_fsn_stream_preserves_last_good_target(tmp_path):
    target = tmp_path / "2026_09_notes.zip"
    target.write_bytes(b"last-good")

    class Response:
        headers = {"content-length": "999"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def raise_for_status(self):
            pass

        def iter_bytes(self, size):
            yield b"short"

    with pytest.raises(ValueError, match="short download"):
        loader.fetch_package(SimpleNamespace(stream=lambda *args: Response()), "https://www.sec.gov/a.zip", target)
    assert target.read_bytes() == b"last-good"
    assert list(tmp_path.iterdir()) == [target]
