"""No provider calls: spawned independent schedulers and local HTTP fixtures."""
import multiprocessing
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from scripts.sec_provider_transport import (
    ProviderAuthError, ProviderScheduler, ProviderTransport, _GovernedRedirect, filing_download_url,
    safe_url,
)


def _reserve_child(path, url, queue):
    try:
        scheduler = ProviderScheduler(path, {"government": (2, 1)})
        queue.put(("ok", scheduler.acquire(url)))
    except Exception as exc:
        queue.put(("error", str(exc)))


def _cooldown_child(path, queue):
    try:
        ProviderScheduler(path, max_wait=0.05).acquire("https://data.sec.gov/file")
        queue.put("unexpected permit")
    except RuntimeError as exc:
        queue.put(str(exc))


class Clock:
    def __init__(self):
        self.now = 1000.0
    def time(self):
        return self.now
    def sleep(self, amount):
        self.now += amount


def test_permit_time_is_sampled_while_holding_the_lock(tmp_path):
    clock = Clock()
    scheduler = ProviderScheduler(tmp_path / "state.sqlite", {"government": (10, 1)},
                                  clock=clock.time, sleep=clock.sleep)
    connect = scheduler._connect
    lock_waits = [1.0]

    class Contended:
        # Another process holds the write lock for one second before BEGIN returns.
        def __init__(self, connection):
            self.connection = connection

        def __enter__(self):
            self.connection.__enter__()
            return self

        def __exit__(self, *exc):
            return self.connection.__exit__(*exc)

        def execute(self, sql, *args):
            if sql == "BEGIN IMMEDIATE" and lock_waits:
                clock.now += lock_waits.pop()
            return self.connection.execute(sql, *args)

        def executemany(self, *args):
            return self.connection.executemany(*args)

    scheduler._connect = lambda: Contended(connect())
    first = scheduler.acquire("https://www.sec.gov/a")
    second = scheduler.acquire("https://www.sec.gov/b")
    assert first == 1001.0  # recorded when the lock was obtained, not before waiting for it
    assert second-first >= 0.1-1e-9  # paced against the request that was actually sent


def test_spawned_processes_share_government_alias_budget(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    urls = ["https://www.sec.gov/a", "https://DATA.SEC.GOV./b", "https://sec.gov./c", "https://www.sec.gov/d"]
    processes = [ctx.Process(target=_reserve_child, args=(str(tmp_path / "state.sqlite"), url, queue)) for url in urls]
    for process in processes:
        process.start()
    results = [queue.get(timeout=15) for _ in processes]
    for process in processes:
        process.join(15)
        assert process.exitcode == 0
    assert all(status == "ok" for status, _ in results)
    stamps = sorted(stamp for _, stamp in results)
    assert all(right-left >= 0.49 for left, right in zip(stamps, stamps[1:]))
    assert stamps[-1]-stamps[0] >= 1.49


def test_cooldown_is_seen_by_independent_spawned_process(tmp_path):
    path = tmp_path / "cooldown.sqlite"
    ProviderScheduler(path).cooldown("https://www.sec.gov/x", "5")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    process = ctx.Process(target=_cooldown_child, args=(str(path), queue))
    process.start()
    assert "wait exceeded" in queue.get(timeout=15)
    process.join(15)
    assert process.exitcode == 0


def test_trailing_dot_government_aliases_share_permits_and_cooldown(tmp_path):
    clock = Clock()
    scheduler = ProviderScheduler(tmp_path / "state.sqlite", clock=clock.time, sleep=clock.sleep)
    assert scheduler.buckets("https://WWW.SEC.GOV./filing") == ("government",)
    scheduler.acquire("https://www.sec.gov/first")
    scheduler.acquire("https://SEC.GOV./second")
    assert clock.now >= 1000.1
    scheduler.cooldown("https://DATA.SEC.GOV./filing", "3")
    scheduler.acquire("https://www.sec.gov/third")
    assert clock.now >= 1003.1
    assert filing_download_url("https://WWW.SEC.GOV./Archives/edgar/data/1/2/a.htm", "key") == "https://edgar-mirror.sec-api.io/1/2/a.htm"


def test_paid_combined_window_and_product_separation(tmp_path):
    clock = Clock()
    scheduler = ProviderScheduler(tmp_path / "state.sqlite", {"downloads": (2, 300)}, clock=clock.time, sleep=clock.sleep)
    scheduler.acquire("https://edgar-mirror.sec-api.io/a")
    scheduler.acquire("https://archive.sec-api.io/b")
    scheduler.acquire("https://api.sec-api.io/full-text-search")
    assert clock.now < 1001
    scheduler.acquire("https://edgar-mirror.sec-api.io/c")
    assert clock.now >= 1300
    assert scheduler.buckets("https://api.sec-api.io", "query") == ("api_host", "query")


def test_dataset_downloads_consume_shared_account_and_archive_windows(tmp_path):
    clock = Clock()
    scheduler = ProviderScheduler(tmp_path / "state.sqlite", {"downloads": (2, 300)}, clock=clock.time, sleep=clock.sleep)
    dataset = "https://api.sec-api.io/datasets/form-345/2026/2026-01.zip"
    assert scheduler.buckets(dataset) == ("api_host", "archive", "downloads")
    scheduler.acquire(dataset)
    scheduler.acquire("https://edgar-mirror.sec-api.io/filing.htm")
    scheduler.acquire("https://archive.sec-api.io/another.htm")
    assert clock.now >= 1300


@pytest.mark.parametrize("bucket,count", [("archive", 51), ("mirror", 201)])
def test_technical_ceilings_cannot_be_exceeded(tmp_path, bucket, count):
    with pytest.raises(ValueError, match="technical ceiling"):
        ProviderScheduler(tmp_path / "state.sqlite", {bucket: (count, 1)})


def test_stricter_policy_cannot_be_relaxed_by_another_process(tmp_path):
    clock = Clock()
    path = tmp_path / "state.sqlite"
    strict = ProviderScheduler(path, {"government": (2, 1)}, clock=clock.time, sleep=clock.sleep)
    fast = ProviderScheduler(path, clock=clock.time, sleep=clock.sleep)
    strict.acquire("https://sec.gov/a")
    fast.acquire("https://www.sec.gov/b")
    assert clock.now >= 1000.5


def test_retry_after_http_date_and_long_bound(tmp_path):
    clock = Clock()
    scheduler = ProviderScheduler(tmp_path / "state.sqlite", clock=clock.time, sleep=clock.sleep, max_wait=1)
    scheduler.cooldown("https://archive.sec-api.io/a", "Thu, 01 Jan 1970 00:16:50 GMT")
    with pytest.raises(RuntimeError, match="wait exceeded"):
        scheduler.acquire("https://edgar-mirror.sec-api.io/b")
    assert clock.now == 1000


def test_sqlite_failure_fails_closed(tmp_path):
    scheduler = ProviderScheduler(tmp_path / "state.sqlite")
    scheduler.state_path = tmp_path / "missing" / "state.sqlite"
    with pytest.raises(RuntimeError, match="state unavailable"):
        scheduler.acquire("https://sec.gov/a")


def test_http_retries_redirects_and_permanent_failure(tmp_path):
    events = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            events.append((self.path, time.time(), self.headers.get("User-Agent")))
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/retry")
                self.end_headers()
            elif self.path == "/retry" and sum(path == "/retry" for path, _, _ in events) == 1:
                self.send_response(429)
                self.send_header("Retry-After", "1")
                self.end_headers()
            elif self.path.startswith("/forbidden"):
                self.send_response(403)
                self.end_headers()
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"original bytes")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        scheduler = ProviderScheduler(tmp_path / "state.sqlite", {"other": (20, 1)})
        client = ProviderTransport("Tests contact@example.com", scheduler=scheduler)
        base = f"http://127.0.0.1:{server.server_port}"
        with client.open(base + "/redirect") as response:
            assert response.read() == b"original bytes"
        retry_times = [stamp for path, stamp, _ in events if path == "/retry"]
        assert retry_times[1]-retry_times[0] >= 1
        assert len(events) == 4  # both initial requests and both redirect destinations
        assert all(agent == "Tests contact@example.com" for _, _, agent in events)
        with pytest.raises(RuntimeError) as caught:
            client.open(base + "/forbidden?token=top-secret", max_attempts=5)
        assert "top-secret" not in str(caught.value)
        # No credential was sent, so a denial is an ordinary transport failure.
        assert not isinstance(caught.value, ProviderAuthError)
        assert sum(path.startswith("/forbidden") for path, _, _ in events) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


def test_redirect_reserves_destination_and_refuses_credentials(tmp_path):
    import urllib.request
    scheduler = ProviderScheduler(tmp_path / "state.sqlite")
    reserved = []
    scheduler.acquire = lambda url, product=None: reserved.append((url, product))
    handler = _GovernedRedirect(scheduler, "downloads")
    request = urllib.request.Request("https://edgar-mirror.sec-api.io/a")
    handler.redirect_request(request, None, 302, "Found", {}, "https://data.sec.gov/a")
    assert reserved == [("https://data.sec.gov/a", None)]
    request.add_header("Authorization", "secret")
    with pytest.raises(RuntimeError, match="credential redirect refused"):
        handler.redirect_request(request, None, 302, "Found", {}, "https://data.sec.gov/a")
    assert len(reserved) == 1


def test_auth_boundary_and_safe_identity(tmp_path):
    client = ProviderTransport("Tests contact@example.com", "secret", ProviderScheduler(tmp_path / "state.sqlite"))
    with pytest.raises(ValueError, match="trusted HTTPS"):
        client.open("https://sec.gov/a", authenticated=True)
    canonical = "https://www.sec.gov/Archives/edgar/data/1/2/a.htm"
    assert filing_download_url(canonical, "secret") == "https://edgar-mirror.sec-api.io/1/2/a.htm"
    assert filing_download_url(canonical) == canonical
    assert safe_url("https://user:secret@api.sec-api.io/a?token=secret#secret") == "https://api.sec-api.io/a"
    with pytest.raises(ValueError, match="ten requests"):
        ProviderScheduler(tmp_path / "bad.sqlite", {"government": (11, 1)})
