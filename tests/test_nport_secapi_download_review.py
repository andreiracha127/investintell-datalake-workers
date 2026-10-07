"""Downloader regressions found by the holistic monthly N-PORT review."""

from __future__ import annotations

import json
import traceback
from pathlib import Path

import pytest

from tools.nport_secapi import download


class DatasetStub:
    """Match the SDK's size-only reuse and atomic temporary-file writer."""

    def __init__(self, payload: bytes = b"old", month: str = "2026-07"):
        self.payload = payload
        self.month = month
        self.updated_at = "2026-08-01"
        self.listing_calls = 0
        self.download_calls = 0
        self.listing_failures: list[Exception] = []
        self.download_failures: list[Exception] = []
        self.short_response: bytes | None = None

    def get_dataset_details(self, name):
        assert name == download.DATASET
        self.listing_calls += 1
        if self.listing_failures:
            raise self.listing_failures.pop(0)
        return {"containers": [{
            "key": f"{self.month[:4]}/{self.month}.jsonl.gz",
            "downloadUrl": "https://api.sec-api.io/container",
            "size": len(self.payload),
            "updatedAt": self.updated_at,
        }]}

    def _download_file(self, url, dest, expected_size=None):
        self.download_calls += 1
        if self.download_failures:
            raise self.download_failures.pop(0)
        dest = Path(dest)
        if dest.exists() and dest.stat().st_size == expected_size:
            return str(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        body, self.short_response = self.short_response or self.payload, None
        temp = dest.with_name(dest.name + ".tmp")
        temp.write_bytes(body)
        temp.rename(dest)
        return str(dest)


def fetch(tmp_path, ds, **kwargs):
    return download.download_months(ds.month, ds.month, str(tmp_path), datasets=ds,
                                    sleep=lambda _: None, log=lambda _: None, **kwargs)


@pytest.mark.parametrize("start,end", [
    ("2026-13", "2027-01"), ("2026-00", "2026-01"),
    ("2026-07", "2026-06"), ("2026-7", "2026-07"),
])
def test_invalid_month_window_is_rejected_before_listing(tmp_path, start, end):
    ds = DatasetStub(month="2027-01")
    with pytest.raises(ValueError, match="month|window"):
        download.download_months(start, end, str(tmp_path), datasets=ds)
    assert ds.listing_calls == ds.download_calls == 0


def test_invalid_cli_month_is_rejected_before_reading_credentials(monkeypatch, tmp_path):
    def unexpected_key(_):
        raise AssertionError("an invalid window must not request credentials")

    monkeypatch.setattr(download, "load_api_key", unexpected_key)
    with pytest.raises(SystemExit) as err:
        download.main(["--from", "2026-13", "--to", "2027-01", "--out", str(tmp_path)])
    assert err.value.code == 2


def test_failed_resync_preserves_last_verified_cache(tmp_path):
    ds = DatasetStub()
    first = fetch(tmp_path, ds)
    dest = Path(first["containers"][0]["path"])
    old_metadata = download._meta_path(dest).read_bytes()
    ds.updated_at = "2026-08-02"
    ds.payload = b"new"
    ds.download_failures = [Exception("API error: 404 - unavailable")]
    with pytest.raises(RuntimeError, match="download failed"):
        fetch(tmp_path, ds)
    assert dest.read_bytes() == b"old"
    assert download._meta_path(dest).read_bytes() == old_metadata


def test_short_response_is_retried_before_publishing_cache(tmp_path):
    ds = DatasetStub(payload=b"complete")
    ds.short_response = b"short"
    result = fetch(tmp_path, ds)
    dest = Path(result["containers"][0]["path"])
    assert ds.download_calls == 2
    assert dest.read_bytes() == b"complete"
    assert json.loads(download._meta_path(dest).read_text())["size"] == len(b"complete")


@pytest.mark.parametrize("metadata", [[], 3, "old"])
def test_non_object_cache_metadata_is_a_cache_miss(tmp_path, metadata):
    ds = DatasetStub()
    dest = Path(fetch(tmp_path, ds)["containers"][0]["path"])
    download._meta_path(dest).write_text(json.dumps(metadata), encoding="utf-8")
    result = fetch(tmp_path, ds)
    assert ds.download_calls == 2
    assert result["bytes_transferred"] == len(ds.payload)


def test_transient_listing_failure_is_retried_and_scrubbed(tmp_path):
    ds = DatasetStub()
    key = "a" * 64
    ds.listing_failures = [Exception(f"503 https://api.sec-api.io/d?token={key}")]
    logs, sleeps = [], []
    result = download.download_months(ds.month, ds.month, str(tmp_path), datasets=ds,
                                      log=logs.append, sleep=sleeps.append)
    assert ds.listing_calls == 2 and sleeps == [5.0]
    assert result["bytes_transferred"] == len(ds.payload)
    assert key not in " ".join(logs)


def test_exhausted_listing_retry_never_leaks_key_in_traceback(tmp_path):
    ds = DatasetStub()
    key = "b" * 64
    ds.listing_failures = [Exception(f"503 https://api.sec-api.io/d?token={key}")] * 2
    with pytest.raises(RuntimeError, match="dataset listing failed") as err:
        fetch(tmp_path, ds, attempts=2)
    assert ds.listing_calls == 2
    assert key not in "".join(traceback.format_exception(err.value))
