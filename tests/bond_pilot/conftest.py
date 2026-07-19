from __future__ import annotations

from io import BytesIO
from pathlib import Path
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


REQUIRED_ROWS = {
    "cusip_id": ["123456789", "987654321"],
    "trd_exctn_dt": ["2024-01-03", "2024-02-05"],
    "pr": [101.25, 99.75],
}


def parquet_bytes(columns: dict[str, list[object]] | None = None) -> bytes:
    buffer = BytesIO()
    pq.write_table(pa.table(columns or REQUIRED_ROWS), buffer)
    return buffer.getvalue()


def zip_bytes(entries: list[tuple[str, bytes, int]]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, payload, attributes in entries:
            info = zipfile.ZipInfo(name)
            info.filename = name
            info.external_attr = attributes
            archive.writestr(info, payload)
    contents = buffer.getvalue()
    for name, _, _ in entries:
        if "\\" in name:
            contents = contents.replace(name.replace("\\", "/").encode("utf-8"), name.encode("utf-8"))
    return contents


@pytest.fixture
def make_source_zip(tmp_path: Path):
    def make_source_zip(
        *,
        columns: dict[str, list[object]] | None = None,
        member_name: str = "nested/source.parquet",
        extra_entries: list[tuple[str, bytes, int]] | None = None,
        member_attributes: int = 0,
    ) -> Path:
        entries = [(member_name, parquet_bytes(columns), member_attributes)]
        entries.extend(extra_entries or [])
        archive = tmp_path / "source-input.zip"
        archive.write_bytes(zip_bytes(entries))
        return archive

    return make_source_zip


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def iter_bytes(self, chunk_size: int):
        yield from (self.payload[index : index + chunk_size] for index in range(0, len(self.payload), chunk_size))

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None


class FakeClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.urls: list[str] = []

    def stream(self, method: str, url: str) -> FakeResponse:
        assert method == "GET"
        self.urls.append(url)
        return FakeResponse(self.payload)
