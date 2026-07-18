"""RR1 TSV streaming with the physical delivery's two known transport quirks."""
from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Iterator
from pathlib import Path

from src.sec_regulatory.tsv import TsvFormatError

_MAX_RR1_FIELD_BYTES = 10_000_000


def stream_tsv(path: Path, *, expected_sha256: str | None = None) -> tuple[tuple[str, ...], Iterator[tuple[int, tuple[str, ...]]]]:
    """Stream one RR1 delivery without losing a long text block or blank tail.

    Historic RR1 labels may end with empty delimiters beyond the frozen header;
    those carry no additional lexical field and are normalized only when every
    surplus cell is blank.  Nonblank surplus cells remain a format error.
    """
    csv.field_size_limit(_MAX_RR1_FIELD_BYTES)
    binary = path.open("rb")
    digest = hashlib.sha256()

    class _HashingReader(io.RawIOBase):
        def readable(self) -> bool:
            return True

        def readinto(self, buffer: bytearray) -> int:
            count = binary.readinto(buffer)
            if count:
                digest.update(memoryview(buffer)[:count])
            return count or 0

        def close(self) -> None:
            if not self.closed:
                binary.close()
            super().close()

    text = io.TextIOWrapper(io.BufferedReader(_HashingReader()), encoding="utf-8", newline="")
    reader = csv.reader(text, delimiter="\t")
    try:
        header = tuple(next(reader))
    except StopIteration as error:
        text.close()
        raise TsvFormatError(f"TSV vazio: {path.name}") from error

    def rows() -> Iterator[tuple[int, tuple[str, ...]]]:
        try:
            for row_number, row in enumerate(reader, start=2):
                if len(row) > len(header) and all(value == "" for value in row[len(header):]):
                    row = row[:len(header)]
                if len(row) != len(header):
                    raise TsvFormatError(f"quantidade de colunas inválida em {path.name}:{row_number}")
                yield row_number, tuple(row)
            if expected_sha256 is not None and digest.hexdigest() != expected_sha256:
                raise TsvFormatError(f"SHA-256 mudou durante leitura: {path.name}")
        finally:
            text.close()

    iterator = rows()

    class _Rows(Iterator[tuple[int, tuple[str, ...]]]):
        def __iter__(self) -> _Rows:
            return self

        def __next__(self) -> tuple[int, tuple[str, ...]]:
            return next(iterator)

        def close(self) -> None:
            text.close()

    return header, _Rows()
