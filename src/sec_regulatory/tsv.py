"""Primitivas streaming para TSVs SEC, sem leituras integrais do arquivo."""

from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Iterator
from pathlib import Path


class TsvFormatError(ValueError):
    """Cabeçalho ou linha TSV não respeita o formato governado."""


def stream_tsv(path: Path, *, expected_sha256: str | None = None) -> tuple[tuple[str, ...], Iterator[tuple[int, tuple[str, ...]]]]:
    """Retorna cabeçalho e iterador de linhas lexicais, uma linha por vez."""
    # Header and data are parsed from one descriptor.  A concurrent replacement
    # cannot turn a verified file into different rows between checks.
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
        expected_header = tuple(next(reader))
    except StopIteration as error:
        text.close()
        raise TsvFormatError(f"TSV vazio: {path.name}") from error

    def rows() -> Iterator[tuple[int, tuple[str, ...]]]:
        try:
            for row_number, row in enumerate(reader, start=2):
                if len(row) != len(expected_header):
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

    return expected_header, _Rows()
