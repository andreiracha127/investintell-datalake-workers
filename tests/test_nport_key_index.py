"""Exact conflict-key storage without one Python object per holding."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.nport_dera.key_index import KeyIndex


def test_exact_duplicate_keys_and_nearby_keys_are_distinct():
    keys = {
        ("2026-05-31", "S1", "123456789"),
        ("2026-05-31", "S1", "123456788"),
        ("2026-05-31", "S2", "123456789"),
        ("2026-06-30", "S1", "123456789"),
        ("2026-05-31", "s1", "123456789"),
        ("2026-05-31", "S1", "é"),
        ("2026-05-31", "S1", "e\u0301"),
    }
    with KeyIndex() as index:
        assert len(index) == 0
        index.update(keys)
        index.update(iter(keys))
        assert len(index) == len(keys)
        assert set(index) == keys
        assert all(key in index for key in keys)
        assert ("2026-05-31", "S1", "123456787") not in index
        index.add(next(iter(keys)))
        assert len(index) == len(keys)


def test_cross_csv_transfer_keeps_keys_when_source_is_closed():
    shared = ("2026-05-31", "S1", "A")
    other = ("2026-05-31", "S2", "A")
    with KeyIndex() as earlier:
        with KeyIndex() as current:
            current.add(shared)
            current.add(other)
            earlier |= current
            earlier |= earlier  # self-transfer is a no-op
        assert shared in earlier and other in earlier
        assert len(earlier) == 2
        with KeyIndex() as next_csv:
            next_csv.update([shared, ("2026-06-30", "S1", "A")])
            duplicates = [key for key in next_csv if key in earlier]
            assert duplicates == [shared]
            earlier.update(next_csv)
        assert len(earlier) == 3


def test_discard_handles_missing_keys_without_removing_nearby_key():
    key, other = ("D", "S", "A"), ("D", "S", "B")
    with KeyIndex() as index:
        index.update([key, other])
        index.discard(key)
        index.discard(key)
        assert key not in index and other in index
        assert set(index) == {other} and len(index) == 1


def test_generator_update_spans_batches_and_deduplicates_exactly():
    with KeyIndex() as index:
        index.update(("2026-05-31", f"S{n % 2}", str(n)) for n in range(25_000))
        index.update(("2026-05-31", f"S{n % 2}", str(n)) for n in range(20_000, 30_000))
        assert len(index) == 30_000
        assert ("2026-05-31", "S1", "29999") in index
        assert ("2026-05-31", "S0", "29999") not in index
        assert len(list(index)) == 30_000


def test_context_manager_removes_sqlite_file_on_exception():
    with pytest.raises(ValueError, match="failed CSV"):
        with KeyIndex() as index:
            db_path = Path(index.path)
            assert db_path.is_file()
            index.add(("D", "S", "A"))
            raise ValueError("failed CSV")
    assert not db_path.exists()
    index.close()  # idempotent
    with pytest.raises(RuntimeError, match="closed"):
        index.add(("D", "S", "B"))


@pytest.mark.parametrize("invalid", ["D,S,A", ["D", "S", "A"], ("D", "S"), ("D", "S", 1)])
def test_only_three_string_tuple_keys_can_be_added(invalid):
    with KeyIndex() as index:
        with pytest.raises(TypeError, match="three strings"):
            index.add(invalid)
        assert len(index) == 0
