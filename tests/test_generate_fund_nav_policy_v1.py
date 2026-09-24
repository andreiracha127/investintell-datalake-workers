"""Offline XNYS policy construction, conservative lifecycle and artifact custody."""

from __future__ import annotations

import copy
import datetime as dt
import errno
import importlib.metadata
import json
import os
import stat
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from scripts import fund_nav_readiness_schema as operator
from scripts import generate_fund_nav_policy_v1 as generator
from src.workers._nav_policy import (
    generation_metadata_digest,
    instrument_evidence_digest,
    policy_content_digest,
)

START = dt.date(2024, 1, 1)
END = dt.date(2027, 12, 31)
OBSERVED = dt.datetime(2026, 9, 23, 10, 0, tzinfo=dt.timezone.utc)


def _iu(
    number: int,
    *,
    ticker: str | None = None,
    active: bool | None = True,
    currency: str = "USD",
    instrument_type: str = "fund",
) -> dict:
    return {
        "instrument_id": uuid.UUID(int=number),
        "instrument_type": instrument_type,
        "ticker": ticker or f"T{number}",
        "isin": f"US{number:010d}",
        "currency": currency,
        "is_active": active,
    }


def _fund(
    number: int,
    *,
    ticker: str | None = None,
    series: str | None = None,
    currency: str = "USD",
    fund_type: str = "etf",
) -> dict:
    return {
        "instrument_id": uuid.UUID(int=number),
        "series_id": series or f"S{number}",
        "ticker": ticker or f"T{number}",
        "isin": f"US{number:010d}",
        "currency": currency,
        "fund_type": fund_type,
    }


@pytest.fixture(scope="module")
def calendar() -> dict:
    return generator.build_calendar(START, END)


def test_xnys_pin_holiday_early_close_dst_and_final_deadline(calendar):
    import exchange_calendars as xcals

    assert xcals.__version__ == "4.13.2"
    assert importlib.metadata.version("exchange_calendars") == "4.13.2"
    assert "exchange_calendars==4.13.2" in (
        generator.ROOT / "requirements.txt"
    ).read_text(encoding="utf-8")
    assert calendar["calendar_source"] == "exchange_calendars/XNYS"
    assert len(calendar["sessions"]) >= 401
    assert calendar["coverage_start"] == "2024-01-02"  # Jan 1 exchange holiday
    assert calendar["coverage_end"] == "2027-12-31"
    lookup = {row["session_date"]: row for row in calendar["sessions"]}
    assert "2024-07-04" not in lookup
    early = lookup["2024-11-29"]
    assert (
        dt.datetime.fromisoformat(early["valuation_close_at"])
        .astimezone(generator.NY)
        .hour
        == 13
    )
    assert (
        dt.datetime.fromisoformat(early["nav_due_at"])
        .astimezone(generator.NY)
        .strftime("%H:%M")
        == "18:05"
    )
    for day, expected_utc in (
        ("2024-03-08", "23:05"),
        ("2024-03-11", "22:05"),
        ("2024-11-01", "22:05"),
        ("2024-11-04", "23:05"),
    ):
        due = dt.datetime.fromisoformat(lookup[day]["nav_due_at"])
        assert due.strftime("%H:%M") == expected_utc
        assert due.astimezone(generator.NY).date() == dt.date.fromisoformat(day)
    assert calendar["valid_through"] == calendar["sessions"][-1]["nav_due_at"]
    assert generator.verify_artifact(calendar)["mode"] == "calendar"


def test_calendar_short_window_and_package_mismatch_fail(monkeypatch):
    with pytest.raises(
        generator.PolicyGenerationError, match="calendar_window_too_short"
    ):
        generator.build_calendar(dt.date(2026, 1, 1), dt.date(2026, 3, 31))
    monkeypatch.setattr(generator.importlib.metadata, "version", lambda name: "4.14.0")
    with pytest.raises(
        generator.PolicyGenerationError, match="calendar_package_version_mismatch"
    ):
        generator.build_calendar(START, END)


@pytest.mark.parametrize(
    "mutation",
    [
        "package",
        "version",
        "digest",
        "session_drop",
        "duplicate",
        "deadline",
        "early_close",
        "expiration",
    ],
)
def test_calendar_tamper_is_rejected(calendar, mutation):
    policy = copy.deepcopy(calendar)
    if mutation == "package":
        policy["source_reference"] = policy["source_reference"].replace(
            "4.13.2", "4.14.0"
        )
    elif mutation == "version":
        policy["calendar_version"] = "floating-latest"
    elif mutation == "digest":
        policy["calendar_digest"] = "0" * 64
    elif mutation == "session_drop":
        policy["sessions"].pop(200)
    elif mutation == "duplicate":
        policy["sessions"].insert(200, policy["sessions"][199])
    elif mutation == "deadline":
        policy["sessions"][10]["nav_due_at"] = policy["sessions"][10][
            "valuation_close_at"
        ]
    elif mutation == "early_close":
        next(row for row in policy["sessions"] if row["session_date"] == "2024-11-29")[
            "valuation_close_at"
        ] = "2024-11-29T21:00:00+00:00"
    elif mutation == "expiration":
        policy["valid_through"] = "2027-12-31T23:59:00+00:00"
    with pytest.raises((generator.PolicyGenerationError, ValueError)):
        generator.verify_artifact(policy)


def test_lifecycle_conflicts_are_unknown_or_unverified(calendar):
    instruments = [
        _iu(1),
        _iu(2, active=False),
        _iu(3, active=False),
        _iu(4, active=None),
        _iu(5, ticker="DUP"),
        _iu(6, ticker="DUP"),
        _iu(7, currency="EUR"),
        _iu(8),
        _iu(9),
        _iu(10),
        _iu(11),
        _iu(12),
        _iu(13),
        _iu(14),
        _iu(15),
        _iu(16),
    ]
    funds = [
        _fund(1),
        _fund(3),
        _fund(4),
        _fund(5, ticker="DUP"),
        _fund(6, ticker="DUP"),
        _fund(7, currency="EUR"),
        _fund(8, series=""),
        _fund(9, ticker="OTHER"),
        _fund(10, fund_type="mmf"),
        _fund(11),
        _fund(11),
        _fund(12),
        _fund(13),
        _fund(14),
        _fund(15),
        _fund(16),
    ]
    funds[6]["series_id"] = None
    instruments[11]["ticker"] = None
    funds[12]["isin"] = "US0000000999"
    for number in (14, 15):
        instruments[number - 1]["isin"] = "US9999999999"
        next(row for row in funds if row["instrument_id"] == uuid.UUID(int=number))[
            "isin"
        ] = "US9999999999"
    instruments[15]["isin"] = None
    evidence, counts = generator.classify_catalog(instruments, funds, OBSERVED)
    rows = {uuid.UUID(row["instrument_id"]).int: row for row in evidence}
    assert rows[1]["fund_status"] == "ACTIVE"
    assert rows[1]["valuation_frequency"] == "daily"
    assert all(
        rows[1][field] is True
        for field in ("identity_verified", "return_basis_verified", "currency_verified")
    )
    assert rows[2]["fund_status"] == "INACTIVE"
    for number in (3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16):
        assert rows[number]["fund_status"] == "UNKNOWN"
        assert rows[number]["return_basis_verified"] is False
    assert rows[10]["fund_status"] == "ACTIVE"
    assert rows[10]["valuation_frequency"] == "unknown"
    assert rows[10]["return_basis_verified"] is False
    assert counts["fund_status"] == {"ACTIVE": 2, "INACTIVE": 1, "UNKNOWN": 13}


def test_policy_content_hash_is_stable_across_generation_timestamps(calendar):
    instruments = [_iu(1)]
    funds = [_fund(1)]
    instruments[0]["name"] = "private-holder-name"
    funds[0]["owner_email"] = "private@example.invalid"
    first = generator.build_policy(
        calendar, instruments, funds, OBSERVED, "policy-demo", "v1"
    )
    second = generator.build_policy(
        calendar,
        instruments,
        funds,
        OBSERVED + dt.timedelta(seconds=1),
        "policy-demo",
        "v1",
    )
    assert first["generation"]["policy_hash"] == second["generation"]["policy_hash"]
    assert (
        first["generation"]["instrument_evidence_digest"]
        == second["generation"]["instrument_evidence_digest"]
    )
    assert generator.canonical_json(first) != generator.canonical_json(second)
    assert first["generation"]["policy_hash"] == policy_content_digest(first)
    assert first["generation"][
        "instrument_evidence_digest"
    ] == instrument_evidence_digest(first["instrument_evidence"])
    assert operator._policy(first)[0] == first
    assert generator.verify_artifact(first)["mode"] == "build"
    text = generator.canonical_json(first).decode()
    assert "postgresql://" not in text and "@" not in json.dumps(
        first["instrument_evidence"]
    )
    assert '"ticker":"T1"' not in text and '"isin":"US0000000001"' not in text
    assert "private-holder-name" not in text and "private@example.invalid" not in text


@pytest.mark.parametrize(
    "field,value",
    [
        ("policy_hash", "0" * 64),
        ("instrument_evidence_digest", "0" * 64),
        ("source_query_sha256", "0" * 64),
        ("source_snapshot_sha256", "0" * 64),
        ("calendar_digest", "0" * 64),
        ("generation_sha256", "0" * 64),
    ],
)
def test_generator_metadata_digest_tamper_fails_operator(calendar, field, value):
    policy = generator.build_policy(
        calendar, [_iu(1)], [_fund(1)], OBSERVED, "pin", "v1"
    )
    policy["generation"][field] = value
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(policy)


@pytest.mark.parametrize("field", ["generator_version", "provider_contract"])
def test_hash_consistent_unsupported_generator_or_provider_is_rejected(calendar, field):
    policy = generator.build_policy(
        calendar, [_iu(1)], [_fund(1)], OBSERVED, "pin", "v1"
    )
    policy[field] = "unsupported-future-contract"
    policy["generation"][field] = "unsupported-future-contract"
    if field == "provider_contract":
        policy["instrument_evidence"][0]["evidence_reference"] = (
            "unsupported-future-contract"
        )
        policy["generation"]["instrument_evidence_digest"] = instrument_evidence_digest(
            policy["instrument_evidence"]
        )
    policy["generation"]["policy_hash"] = policy_content_digest(policy)
    policy["generation"]["generation_sha256"] = generation_metadata_digest(
        policy["generation"]
    )
    with pytest.raises(ValueError, match="generator_metadata_invalid"):
        operator._policy(policy)


def test_output_creation_overwrite_and_offline_verify(tmp_path, calendar, capsys):
    target = tmp_path / "reference.json"
    content = generator.canonical_json(calendar)
    generator.write_artifact(target, content, force=False, build=False)
    assert target.read_bytes() == content
    if os.name == "posix":
        assert stat.S_IMODE(target.stat().st_mode) == 0o600
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_already_exists"
    ):
        generator.write_artifact(target, content, force=False, build=False)
    generator.write_artifact(target, content, force=True, build=False)
    assert generator.main(["verify", "--policy-file", str(target)]) == 0
    printed = capsys.readouterr().out
    assert "T1" not in printed and "postgresql://" not in printed
    assert (
        generator.verify_artifact(
            json.loads(target.read_bytes()), raw=target.read_bytes()
        )["mode"]
        == "calendar"
    )


def test_calendar_cli_emits_unpublished_reference_without_dsn(
    tmp_path, monkeypatch, capsys
):
    monkeypatch.delenv("NAV_READINESS_DATABASE_URL", raising=False)
    target = tmp_path / "calendar.json"
    assert (
        generator.main(
            [
                "calendar",
                "--coverage-start",
                START.isoformat(),
                "--coverage-end",
                END.isoformat(),
                "--output",
                str(target),
            ]
        )
        == 0
    )
    artifact = json.loads(target.read_bytes())
    assert artifact["publication_state"] == "unpublished"
    assert "instrument_evidence" not in artifact and "policy_hash" not in artifact
    assert generator.main(["verify", "--policy-file", str(target)]) == 0
    assert "private" not in capsys.readouterr().out


def _private_custody(tmp_path):
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    return root


def test_build_requires_posix_and_private_external_custody(tmp_path, monkeypatch):
    root = _private_custody(tmp_path)
    with pytest.raises(generator.PolicyGenerationError, match="custody_root_required"):
        generator.write_artifact(
            root / "policy.json", b"private", force=False, build=True
        )
    monkeypatch.setattr(generator, "_platform_name", lambda: "nt")
    with pytest.raises(
        generator.PolicyGenerationError, match="productive_artifact_requires_posix"
    ):
        generator.write_artifact(
            root / "policy.json", b"private", force=False, build=True, custody_root=root
        )
    generator.write_artifact(
        root / "calendar.json", b"unpublished", force=False, build=False
    )
    assert (root / "calendar.json").read_bytes() == b"unpublished"


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_build_rejects_git_directory_file_and_symlink_into_checkout(tmp_path):
    root = _private_custody(tmp_path)
    worktree = root / "worktree"
    worktree.mkdir()
    (worktree / ".git").touch()  # Worktree marker file; contents never read.
    checkout = root / "checkout"
    checkout.mkdir()
    (checkout / ".git").mkdir()
    for destination in (worktree / "artifact.json", checkout / "artifact.json"):
        with pytest.raises(
            generator.PolicyGenerationError, match="artifact_inside_git_checkout"
        ):
            generator.write_artifact(
                destination, b"secret", force=False, build=True, custody_root=root
            )
        assert not destination.exists()
    link = root / "link"
    link.symlink_to(checkout, target_is_directory=True)
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_inside_git_checkout"
    ):
        generator.write_artifact(
            link / "artifact.json",
            b"secret",
            force=False,
            build=True,
            custody_root=root,
        )
    root.chmod(0o755)
    with pytest.raises(
        generator.PolicyGenerationError, match="custody_root_not_private"
    ):
        generator.write_artifact(
            root / "artifact.json",
            b"secret",
            force=False,
            build=True,
            custody_root=root,
        )


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
@pytest.mark.parametrize("force", [False, True])
def test_disk_failure_leaves_no_partial_destination_and_preserves_existing(
    tmp_path, monkeypatch, force
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    if force:
        destination.write_bytes(b"old-complete-artifact")
    real_fsync = os.fsync

    def fail_file_sync(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(errno.ENOSPC, "synthetic disk failure")
        return real_fsync(fd)

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "fsync", fail_file_sync)
        with pytest.raises(OSError):
            generator.write_artifact(
                destination,
                b"new-complete-artifact",
                force=force,
                build=True,
                custody_root=root,
            )
    assert (
        destination.read_bytes() == b"old-complete-artifact"
        if force
        else not destination.exists()
    )
    assert not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_midwrite_failure_cleans_private_temp_without_publishing(tmp_path, monkeypatch):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    real_fdopen = os.fdopen

    class BrokenStream:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, data):
            self.stream.write(data[: len(data) // 2])
            raise OSError(errno.ENOSPC, "synthetic midwrite failure")

    with monkeypatch.context() as ctx:
        ctx.setattr(
            generator.os, "fdopen", lambda fd, mode: BrokenStream(real_fdopen(fd, mode))
        )
        with pytest.raises(OSError):
            generator.write_artifact(
                destination,
                b"complete-artifact",
                force=False,
                build=True,
                custody_root=root,
            )
    assert not destination.exists() and not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
@pytest.mark.parametrize("force", [False, True])
def test_atomic_publish_syscall_failure_keeps_prior_artifact(
    tmp_path, monkeypatch, force
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    if force:
        destination.write_bytes(b"old-complete")

    def fail_publish(*_args, **_kwargs):
        raise OSError(errno.EIO, "synthetic publish failure")

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "replace" if force else "link", fail_publish)
        with pytest.raises(OSError):
            generator.write_artifact(
                destination, b"new-complete", force=force, build=True, custody_root=root
            )
    if force:
        assert destination.read_bytes() == b"old-complete"
    else:
        assert not destination.exists()
    assert not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
@pytest.mark.parametrize("force", [False, True])
def test_symlink_inserted_during_private_temp_write_fails_closed(
    tmp_path, monkeypatch, force
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"outside-unchanged")
    real_fsync = os.fsync

    def race_before_publish(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            destination.symlink_to(outside)
        return real_fsync(fd)

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "fsync", race_before_publish)
        with pytest.raises(
            generator.PolicyGenerationError, match="artifact_target_not_regular"
        ):
            generator.write_artifact(
                destination, b"private", force=force, build=True, custody_root=root
            )
    assert outside.read_bytes() == b"outside-unchanged"
    assert destination.is_symlink() and not list(root.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_parent_replaced_by_git_symlink_during_write_fails_closed(
    tmp_path, monkeypatch
):
    root = _private_custody(tmp_path)
    parent = root / "nested"
    parent.mkdir(mode=0o700)
    moved = root / "moved"
    checkout = tmp_path / "another-checkout"
    checkout.mkdir()
    (checkout / ".git").touch()
    destination = parent / "policy.json"
    real_fsync = os.fsync

    def swap_parent(fd):
        if stat.S_ISREG(os.fstat(fd).st_mode):
            parent.rename(moved)
            parent.symlink_to(checkout, target_is_directory=True)
        return real_fsync(fd)

    with monkeypatch.context() as ctx:
        ctx.setattr(generator.os, "fsync", swap_parent)
        with pytest.raises(
            generator.PolicyGenerationError, match="artifact_parent_changed"
        ):
            generator.write_artifact(
                destination, b"private", force=False, build=True, custody_root=root
            )
    assert not (checkout / "policy.json").exists()
    assert not list(moved.glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_intermediate_parent_symlink_race_is_rejected_before_private_write(
    tmp_path, monkeypatch
):
    root = _private_custody(tmp_path)
    middle = root / "middle"
    middle.mkdir(mode=0o700)
    inner = middle / "inner"
    inner.mkdir(mode=0o700)
    checkout = tmp_path / "other-checkout"
    checkout.mkdir()
    (checkout / ".git").mkdir()
    (checkout / "inner").mkdir()
    destination = inner / "policy.json"
    original = generator._pinned_parent

    def swap_before_open(path):
        middle.rename(root / "moved")
        middle.symlink_to(checkout, target_is_directory=True)
        return original(path)

    monkeypatch.setattr(generator, "_pinned_parent", swap_before_open)
    with pytest.raises(
        generator.PolicyGenerationError, match="artifact_parent_changed"
    ):
        generator.write_artifact(
            destination, b"private", force=False, build=True, custody_root=root
        )
    assert not (checkout / "inner" / "policy.json").exists()
    assert not list((root / "moved" / "inner").glob(".nav-policy-*.tmp"))


@pytest.mark.skipif(
    os.name != "posix", reason="productive build intentionally POSIX-only"
)
def test_concurrent_no_replace_creators_publish_one_complete_file(
    tmp_path, monkeypatch
):
    root = _private_custody(tmp_path)
    destination = root / "policy.json"
    barrier = Barrier(2)
    check = generator._check_parent_and_target

    def concurrent_check(*args, **kwargs):
        check(*args, **kwargs)
        barrier.wait(timeout=5)

    monkeypatch.setattr(generator, "_check_parent_and_target", concurrent_check)

    def create(content):
        try:
            generator.write_artifact(
                destination, content, force=False, build=True, custody_root=root
            )
            return "published"
        except generator.PolicyGenerationError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, (b"first-complete", b"second-complete")))
    assert sorted(results) == ["artifact_already_exists", "published"]
    assert destination.read_bytes() in (b"first-complete", b"second-complete")
    assert not list(root.glob(".nav-policy-*.tmp"))
