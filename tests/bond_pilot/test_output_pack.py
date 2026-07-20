from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from src.bond_pilot._secure_local_fs import secure_open_dir
from src.bond_pilot.contracts import PilotError
from src.bond_pilot.artifacts import canonical_json_bytes
from src.bond_pilot.output_pack import OutputPack, open_validated_output_pack, validate_output_pack


def _pack(parent, run_id: str = "run-1") -> OutputPack:
    return OutputPack.create(parent, run_id=run_id, pack_schema_version="pack-v1", producer_version="test")


def _replace_completion(pack: OutputPack, contents: bytes) -> None:
    pack.directory.unlink_file("completion.json", error_code="incomplete_output")
    created = pack.directory.create_file(".pending-replacement", error_code="incomplete_output")
    try:
        created.write(contents)
        pack.directory.publish_no_replace(created, "completion.json", error_code="incomplete_output")
    finally:
        created.close()


def test_sealed_pack_is_exact_and_completion_is_required(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        incomplete = _pack(parent, "incomplete")
        incomplete.write_payload("panel.parquet", b"panel")
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "incomplete", expected_pack_schema_version="pack-v1")
        incomplete.close()

        pack = _pack(parent)
        pack.write_payload("panel.parquet", b"panel")
        pack.write_payload("reports/quality.json", b"{}")
        completed = pack.finalize(completed_at="2026-07-20T00:00:00Z")
        assert completed.files == ("checksums.sha256", "completion.json", "panel.parquet", "reports/quality.json")
        assert validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1") == completed

        extra = pack.directory.create_file("extra.bin", error_code="incomplete_output")
        try:
            extra.write(b"unexpected")
            pack.directory.publish_no_replace(extra, "extra.bin", error_code="incomplete_output")
        finally:
            extra.close()
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")
        pack.close()


def test_output_pack_never_reuses_reserved_run_name(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        first = _pack(parent)
        with pytest.raises(PilotError, match="^already_exists$"):
            _pack(parent)
        first.close()


def test_output_pack_reserves_open_payloads_and_aborts_them_on_close(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        writer = pack.create_payload("panel.parquet")
        with pytest.raises(PilotError, match="^already_exists$"):
            pack.create_payload("panel.parquet")
        pack.close()
        assert writer.closed


def test_completion_publication_is_the_last_fallible_filesystem_operation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        events: list[str] = []
        publish = pack.directory.publish_no_replace
        run_flush = pack.directory.flush
        parent_flush = parent.flush

        def record_publish(created, final_name: str, **kwargs) -> None:
            events.append(f"publish:{final_name}")
            publish(created, final_name, **kwargs)

        monkeypatch.setattr(pack.directory, "publish_no_replace", record_publish)
        monkeypatch.setattr(pack.directory, "flush", lambda **kwargs: (events.append("flush:run"), run_flush(**kwargs))[1])
        monkeypatch.setattr(parent, "flush", lambda **kwargs: (events.append("flush:parent"), parent_flush(**kwargs))[1])

        pack.finalize(completed_at="2026-07-20T00:00:00Z")

        assert events == ["publish:checksums.sha256", "flush:run", "flush:parent", "publish:completion.json"]
        pack.close()


def test_failed_marker_durability_removes_marker_and_validator_rejects(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        publish = pack.directory.publish_no_replace

        def fail_after_link(created, final_name: str, **kwargs) -> None:
            publish(created, final_name, **kwargs)
            if final_name == "completion.json":
                raise PilotError("durability_error")

        monkeypatch.setattr(pack.directory, "publish_no_replace", fail_after_link)
        with pytest.raises(PilotError, match="^durability_error$"):
            pack.finalize(completed_at="2026-07-20T00:00:00Z")
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")
        pack.close()


def test_failed_marker_cleanup_poison_closes_pack_and_blocks_validation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        publish = pack.directory.publish_no_replace

        def fail_after_link(created, final_name: str, **kwargs) -> None:
            publish(created, final_name, **kwargs)
            if final_name == "completion.json":
                raise PilotError("durability_error")

        monkeypatch.setattr(pack.directory, "publish_no_replace", fail_after_link)
        monkeypatch.setattr(pack.directory, "unlink_file", lambda *_args, **_kwargs: (_ for _ in ()).throw(PilotError("durability_error")))
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            pack.finalize(completed_at="2026-07-20T00:00:00Z")
        assert pack.state == "POISONED"
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")


def test_failed_marker_rollback_flush_poison_closes_pack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        publish = pack.directory.publish_no_replace
        run_flush = pack.directory.flush
        rollback_started = False

        def fail_after_link(created, final_name: str, **kwargs) -> None:
            nonlocal rollback_started
            publish(created, final_name, **kwargs)
            if final_name == "completion.json":
                rollback_started = True
                raise PilotError("durability_error")

        def fail_rollback_flush(**kwargs) -> None:
            if rollback_started:
                raise PilotError("durability_error")
            run_flush(**kwargs)

        monkeypatch.setattr(pack.directory, "publish_no_replace", fail_after_link)
        monkeypatch.setattr(pack.directory, "flush", fail_rollback_flush)
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            pack.finalize(completed_at="2026-07-20T00:00:00Z")
        assert pack.state == "POISONED"
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")


def test_completion_close_failure_after_success_poison_closes_pack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        create_file = pack.directory.create_file
        calls = 0

        def fail_completion_close(name: str, **kwargs):
            nonlocal calls
            created = create_file(name, **kwargs)
            calls += 1
            if calls == 2:
                close = created.close

                def close_then_fail() -> None:
                    close()
                    raise OSError("completion close failed")

                monkeypatch.setattr(created, "close", close_then_fail)
            return created

        monkeypatch.setattr(pack.directory, "create_file", fail_completion_close)
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            pack.finalize(completed_at="2026-07-20T00:00:00Z")
        assert pack.state == "POISONED"
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")


def test_publish_rollback_attempts_close_unlink_and_explicit_run_flush(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        create_file = pack.directory.create_file
        publish = pack.directory.publish_no_replace
        run_flush = pack.directory.flush
        calls = 0
        rollback_started = False
        rollback_flushes = 0

        def fail_completion_close(name: str, **kwargs):
            nonlocal calls
            created = create_file(name, **kwargs)
            calls += 1
            if calls == 2:
                close = created.close

                def close_then_fail() -> None:
                    close()
                    raise OSError("completion close failed")

                monkeypatch.setattr(created, "close", close_then_fail)
            return created

        def fail_after_link(created, final_name: str, **kwargs) -> None:
            nonlocal rollback_started
            publish(created, final_name, **kwargs)
            if final_name == "completion.json":
                rollback_started = True
                raise PilotError("durability_error")

        def record_flush(**kwargs) -> None:
            nonlocal rollback_flushes
            if rollback_started:
                rollback_flushes += 1
            run_flush(**kwargs)

        monkeypatch.setattr(pack.directory, "create_file", fail_completion_close)
        monkeypatch.setattr(pack.directory, "publish_no_replace", fail_after_link)
        monkeypatch.setattr(pack.directory, "flush", record_flush)
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            pack.finalize(completed_at="2026-07-20T00:00:00Z")
        assert rollback_flushes == 1
        with parent.open_dir("run-1", error_code="incomplete_output") as run:
            assert "completion.json" not in run.enumerate()


@pytest.mark.skipif(os.name != "nt", reason="named pending files are a Windows implementation detail")
def test_aborted_windows_payload_unlinks_named_pending_file(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        writer = pack.create_payload("payload.bin")
        temporary_name = writer._created.name
        assert temporary_name in pack.directory.enumerate()
        writer.abort()
        assert temporary_name not in pack.directory.enumerate()
        pack.close()


@pytest.mark.skipif(os.name != "nt", reason="named pending files are a Windows implementation detail")
def test_failed_windows_payload_publish_unlinks_named_pending_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        writer = pack.create_payload("payload.bin")
        temporary_name = writer._created.name
        monkeypatch.setattr(pack.directory, "publish_no_replace", lambda *_args, **_kwargs: (_ for _ in ()).throw(PilotError("incomplete_output")))
        with pytest.raises(PilotError, match="^incomplete_output$"):
            writer.close()
        assert temporary_name not in pack.directory.enumerate()
        pack.close()


@pytest.mark.skipif(os.name != "nt", reason="case aliases are a Windows filesystem behavior")
def test_poison_key_uses_parent_handle_identity_across_case_alias(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        create_file = pack.directory.create_file
        calls = 0

        def fail_completion_close(name: str, **kwargs):
            nonlocal calls
            created = create_file(name, **kwargs)
            calls += 1
            if calls == 2:
                close = created.close

                def close_then_fail() -> None:
                    close()
                    raise OSError("close failed")

                monkeypatch.setattr(created, "close", close_then_fail)
            return created

        monkeypatch.setattr(pack.directory, "create_file", fail_completion_close)
        with pytest.raises(PilotError, match="^indeterminate_durability$"):
            pack.finalize(completed_at="2026-07-20T00:00:00Z")
        alias = Path(str(tmp_path).swapcase())
        with secure_open_dir(alias, error_code="unsafe_parent") as alias_parent:
            with pytest.raises(PilotError, match="^indeterminate_durability$"):
                validate_output_pack(alias_parent, "run-1", expected_pack_schema_version="pack-v1")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda valid: canonical_json_bytes({**json.loads(valid), "extra": "forbidden"}),
        lambda _valid: b'{"schema_version":"output-completion-v1","schema_version":"duplicate"}\n',
        lambda _valid: b'{"schema_version":NaN}\n',
        lambda valid: json.dumps(json.loads(valid), indent=2).encode("utf-8"),
    ],
    ids=("extra-key", "duplicate-key", "nonfinite", "noncanonical"),
)
def test_completion_json_must_be_strict_exact_and_canonical(tmp_path: Path, mutate) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        pack.finalize(completed_at="2026-07-20T00:00:00Z")
        with pack.directory.open_file("completion.json", error_code="incomplete_output") as marker:
            valid = marker.read_all(max_bytes=1024 * 1024)
        _replace_completion(pack, mutate(valid))
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")
        pack.close()


def test_open_validated_pack_yields_same_retained_handle_and_exact_payload_allowlist(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        completed = pack.finalize(completed_at="2026-07-20T00:00:00Z")
        with open_validated_output_pack(
            parent,
            "run-1",
            expected_pack_schema_version="pack-v1",
            expected_payloads=("payload.bin",),
        ) as (directory, validated):
            retained_handle = directory.native_handle
            assert validated == completed
            with directory.open_file("payload.bin", error_code="incomplete_output") as payload:
                assert payload.read_all(max_bytes=100) == b"payload"
            assert directory.native_handle == retained_handle
        with pytest.raises(ValueError, match="closed capability"):
            directory.enumerate()
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(
                parent,
                "run-1",
                expected_pack_schema_version="pack-v1",
                expected_payloads=("different.bin",),
            )
        pack.close()


def test_validator_rejects_wrong_expected_pack_schema(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        pack.finalize(completed_at="2026-07-20T00:00:00Z")
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v2")
        pack.close()


@pytest.mark.skipif(os.name != "nt", reason="NTFS durability eligibility is a Windows requirement")
def test_windows_non_ntfs_is_rejected_before_run_directory_creation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        monkeypatch.setattr(parent._backend.api, "filesystem_name", lambda _handle: "ReFS")
        with pytest.raises(PilotError, match="^durability_error$"):
            _pack(parent)
        assert not (tmp_path / "run-1").exists()


@pytest.mark.skipif(os.name != "nt", reason="private DACL is a Windows requirement")
def test_windows_private_run_directory_has_protected_operator_and_system_dacl_and_flush_access(tmp_path: Path) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        api = pack.directory._backend.api
        owner, dacl = api.owner_sid_and_dacl_sddl(pack.directory.native_handle)
        assert owner == api._current_user_sid()
        assert dacl == api.private_directory_sddl()
        assert "D:P" in dacl and ";;;SY)" in dacl
        pack.write_payload("nested/child.bin", b"x")
        with pack.directory.open_dir("nested", error_code="incomplete_output") as nested:
            nested_owner, nested_dacl = api.owner_sid_and_dacl_sddl(nested.native_handle)
            assert nested_owner == api._current_user_sid()
            assert nested_dacl == api.private_directory_sddl()
            with nested.open_file("child.bin", error_code="incomplete_output") as child:
                child_owner, child_dacl = api.owner_sid_and_dacl_sddl(child.native_handle)
                assert child_owner == api._current_user_sid()
                assert child_dacl == api.private_directory_sddl()
        pack.finalize(completed_at="2026-07-20T00:00:00Z")
        for control_name in ("checksums.sha256", "completion.json"):
            with pack.directory.open_file(control_name, error_code="incomplete_output") as control:
                control_owner, control_dacl = api.owner_sid_and_dacl_sddl(control.native_handle)
                assert control_owner == api._current_user_sid()
                assert control_dacl == api.private_directory_sddl()
        pack.close()


@pytest.mark.skipif(os.name != "nt", reason="owner SID and DACL authenticity are Windows requirements")
def test_windows_validator_rejects_wrong_owner_even_with_expected_dacl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        pack = _pack(parent)
        pack.write_payload("payload.bin", b"payload")
        pack.finalize(completed_at="2026-07-20T00:00:00Z")
        api = parent._backend.api
        monkeypatch.setattr(api, "owner_sid_and_dacl_sddl", lambda _handle: ("S-1-5-21-wrong", api.private_directory_sddl()))
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "run-1", expected_pack_schema_version="pack-v1")
        pack.close()


@pytest.mark.skipif(os.name != "nt", reason="protected DACL authenticity is a Windows requirement")
def test_windows_validator_rejects_attacker_pack_under_shared_parent(tmp_path: Path) -> None:
    attacker = tmp_path / "attacker-pack"
    attacker.mkdir()
    with secure_open_dir(tmp_path, error_code="unsafe_parent") as parent:
        with parent.open_dir("attacker-pack", error_code="incomplete_output") as attacker_directory:
            _owner, attacker_dacl = parent._backend.api.owner_sid_and_dacl_sddl(attacker_directory.native_handle)
            assert attacker_dacl != parent._backend.api.private_directory_sddl()
        with pytest.raises(PilotError, match="^incomplete_output$"):
            validate_output_pack(parent, "attacker-pack", expected_pack_schema_version="pack-v1")
