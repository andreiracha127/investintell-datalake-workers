"""Guards for the quant-core publish gate.

Two things are being protected here:

1. the repository invariants the gate relies on (one version, declared in every
   file that has to state it — this is what silently drifted before); and
2. the comparison logic itself, so a future refactor cannot turn the gate into
   a no-op that waves a mismatched wheel through.
"""

from __future__ import annotations

import shutil
import sys
import zipfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.ci import publish_quant_core as gate  # noqa: E402


# --------------------------------------------------------------------------- #
# Repository invariants
# --------------------------------------------------------------------------- #


def test_declared_version_agrees_across_every_manifest() -> None:
    """A bump has to touch all three files, or the gate refuses to publish."""
    versions = gate.declared_versions(ROOT)

    assert versions.agreed is not None, (
        "investintell-quant-core version disagrees between "
        f"packages/investintell_quant_core/pyproject.toml ({versions.core_pyproject}), "
        f"version.py ({versions.version_module}) and the quant_engine pin ({versions.engine_pin})"
    )


def test_declared_version_is_the_one_the_engine_pins() -> None:
    assert gate.resolve_version(ROOT) == gate.declared_versions(ROOT).engine_pin


def test_tracked_sources_are_keyed_by_wheel_relative_path() -> None:
    payload = gate.tracked_sources(ROOT)

    assert "investintell_quant_core/version.py" in payload
    assert "investintell_quant_core/py.typed" in payload
    # ``allocator/`` is the subpackage that main lost while the published wheel
    # carried it; keep it explicitly in the guard.
    assert any(name.startswith("investintell_quant_core/allocator/") for name in payload)
    assert all(name.startswith("investintell_quant_core/") for name in payload)


def test_registry_coordinates_match_the_app_private_index() -> None:
    """The app resolves this exact index; drifting apart would silently split them."""
    assert gate.SIMPLE_INDEX == (
        "https://southamerica-east1-python.pkg.dev"
        "/investintell-research-analisys/python/simple/investintell-quant-core/"
    )


def test_publish_is_pinned_to_main() -> None:
    assert gate.PUBLISH_REF == "refs/heads/main"


def test_workflow_publishes_only_from_main_using_the_gate() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "quant-core-publish" in workflow
    assert "scripts/ci/publish_quant_core.py preflight" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "twine upload" in workflow


# --------------------------------------------------------------------------- #
# Version parsing
# --------------------------------------------------------------------------- #


def _write_package(tmp_path: Path, *, core: str, module: str, pin: str) -> Path:
    package = tmp_path / "packages" / "investintell_quant_core"
    module_dir = package / "src" / "investintell_quant_core"
    module_dir.mkdir(parents=True)
    (package / "pyproject.toml").write_text(
        f'[project]\nname = "investintell-quant-core"\nversion = "{core}"\n', encoding="utf-8"
    )
    (module_dir / "version.py").write_text(f'__version__ = "{module}"\n', encoding="utf-8")
    engine = tmp_path / "services" / "quant_engine"
    engine.mkdir(parents=True)
    (engine / "pyproject.toml").write_text(
        '[project]\nname = "investintell-quant-engine"\nversion = "0.1.0"\n'
        f'dependencies = ["investintell-quant-core=={pin}", "numpy>=1.26"]\n',
        encoding="utf-8",
    )
    return tmp_path


def test_resolve_version_accepts_a_consistent_bump(tmp_path: Path) -> None:
    root = _write_package(tmp_path, core="0.4.0", module="0.4.0", pin="0.4.0")

    assert gate.resolve_version(root) == "0.4.0"


@pytest.mark.parametrize(
    ("core", "module", "pin"),
    [
        ("0.4.0", "0.3.0", "0.4.0"),  # version.py forgotten
        ("0.4.0", "0.4.0", "0.3.0"),  # engine pin forgotten
        ("0.3.0", "0.4.0", "0.4.0"),  # manifest forgotten
    ],
)
def test_resolve_version_rejects_a_partial_bump(
    tmp_path: Path, core: str, module: str, pin: str
) -> None:
    root = _write_package(tmp_path, core=core, module=module, pin=pin)

    with pytest.raises(gate.VerificationError):
        gate.resolve_version(root)


def test_missing_exact_pin_is_rejected(tmp_path: Path) -> None:
    root = _write_package(tmp_path, core="0.4.0", module="0.4.0", pin="0.4.0")
    engine = root / "services" / "quant_engine" / "pyproject.toml"
    engine.write_text(
        '[project]\nname = "x"\nversion = "0.1.0"\n'
        'dependencies = ["investintell-quant-core>=0.4.0"]\n',
        encoding="utf-8",
    )

    with pytest.raises(gate.VerificationError):
        gate.resolve_version(root)


# --------------------------------------------------------------------------- #
# Wheel/source comparison
# --------------------------------------------------------------------------- #


def _wheel(path: Path, members: dict[str, bytes], *, version: str = "0.4.0") -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in members.items():
            archive.writestr(name, payload)
        archive.writestr(
            f"investintell_quant_core-{version}.dist-info/METADATA",
            f"Metadata-Version: 2.4\nName: investintell-quant-core\nVersion: {version}\n",
        )
        archive.writestr(
            f"investintell_quant_core-{version}.dist-info/WHEEL",
            "Wheel-Version: 1.0\nGenerator: setuptools (83.0.0)\n",
        )
    return path


def test_wheel_payload_drops_only_dist_info(tmp_path: Path) -> None:
    wheel = _wheel(
        tmp_path / "w.whl",
        {"investintell_quant_core/version.py": b'__version__ = "0.4.0"\n'},
    )

    assert gate.wheel_payload(wheel) == {
        "investintell_quant_core/version.py": b'__version__ = "0.4.0"\n'
    }


def test_wheel_metadata_version_is_read_from_dist_info(tmp_path: Path) -> None:
    wheel = _wheel(tmp_path / "w.whl", {"investintell_quant_core/__init__.py": b""})

    assert gate.wheel_metadata_version(wheel) == "0.4.0"


def test_compare_payloads_is_silent_when_content_matches() -> None:
    left = {"a.py": b"same", "b/c.py": b"also same"}

    assert gate.compare_payloads(left, dict(left), left_label="l", right_label="r") == []


def test_compare_payloads_reports_missing_extra_and_changed() -> None:
    problems = gate.compare_payloads(
        {"kept.py": b"x", "only_left.py": b"y", "changed.py": b"before"},
        {"kept.py": b"x", "only_right.py": b"z", "changed.py": b"after"},
        left_label="wheel",
        right_label="source",
    )

    joined = "\n".join(problems)
    assert "only_left.py" in joined
    assert "only_right.py" in joined
    assert "changed.py" in joined
    assert "kept.py" not in joined
    assert len(problems) == 3


def test_verify_wheel_against_sources_rejects_tampered_content(tmp_path: Path) -> None:
    """The whole point: a wheel whose bytes are not main's bytes must not pass."""
    payload = gate.tracked_sources(ROOT)
    version = gate.resolve_version(ROOT)
    tampered = dict(payload)
    tampered["investintell_quant_core/version.py"] = b'__version__ = "hand-built"\n'
    wheel = _wheel(
        tmp_path / f"investintell_quant_core-{version}-py3-none-any.whl",
        tampered,
        version=version,
    )

    with pytest.raises(gate.VerificationError) as excinfo:
        gate.verify_wheel_against_sources(wheel, version, ROOT)

    assert any("version.py" in problem for problem in excinfo.value.problems)


def test_verify_wheel_against_sources_rejects_a_dropped_subpackage(tmp_path: Path) -> None:
    """This is the 0.3.0 failure mode, inverted: source present, wheel missing it."""
    payload = gate.tracked_sources(ROOT)
    version = gate.resolve_version(ROOT)
    pruned = {
        name: data
        for name, data in payload.items()
        if not name.startswith("investintell_quant_core/allocator/")
    }
    assert len(pruned) < len(payload)
    wheel = _wheel(
        tmp_path / f"investintell_quant_core-{version}-py3-none-any.whl",
        pruned,
        version=version,
    )

    with pytest.raises(gate.VerificationError) as excinfo:
        gate.verify_wheel_against_sources(wheel, version, ROOT)

    assert any("allocator/" in problem for problem in excinfo.value.problems)


def test_verify_wheel_against_sources_rejects_a_version_mismatch(tmp_path: Path) -> None:
    payload = gate.tracked_sources(ROOT)
    version = gate.resolve_version(ROOT)
    wheel = _wheel(
        tmp_path / f"investintell_quant_core-{version}-py3-none-any.whl",
        payload,
        version="9.9.9",
    )

    with pytest.raises(gate.VerificationError) as excinfo:
        gate.verify_wheel_against_sources(wheel, version, ROOT)

    assert any("METADATA" in problem for problem in excinfo.value.problems)


@pytest.mark.skipif(shutil.which("git") is None, reason="git is required")
def test_real_build_matches_the_tracked_source(tmp_path: Path) -> None:
    """End-to-end: setuptools' output must equal what git says main contains."""
    pytest.importorskip("build")
    version = gate.resolve_version(ROOT)

    wheel = gate.build_wheel(tmp_path / "dist", ROOT)
    gate.verify_wheel_against_sources(wheel, version, ROOT)


# --------------------------------------------------------------------------- #
# Simple-index parsing
# --------------------------------------------------------------------------- #


SIMPLE_PAGE = """<!DOCTYPE html>
<html><body>
<a href="https://host/investintell_quant_core-0.3.0-py3-none-any.whl#sha256=abc"
   data-requires-python="&gt;=3.11">investintell_quant_core-0.3.0-py3-none-any.whl</a><br/>
</body></html>
"""


def test_published_wheel_url_finds_the_declared_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gate, "_authorized_get", lambda url, token: SIMPLE_PAGE.encode())

    url = gate.published_wheel_url("token", "0.3.0")

    assert url is not None
    assert url.startswith("https://host/investintell_quant_core-0.3.0-py3-none-any.whl")


def test_published_wheel_url_returns_none_for_an_unpublished_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gate, "_authorized_get", lambda url, token: SIMPLE_PAGE.encode())

    assert gate.published_wheel_url("token", "0.4.0") is None


def test_preflight_refuses_a_non_main_ref(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GITHUB_REF", "refs/heads/feat/something")
    monkeypatch.setenv("GAR_ACCESS_TOKEN", "token")

    with pytest.raises(gate.VerificationError) as excinfo:
        gate.run_preflight(tmp_path / "dist", None, allow_dirty=True, root=ROOT)

    assert "refs/heads/main" in str(excinfo.value)
