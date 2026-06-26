from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from scripts import repeatability_matrix as rm


def test_container_runner_preflights_isolated_mounts(
    tmp_path: Path, monkeypatch
) -> None:
    combo_root = tmp_path / "combo"
    combo_root.mkdir()
    out_dir = tmp_path / "out"
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return SimpleNamespace(stdout="")

    monkeypatch.setattr(rm.subprocess, "run", fake_run)

    rm._run_container(
        rm.G0,
        combo_root=combo_root,
        out_dir=out_dir,
        jobs=1,
        image="quant-engine:test",
        worker_commit="abc123",
    )

    assert len(calls) == 2
    preflight, actual = calls
    assert "--entrypoint" in preflight
    assert "/bin/sh" in preflight
    assert rm._container_isolation_probe_script() in preflight
    combo_mount = f"type=bind,src={combo_root.resolve()},dst=/input/combo,readonly"
    output_mount = f"type=bind,src={out_dir.resolve()},dst=/outputs"
    assert combo_mount in preflight
    assert output_mount in preflight
    assert combo_mount in actual
    assert output_mount in actual
    assert "--entrypoint" not in actual
    assert out_dir.exists()
