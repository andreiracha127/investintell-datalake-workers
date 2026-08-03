"""The pre-activation evidence replay stays OUT of the per-PR gate — and stays alive.

Wave 4 moved the frozen pre-activation suites (shadow, shadow pilot, external
executor handshake, controlled shadow, controlled activation proposal, dark
launch, A5 preflight, runtime integration skeleton, direct-activation plan) off
the ``quant_changed`` gate. Those suites pin ``Final`` sha256 constants over one
historical bundle and a one-second execution window, so they cannot fail for a
reason a new commit caused; running ~560 of them on every pull request that
touches anything under ``src/`` bought no signal.

Nothing was deleted or archived. This test is the two-sided guard:

* the suites are absent from the per-PR gate step AND the step filters them out
  by marker, so re-adding a path cannot silently put the ceremony back; and
* every one of them is still named by ``preactivation-evidence.yml`` and still
  carries the ``preactivation`` marker, so they cannot quietly stop running
  either.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PREACTIVATION_WORKFLOW = ROOT / ".github" / "workflows" / "preactivation-evidence.yml"

# The suites that were moved off the per-PR gate.
PREACTIVATION_SUITES = (
    "tests/test_a5_preflight_readiness.py",
    "tests/test_controlled_activation_proposal.py",
    "tests/test_controlled_shadow.py",
    "tests/test_dark_launch_readiness.py",
    "tests/test_direct_activation_plan.py",
    "tests/test_external_executor_handshake.py",
    "tests/test_runtime_integration_skeleton.py",
    "tests/test_shadow_pilot.py",
    "tests/test_shadow_readiness.py",
)


def _workflow(path: Path) -> dict:
    return yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def _gate_step_run() -> str:
    steps = _workflow(CI_WORKFLOW)["jobs"]["quant-engine"]["steps"]
    for step in steps:
        if step.get("name") == "Run governance and quant-engine tests":
            return str(step["run"])
    raise AssertionError("quant gate step not found in ci.yml")


@pytest.mark.parametrize("suite", PREACTIVATION_SUITES)
def test_preactivation_suite_is_not_in_the_per_pr_gate(suite: str) -> None:
    assert suite not in _gate_step_run(), (
        f"{suite} is frozen pre-activation evidence; it belongs in "
        "preactivation-evidence.yml, not in the per-PR quant gate"
    )


def test_per_pr_gate_deselects_the_preactivation_marker() -> None:
    """Belt-and-braces: even a re-added path is filtered out by marker."""
    assert '-m "not preactivation"' in _gate_step_run()


@pytest.mark.parametrize("suite", PREACTIVATION_SUITES)
def test_preactivation_suite_still_runs_on_demand(suite: str) -> None:
    text = PREACTIVATION_WORKFLOW.read_text(encoding="utf-8")
    assert suite in text, f"{suite} lost its on-demand home"


def test_preactivation_workflow_is_manual_and_scheduled() -> None:
    workflow = _workflow(PREACTIVATION_WORKFLOW)
    triggers = workflow["on"]
    assert "workflow_dispatch" in triggers
    assert "schedule" in triggers, "keep a bit-rot canary so the evidence cannot rot silently"
    # It must never re-attach to pull requests.
    assert "pull_request" not in triggers
    assert "push" not in triggers


@pytest.mark.parametrize("suite", PREACTIVATION_SUITES)
def test_preactivation_suite_carries_the_marker(suite: str) -> None:
    """The marker is what makes ``-m "not preactivation"`` meaningful."""
    module = ast.parse((ROOT / suite).read_text(encoding="utf-8"))
    marked = any(
        isinstance(node, ast.Assign)
        and any(getattr(t, "id", None) == "pytestmark" for t in node.targets)
        and ast.unparse(node.value) == "pytest.mark.preactivation"
        for node in module.body
    )
    assert marked, f"{suite} must declare `pytestmark = pytest.mark.preactivation`"


def test_the_gate_still_runs_the_current_surface_suites() -> None:
    """Dissolving ceremony must not dissolve the real gate."""
    run = _gate_step_run()
    for suite in (
        "tests/input_packs",
        "tests/quant_engine",
        "tests/quant_core",
        "tests/test_p1_pack.py",
        "tests/test_calibration_candidate.py",
        "tests/test_contract_metric_backtest.py",
        "tests/test_direct_activation_stage_a.py",
        "tests/test_phase0q_harness.py",
    ):
        assert suite in run, f"{suite} must stay in the per-PR gate"
