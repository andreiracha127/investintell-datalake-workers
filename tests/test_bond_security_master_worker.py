"""The security master's publication-identity revision ladder.

The deployed image carries no ``.git``, so the ladder is what keeps the
``bond_security_v1`` publication identity moving with the code. The historical
gap (``CODE_REVISION`` only, then git) resolved to "unknown" in production,
which froze the identity: the daily chain re-pointed to a pre-withholding v1
build and the serving refresh failed its fund-exposure identity guard on the
cross-identity ISIN aliases that build carries (production, 2026-09-17/18).
"""
from __future__ import annotations

import pytest

from src.workers import bond_security_master


def _clear_revision_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every rung starts absent: the host's own CI env must not leak into a rung."""
    for name in bond_security_master._REVISION_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_revision_ladder_covers_the_railway_deploy_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same ladder, same order, as the sibling publication workers."""
    assert bond_security_master._REVISION_ENV_VARS == (
        "CODE_REVISION", "GIT_SHA", "SOURCE_COMMIT", "RAILWAY_GIT_COMMIT_SHA",
    )


def test_code_revision_prefers_explicit_pins_over_the_deploy_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Precedence, weakest rung first: an explicit pin outranks the injected sha.

    ``CODE_REVISION`` wins because it is a DELIBERATE choice (a replay that must
    land on a known publication). Railway's per-deploy sha is what moves the
    identity with no operator action -- which is why a permanently-set
    ``CODE_REVISION`` is the trap: it shadows the sha and freezes the identity.
    """
    _clear_revision_env(monkeypatch)
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", "railway-sha")
    assert bond_security_master._code_revision() == "railway-sha"
    monkeypatch.setenv("SOURCE_COMMIT", "source-sha")
    assert bond_security_master._code_revision() == "source-sha"
    monkeypatch.setenv("GIT_SHA", "git-sha")
    assert bond_security_master._code_revision() == "git-sha"
    monkeypatch.setenv("CODE_REVISION", "code-revision")
    assert bond_security_master._code_revision() == "code-revision"


def test_code_revision_uses_the_railway_deploy_sha_when_nothing_is_pinned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production case: no pin, no ``.git``, and the identity still moves."""
    _clear_revision_env(monkeypatch)
    sha = "cfa628e552a96eedc142e4e24e19410e56317f8a"
    monkeypatch.setenv("RAILWAY_GIT_COMMIT_SHA", sha)
    # git must not be consulted at all once a rung resolved.
    monkeypatch.setattr(
        bond_security_master.subprocess, "run",
        lambda *a, **k: pytest.fail("git consulted while an env rung was set"),
    )
    assert bond_security_master._code_revision() == sha


def test_code_revision_falls_back_to_git_then_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Developer checkout resolves the short HEAD; a bare image degrades to 'unknown'."""
    _clear_revision_env(monkeypatch)
    monkeypatch.setattr(
        bond_security_master.subprocess, "run",
        lambda *a, **k: type(
            "Result", (), {"stdout": "abcd123\n", "stderr": ""}
        )(),
    )
    assert bond_security_master._code_revision() == "abcd123"
    monkeypatch.setattr(
        bond_security_master.subprocess, "run",
        lambda *a, **k: type("Result", (), {"stdout": "", "stderr": ""})(),
    )
    assert bond_security_master._code_revision() == "unknown"
