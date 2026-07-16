from __future__ import annotations

import importlib
import sys
from types import ModuleType


MODULE_NAME = "harness.phase0q_cloud.backtest_main"


def test_empty_algorithm_imports_namespace_is_treated_as_lean_absent(
    monkeypatch,
) -> None:
    empty_namespace = ModuleType("AlgorithmImports")
    monkeypatch.setitem(sys.modules, "AlgorithmImports", empty_namespace)
    previous = sys.modules.pop(MODULE_NAME, None)
    try:
        module = importlib.import_module(MODULE_NAME)
        assert not hasattr(module, "OpenMacroV03Phase0QCloudBacktest")
    finally:
        sys.modules.pop(MODULE_NAME, None)
        if previous is not None:
            sys.modules[MODULE_NAME] = previous
