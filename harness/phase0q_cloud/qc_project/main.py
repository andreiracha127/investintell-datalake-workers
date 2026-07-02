# ruff: noqa: F403,F405
"""Placeholder algorithm for QuantConnect project loading.

The phase0q cloud-leg reproducibility check is a Research notebook workflow. This
file exists only because QuantConnect cloud projects expect an algorithm class in
main.py. It must never be run as a backtest.
"""

from AlgorithmImports import *


class OpenMacroV03Phase0QCloudLegPlaceholder(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2026, 7, 1)
        self.set_end_date(2026, 7, 2)
        self.set_cash(100000)
        raise RuntimeError(
            "open_macro_v03_phase0q_harness is diagnostic-only. Do not run this "
            "project as a backtest; open and run phase0q_cloud_leg.ipynb in the "
            "QuantConnect Research environment. A5 stays blocked; this project "
            "grants no activation."
        )
