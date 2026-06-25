# ruff: noqa: F403,F405
"""Placeholder algorithm for QuantConnect project loading.

The A3 parity check is a Research notebook workflow. This file exists only
because QuantConnect cloud projects expect an algorithm class in main.py.
"""

from AlgorithmImports import *


class QCA3ParityPlaceholder(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2026, 6, 24)
        self.set_end_date(2026, 6, 25)
        self.set_cash(100000)
        self.debug(
            "qc-a3-parity is diagnostic-only; run qc_a3_parity.ipynb in Research."
        )
