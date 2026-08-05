"""Operator tooling for the SEC N-PORT DERA quarterly bulk datasets.

These two modules are the entire write path of ``sec_nport_holdings`` — 96M
rows, the most-read table in the datalake. Until 2026-08-05 they lived only on
one operator's disk, outside every repository, with no test and no post-load
check. Two quarterly packages (``2023q4``, ``2025q1``) were loaded with a
partially populated ``HOLDING_ID -> ISIN`` map and nobody noticed for one and
two years respectively, because a package that loses its ISIN side still loads:
every row arrives, every other column is populated, the job goes green.

See ``docs/runbooks/nport-identifier-coverage.md`` and
``src/workers/nport_identifier_coverage.py``.
"""
