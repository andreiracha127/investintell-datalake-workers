"""P1 certified input pack table contract.

Reuses the immutable P0 :class:`~src.input_packs.p0_contract.TableSpec` dataclass
and normalization helpers by import (no P0 code is modified). Only the two P1
source tables are declared here.

Notes on the two tables:

* ``macro_observation_vintage`` is the point-in-time (PIT) macro vintage table the
  open_macro_v03 decision path reads via ``src/macro_pit.py latest_vintage_as_of``.
  Its natural key is ``(series_id, observation_period, vintage_date)``.
  ``available_at`` is a full ISO-8601 timestamp with timezone (not a plain date),
  so it is carried as an opaque string rather than a ``date_columns`` member.
  ``value`` and ``revision_number`` are numeric.
* ``eod_prices`` is the sleeve price table, keyed ``(ticker, date)`` with numeric
  ``close``/``adjusted_close``/``volume``. It mirrors the P0 ``eod_prices`` spec
  but is populated from the P1 sleeve export (6 sleeve tickers).
"""

from __future__ import annotations

from typing import Mapping

from src.input_packs.p0_contract import TableSpec

MACRO_OBSERVATION_VINTAGE = TableSpec(
    name="macro_observation_vintage",
    key_columns=("series_id", "observation_period", "vintage_date"),
    columns=(
        "series_id",
        "observation_period",
        "vintage_date",
        "value",
        "available_at",
        "revision_number",
        "source",
        "source_spec_version",
    ),
    numeric_columns=frozenset({"value", "revision_number"}),
    date_columns=frozenset({"observation_period", "vintage_date"}),
    as_of_column="vintage_date",
)

EOD_PRICES = TableSpec(
    name="eod_prices",
    key_columns=("ticker", "date"),
    columns=("ticker", "date", "close", "adjusted_close", "volume"),
    numeric_columns=frozenset({"close", "adjusted_close", "volume"}),
    date_columns=frozenset({"date"}),
    as_of_column="date",
)

P1_TABLE_SPECS: tuple[TableSpec, ...] = (MACRO_OBSERVATION_VINTAGE, EOD_PRICES)

P1_TABLES_BY_NAME: Mapping[str, TableSpec] = {spec.name: spec for spec in P1_TABLE_SPECS}
