"""Universe-aware, no-look-ahead matching and fund-level pilot metrics."""

from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import AbstractContextManager
from dataclasses import dataclass, fields
from datetime import date
from decimal import Decimal
from enum import StrEnum
import math
from numbers import Real
from pathlib import Path
import sqlite3
from typing import Iterable, Mapping

import pyarrow.parquet as pq

from .artifacts import commit_partial, partial_path
from .contracts import DebtState, FieldState, MatchState, PilotError
from .debt_mapping import DebtMapping, _is_loader_validated
from .identifiers import normalize_cusip9


class WeightState(StrEnum):
    NULL = "null"
    NON_NUMERIC = "non_numeric"
    NON_FINITE = "non_finite"
    NEGATIVE = "negative"
    VALID = "valid"


def _mapping_value(value: object) -> object:
    return value.value if isinstance(value, StrEnum) else value


def _record_mapping(record: object) -> dict[str, object]:
    return {field.name: _mapping_value(getattr(record, field.name)) for field in fields(record)}


@dataclass(frozen=True)
class HoldingRecord:
    publication_id: object = None
    accession_number: object = None
    holding_id: object = None
    source_run_id: object = None
    report_date: object = None
    filing_date: object = None
    series_id: object = None
    class_id: object = None
    instrument_id: object = None
    issuer_category: object = None
    original_cusip: object = None
    signed_market_value: object = None
    signed_pct_of_nav: object = None
    currency: object = None
    raw_values: Mapping[str, object] | None = None

    def to_mapping(self) -> dict[str, object]:
        return _record_mapping(self)


@dataclass(frozen=True)
class Observation:
    cusip: str
    observation_date: str
    source_row_number: int
    price: object
    price_state: object
    ytm: object
    db_type: object
    db_type_state: object
    daily_key_state: object

    def to_mapping(self) -> dict[str, object]:
        return _record_mapping(self)


@dataclass(frozen=True)
class MatchResult:
    holding: HoldingRecord
    state: MatchState
    normalized_cusip9: str | None = None
    observation_date: str | None = None
    observations: tuple[Observation, ...] = ()
    observation_age_days: int | None = None
    is_144a: bool | None = None

    def to_mapping(self) -> dict[str, object]:
        return {
            "holding": self.holding.to_mapping(),
            "state": self.state.value,
            "normalized_cusip9": self.normalized_cusip9,
            "observation_date": self.observation_date,
            "observations": [row.to_mapping() for row in self.observations],
            "observation_age_days": self.observation_age_days,
            "is_144a": self.is_144a,
        }


@dataclass(frozen=True)
class SeriesMetric:
    series_id: object
    report_date: object
    publication_id: object
    source_run_id: object
    state_counts: Mapping[str, int]
    denominator_diagnostics: Mapping[str, int]
    denominator_weight: float | None
    numerator_weight: float | None
    nav_ratio: float | None
    eligible_market_value_by_currency: Mapping[str, float]
    matched_market_value_by_currency: Mapping[str, float]
    market_value_diagnostics: Mapping[str, int]

    def to_mapping(self) -> dict[str, object]:
        return _record_mapping(self)


@dataclass(frozen=True)
class CrossSeriesSummary:
    metric: str
    p25: float | None
    median: float | None
    p75: float | None
    count: int
    excluded_count: int
    excluded_reasons: Mapping[str, int]

    def to_mapping(self) -> dict[str, object]:
        return {
            "metric": self.metric,
            "p25": self.p25,
            "median": self.median,
            "p75": self.p75,
            "count": self.count,
            "excluded_count": self.excluded_count,
            "excluded_reasons": dict(self.excluded_reasons),
        }


def _date(value: object) -> date:
    if not isinstance(value, str):
        raise PilotError("invalid_match_date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise PilotError("invalid_match_date", {"value": value}) from exc


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


class ObservationIndex(AbstractContextManager["ObservationIndex"]):
    """On-disk as-of index that retains source-row multiplicity."""

    def __init__(self, index_path: str | Path, connection: sqlite3.Connection) -> None:
        self.index_path = Path(index_path)
        self._connection = connection

    @classmethod
    def build(cls, panel_path: str | Path, index_path: str | Path, cohort_cusips: Iterable[object]) -> "ObservationIndex":
        destination = Path(index_path)
        if destination.exists():
            raise PilotError("already_exists", {"path": str(destination)})
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = partial_path(destination)
        cohort = {
            item.normalized_cusip9
            for value in cohort_cusips
            if (item := normalize_cusip9(value)).normalized_cusip9 is not None
        }
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(partial)
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("CREATE TABLE universe (cusip TEXT PRIMARY KEY) WITHOUT ROWID")
            connection.execute(
                "CREATE TABLE observations ("
                "cusip TEXT NOT NULL, observation_date TEXT NOT NULL, source_row_number INTEGER NOT NULL, "
                "price, price_state TEXT, ytm, db_type, db_type_state TEXT, daily_key_state TEXT, "
                "PRIMARY KEY (cusip, observation_date, source_row_number)) WITHOUT ROWID"
            )
            connection.executemany("INSERT INTO universe (cusip) VALUES (?)", ((value,) for value in sorted(cohort)))
            with pq.ParquetFile(panel_path) as panel:
                available = set(panel.schema_arrow.names)
                needed = {"normalized_cusip9", "observation_date", "source_row_number"}
                if not needed.issubset(available):
                    raise PilotError("missing_panel_columns", {"columns": sorted(needed - available)})
                optional = [name for name in ("pr", "pr_state", "ytm", "db_type", "db_type_state", "daily_key_state", "observation_date_state") if name in available]
                columns = ["normalized_cusip9", "observation_date", "source_row_number", *optional]
                for batch in panel.iter_batches(columns=columns):
                    values = batch.to_pydict()
                    rows: list[tuple[object, ...]] = []
                    for index, raw_cusip in enumerate(values["normalized_cusip9"]):
                        cusip = normalize_cusip9(raw_cusip).normalized_cusip9
                        raw_date = values["observation_date"][index]
                        date_state = values.get("observation_date_state", ["present"] * batch.num_rows)[index]
                        if cusip not in cohort or date_state != FieldState.PRESENT.value:
                            continue
                        try:
                            observed_on = _date(raw_date).isoformat()
                        except PilotError:
                            continue
                        source_row = values["source_row_number"][index]
                        if isinstance(source_row, bool) or not isinstance(source_row, int):
                            continue
                        rows.append(
                            (
                                cusip,
                                observed_on,
                                source_row,
                                values.get("pr", [None] * batch.num_rows)[index],
                                values.get("pr_state", [None] * batch.num_rows)[index],
                                values.get("ytm", [None] * batch.num_rows)[index],
                                values.get("db_type", [None] * batch.num_rows)[index],
                                values.get("db_type_state", [FieldState.INVALID.value] * batch.num_rows)[index],
                                values.get("daily_key_state", [None] * batch.num_rows)[index],
                            )
                        )
                    if rows:
                        connection.executemany("INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
            connection.commit()
            connection.close()
            connection = None
            commit_partial(partial, destination)
            return cls(destination, sqlite3.connect(destination))
        except Exception:
            if connection is not None:
                connection.close()
            partial.unlink(missing_ok=True)
            Path(f"{partial}-journal").unlink(missing_ok=True)
            raise

    def is_universe_member(self, cusip: str) -> bool:
        return self._connection.execute("SELECT 1 FROM universe WHERE cusip = ?", (cusip,)).fetchone() is not None

    @staticmethod
    def _observation(row: tuple[object, ...]) -> Observation:
        return Observation(*row)  # type: ignore[arg-type]

    def lookup_asof(self, cusip: str, report_date: object) -> tuple[Observation, ...]:
        as_of = _date(report_date).isoformat()
        row = self._connection.execute(
            "SELECT MAX(observation_date) FROM observations WHERE cusip = ? AND observation_date <= ?", (cusip, as_of)
        ).fetchone()
        if row is None or row[0] is None:
            return ()
        rows = self._connection.execute(
            "SELECT cusip, observation_date, source_row_number, price, price_state, ytm, db_type, db_type_state, daily_key_state "
            "FROM observations WHERE cusip = ? AND observation_date = ? ORDER BY source_row_number",
            (cusip, row[0]),
        ).fetchall()
        return tuple(self._observation(row) for row in rows)

    def latest_rows(self) -> tuple[Observation, ...]:
        rows = self._connection.execute(
            "SELECT o.cusip, o.observation_date, o.source_row_number, o.price, o.price_state, o.ytm, o.db_type, o.db_type_state, o.daily_key_state "
            "FROM observations o JOIN (SELECT cusip, MAX(observation_date) AS observation_date FROM observations GROUP BY cusip) latest "
            "ON o.cusip = latest.cusip AND o.observation_date = latest.observation_date "
            "ORDER BY o.cusip, o.source_row_number"
        ).fetchall()
        return tuple(self._observation(row) for row in rows)

    def close(self) -> None:
        self._connection.close()

    def __exit__(self, *_: object) -> None:
        self.close()


def _state_for_category(state: DebtState) -> MatchState:
    return MatchState(state.value)


def _is_144a(observation: Observation | None) -> bool | None:
    if observation is None or observation.db_type_state != FieldState.PRESENT.value:
        return None
    value = _finite_number(observation.db_type)
    if value is None or not value.is_integer():
        return None
    return int(value) == 3


def _require_valid_mapping(mapping: object) -> DebtMapping:
    if not isinstance(mapping, DebtMapping) or not _is_loader_validated(mapping):
        raise PilotError("debt_mapping_unapproved")
    return mapping


def match_holding(
    holding: HoldingRecord,
    debt_mapping: DebtMapping,
    observations: ObservationIndex,
    global_start: object,
    cutoff: object,
) -> MatchResult:
    """Match a holding according to the fixed, category-first state precedence."""
    if not isinstance(observations, ObservationIndex):
        raise PilotError("invalid_observation_index")
    debt_mapping = _require_valid_mapping(debt_mapping)
    category_state = debt_mapping.classify(holding.issuer_category)
    if category_state is not DebtState.DEBT_LIKE_ELIGIBLE:
        return MatchResult(holding, _state_for_category(category_state))
    identifier = normalize_cusip9(holding.original_cusip)
    if identifier.normalized_cusip9 is None:
        return MatchResult(holding, MatchState.INVALID_IDENTIFIER)
    report_date = _date(holding.report_date)
    if report_date < _date(global_start):
        return MatchResult(holding, MatchState.OUTSIDE_WINDOW_BEFORE_SOURCE, identifier.normalized_cusip9)
    if report_date > _date(cutoff):
        return MatchResult(holding, MatchState.OUTSIDE_WINDOW_AFTER_CUTOFF, identifier.normalized_cusip9)
    if not observations.is_universe_member(identifier.normalized_cusip9):
        return MatchResult(holding, MatchState.UNMATCHED_NO_CUSIP, identifier.normalized_cusip9)
    rows = observations.lookup_asof(identifier.normalized_cusip9, holding.report_date)
    if not rows:
        return MatchResult(holding, MatchState.UNMATCHED_NO_PRIOR_OBSERVATION, identifier.normalized_cusip9)
    observation_date = _date(rows[0].observation_date)
    age = (report_date - observation_date).days
    if age >= 31:
        return MatchResult(holding, MatchState.STALE, identifier.normalized_cusip9, observation_date.isoformat(), rows, age)
    unique = rows[0] if len(rows) == 1 else None
    if unique is None or unique.price_state != FieldState.PRESENT.value or _finite_number(unique.price) is None:
        return MatchResult(holding, MatchState.UNAVAILABLE_AMBIGUOUS, identifier.normalized_cusip9, observation_date.isoformat(), rows, age)
    return MatchResult(
        holding, MatchState.MATCHED, identifier.normalized_cusip9, observation_date.isoformat(), rows, age, _is_144a(unique)
    )


def match_holdings_asof(
    holdings: Iterable[HoldingRecord],
    debt_mapping: DebtMapping,
    observations: ObservationIndex,
    global_start: object,
    cutoff: object,
) -> tuple[MatchResult, ...]:
    """Historical-only batch matching; the latest-output lane is never accepted."""
    debt_mapping = _require_valid_mapping(debt_mapping)
    if not isinstance(observations, ObservationIndex):
        raise PilotError("invalid_observation_index")
    return tuple(match_holding(holding, debt_mapping, observations, global_start, cutoff) for holding in holdings)


def classify_weight(value: object) -> WeightState:
    if value is None:
        return WeightState.NULL
    number = _finite_number(value)
    if number is None:
        return WeightState.NON_NUMERIC if not isinstance(value, (Real, Decimal)) or isinstance(value, bool) else WeightState.NON_FINITE
    return WeightState.NEGATIVE if number < 0 else WeightState.VALID


def _currency(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _numeric_reason(value: object) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (Real, Decimal)):
        return "non_numeric"
    return None if _finite_number(value) is not None else "non_finite"


def _add_finite_amount(
    totals: dict[str, float], poisoned: set[str], currency: str, amount: float, diagnostics: Counter[str]
) -> None:
    if currency in poisoned:
        return
    total = totals.get(currency, 0.0) + amount
    if not math.isfinite(total):
        totals.pop(currency, None)
        poisoned.add(currency)
        diagnostics["aggregate_non_finite"] += 1
        return
    totals[currency] = total


def _eligible(match: MatchResult) -> bool:
    return (
        normalize_cusip9(match.holding.original_cusip).normalized_cusip9 is not None
        and match.holding.issuer_category is not None
        and match.state not in {MatchState.INELIGIBLE_NON_DEBT, MatchState.AMBIGUOUS_CATEGORY, MatchState.MISSING_CATEGORY, MatchState.INVALID_IDENTIFIER}
    )


def compute_series_metrics(matches: Iterable[MatchResult]) -> tuple[SeriesMetric, ...]:
    groups: dict[tuple[object, object], list[MatchResult]] = defaultdict(list)
    for match in matches:
        groups[(match.holding.series_id, match.holding.report_date)].append(match)
    metrics: list[SeriesMetric] = []
    for (series_id, report_date), rows in groups.items():
        lineages = {(row.holding.publication_id, row.holding.source_run_id) for row in rows}
        if len(lineages) != 1:
            raise PilotError("mixed_series_lineage", {"series_id": series_id, "report_date": report_date})
        publication_id, source_run_id = next(iter(lineages))
        states = Counter(row.state.value for row in rows)
        diagnostics = Counter()
        market_diagnostics = Counter()
        denominator = 0.0
        numerator = 0.0
        nav_aggregate_non_finite = False
        eligible_value: dict[str, float] = defaultdict(float)
        matched_value: dict[str, float] = defaultdict(float)
        eligible_poisoned: set[str] = set()
        matched_poisoned: set[str] = set()
        for row in rows:
            if not _eligible(row):
                continue
            weight_state = classify_weight(row.holding.signed_pct_of_nav)
            if weight_state is WeightState.VALID:
                weight = _finite_number(row.holding.signed_pct_of_nav)
                assert weight is not None
                candidate = denominator + weight
                if not math.isfinite(candidate):
                    nav_aggregate_non_finite = True
                else:
                    denominator = candidate
                if row.state is MatchState.MATCHED:
                    candidate = numerator + weight
                    if not math.isfinite(candidate):
                        nav_aggregate_non_finite = True
                    else:
                        numerator = candidate
            else:
                diagnostics[weight_state.value] += 1
            amount_reason = _numeric_reason(row.holding.signed_market_value)
            currency = _currency(row.holding.currency)
            if amount_reason is not None:
                market_diagnostics[amount_reason] += 1
            if currency is None:
                market_diagnostics["missing_currency"] += 1
            if amount_reason is None and currency is not None:
                value = _finite_number(row.holding.signed_market_value)
                assert value is not None
                _add_finite_amount(eligible_value, eligible_poisoned, currency, value, market_diagnostics)
                if row.state is MatchState.MATCHED:
                    _add_finite_amount(matched_value, matched_poisoned, currency, value, market_diagnostics)
        if nav_aggregate_non_finite:
            diagnostics["aggregate_non_finite"] += 1
            denominator_output = None
            numerator_output = None
            ratio = None
        else:
            denominator_output = denominator
            numerator_output = numerator
            ratio = numerator / denominator if denominator else None
        if ratio is None and not nav_aggregate_non_finite:
            diagnostics["zero_valid_denominator"] += 1
        metrics.append(
            SeriesMetric(
                series_id,
                report_date,
                publication_id,
                source_run_id,
                dict(states),
                dict(diagnostics),
                denominator_output,
                numerator_output,
                ratio,
                dict(eligible_value),
                dict(matched_value),
                dict(market_diagnostics),
            )
        )
    return tuple(metrics)


def _quantile(values: list[float], quantile: float) -> float:
    position = (len(values) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def compute_cross_series_summary(metrics: Iterable[SeriesMetric]) -> CrossSeriesSummary:
    values: list[float] = []
    excluded = Counter()
    for metric in metrics:
        if metric.nav_ratio is None:
            reason = "aggregate_non_finite" if metric.denominator_diagnostics.get("aggregate_non_finite") else "zero_valid_denominator"
            excluded[reason] += 1
        elif not math.isfinite(metric.nav_ratio):
            excluded["non_finite_metric"] += 1
        else:
            values.append(metric.nav_ratio)
    values.sort()
    return CrossSeriesSummary(
        "nav_match_ratio",
        _quantile(values, 0.25) if values else None,
        _quantile(values, 0.5) if values else None,
        _quantile(values, 0.75) if values else None,
        len(values),
        sum(excluded.values()),
        dict(excluded),
    )
