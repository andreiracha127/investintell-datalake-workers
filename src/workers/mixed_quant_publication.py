"""Registered materializer for the mixed_quant_v1 point-in-time publication.

This turns the artifact-only pure computations into a production writer: it
resolves stable instrument identities, folds alias history, records observed
returns and income, and wraps the pure V2 look-through engine
(``nport_v2_lookthrough.expand_series``) to derive exposures. Everything lands in
one inactive publication under an advisory lock, stage by stage with
checkpoints, so the build is reproducible and restartable. Promotion to the
active pointer is a separate, atomic step (``publication.promote``).

Contract: ``run(dsn, *, calc_date=None, limit=None) -> dict`` (see src/run.py).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import os
import subprocess
from typing import Any, Callable
import uuid

from src.db import LOCK_MIXED_QUANT_PUBLICATION, advisory_lock, connect, resolve_dsn
from src.quant_data import publication as pub
from src.quant_data.contracts import (
    NAMED_BOND_FACTORS,
    IdentityObservation,
    ResolvedInstrument,
    mint_instrument_id,
    resolve_identities,
    validate_bond_factor_row,
    validate_class_factor_row,
)
from src.workers.nport_v2_lookthrough import SYNTHETIC_PREFIXES, _exposure_rows, expand_series

PRODUCT = pub.PRODUCT
_OBSERVATION_TABLES = (
    "mixed_quant_identity_observation",
    "mixed_quant_return_observation",
    "mixed_quant_income_observation",
    "mixed_quant_holding_observation",
    "mixed_quant_class_factor_observation",
    "mixed_quant_bond_factor_observation",
)


def _git_revision() -> str | None:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        return out.stdout.strip() or None
    except Exception:
        return None


def _resolve_as_of(conn: Any, calc_date: str | None) -> date | None:
    if calc_date:
        return date.fromisoformat(calc_date)
    latest: date | None = None
    for table in _OBSERVATION_TABLES:
        row = conn.execute(f"SELECT max(as_of) FROM {table}").fetchone()
        if row and row[0] is not None:
            latest = row[0] if latest is None else max(latest, row[0])
    return latest


def _watermarks(conn: Any, as_of: date) -> dict[str, Any]:
    marks: dict[str, Any] = {}
    for table in _OBSERVATION_TABLES:
        marks[table] = conn.execute(
            f"SELECT count(*) FROM {table} WHERE as_of=%s", (as_of,)
        ).fetchone()[0]
    return marks


def _load_identity_observations(conn: Any, as_of: date) -> list[IdentityObservation]:
    rows = conn.execute(
        "SELECT observation_id, instrument_type, currency, alias_type, alias_value, "
        "valid_from, valid_to, observed_at, source_lineage, issuer_id, security_id, deterministic_key "
        "FROM mixed_quant_identity_observation WHERE as_of=%s "
        "ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    return [
        IdentityObservation(
            observation_id=r[0], instrument_type=r[1], currency=r[2], alias_type=r[3],
            alias_value=r[4], valid_from=r[5], valid_to=r[6], observed_at=r[7],
            source_lineage=r[8], issuer_id=r[9], security_id=r[10], deterministic_key=r[11],
        )
        for r in rows
    ]


def _alias_index(resolved: list[ResolvedInstrument]) -> dict[tuple[str, str], list[uuid.UUID]]:
    index: dict[tuple[str, str], set[uuid.UUID]] = defaultdict(set)
    for inst in resolved:
        for alias in inst.aliases:
            index[(alias.alias_type, alias.alias_value)].add(inst.instrument_id)
    return {key: sorted(ids, key=str) for key, ids in index.items()}


def _unique_instrument(index: dict[tuple[str, str], list[uuid.UUID]], alias_type: str, alias_value: str) -> uuid.UUID | None:
    ids = index.get((alias_type, alias_value))
    # A collision (alias resolving to >1 unresolved instrument) is intentionally
    # left unattached rather than guessed at.
    return ids[0] if ids and len(ids) == 1 else None


def _stage_identities(conn: Any, publication_id: uuid.UUID, as_of: date) -> list[ResolvedInstrument]:
    resolved = resolve_identities(_load_identity_observations(conn, as_of))
    if pub.get_checkpoint(conn, publication_id, "identities") is None:
        for inst in resolved:
            pub.write_instrument(conn, publication_id, inst)
        pub.record_checkpoint(conn, publication_id, "identities", {"instruments": len(resolved)})
    return resolved


def _stage_returns(conn: Any, publication_id: uuid.UUID, as_of: date, index: dict[tuple[str, str], list[uuid.UUID]]) -> None:
    if pub.get_checkpoint(conn, publication_id, "returns") is not None:
        return
    rows = conn.execute(
        "SELECT alias_type, alias_value, period_end, frequency, total_return, observed_at, source_lineage "
        "FROM mixed_quant_return_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    written = 0
    for alias_type, alias_value, period_end, frequency, total_return, observed_at, lineage in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None:
            continue
        pub.write_return(
            conn, publication_id, instrument_id, period_end=period_end, frequency=frequency,
            total_return=total_return, observed_at=observed_at, source_lineage=lineage,
        )
        written += 1
    pub.record_checkpoint(conn, publication_id, "returns", {"written": written, "seen": len(rows)})


def _holdings_reader(conn: Any, as_of: date) -> tuple[Callable[[str], tuple[date, list[dict[str, Any]]] | None], list[str]]:
    rows = conn.execute(
        "SELECT series_id, report_date, holdings FROM mixed_quant_holding_observation WHERE as_of=%s",
        (as_of,),
    ).fetchall()
    table: dict[str, tuple[date, list[dict[str, Any]]]] = {r[0]: (r[1], list(r[2])) for r in rows}

    def get_holdings(series_id: str) -> tuple[date, list[dict[str, Any]]] | None:
        return table.get(series_id)

    return get_holdings, [r[0] for r in rows]


def _stage_exposures(conn: Any, publication_id: uuid.UUID, as_of: date, resolved: list[ResolvedInstrument]) -> None:
    if pub.get_checkpoint(conn, publication_id, "exposures") is not None:
        return
    present = {inst.instrument_id for inst in resolved}
    get_holdings, series_ids = _holdings_reader(conn, as_of)
    written = 0
    for series_id in series_ids:
        # A fund's holdings are attributed to the instrument whose deterministic
        # identity is 'series:<series_id>'.
        instrument_id = mint_instrument_id(f"series:{series_id}")
        if instrument_id not in present:
            continue
        # No fund-of-funds edges are wired for the point-in-time snapshot yet;
        # expand_series still needs the alias buckets present to look children up.
        exposures, summary = expand_series(series_id, get_holdings, fund_map={"cusip": {}, "isin": {}})
        for row in _exposure_rows(exposures):
            factor = f"{row['dimension']}:{row['key']}"
            pub.write_exposure(
                conn, publication_id, instrument_id,
                factor=factor,
                value=float(row["direct_pct"]) + float(row["indirect_pct"]),
                method="nport_v2_lookthrough",
                coverage={
                    "measurement_type": "observed",
                    "direct_pct": row["direct_pct"],
                    "indirect_pct": row["indirect_pct"],
                    "label": row["label"],
                    "report_date": summary["report_date"].isoformat(),
                },
                source_lineage={"engine": "expand_series", "series_id": series_id, "as_of": as_of.isoformat()},
            )
            written += 1
    pub.record_checkpoint(conn, publication_id, "exposures", {"written": written})


def _link_targets(holding: dict[str, Any]) -> list[tuple[str, str]]:
    """Alias lookups a direct holding may resolve to (skips synthetic ids)."""
    out: list[tuple[str, str]] = []
    cusip = (holding.get("cusip") or "").strip()
    isin = (holding.get("isin") or "").strip()
    if cusip and not cusip.upper().startswith(SYNTHETIC_PREFIXES):
        out.append(("cusip", cusip))
    if isin:
        out.append(("isin", isin))
    return out


def _stage_linkage(
    conn: Any,
    publication_id: uuid.UUID,
    as_of: date,
    resolved: list[ResolvedInstrument],
    index: dict[tuple[str, str], list[uuid.UUID]],
) -> None:
    """Link each fund's direct holdings to the security identities they resolve to.

    Only unambiguous resolutions (a holding alias resolving to exactly one
    instrument other than the fund itself) are linked; collisions and unresolved
    holdings are intentionally left unlinked rather than guessed at.
    """
    if pub.get_checkpoint(conn, publication_id, "linkage") is not None:
        return
    present = {inst.instrument_id for inst in resolved}
    get_holdings, series_ids = _holdings_reader(conn, as_of)
    written = 0
    for series_id in series_ids:
        fund_id = mint_instrument_id(f"series:{series_id}")
        if fund_id not in present:
            continue
        record = get_holdings(series_id)
        if record is None:
            continue
        report_date, holdings = record
        # Aggregate direct weight per resolved security (a security may appear
        # under several holding lines).
        links: dict[uuid.UUID, dict[str, Any]] = {}
        for holding in holdings:
            if holding.get("pct_of_nav") is None:
                continue
            for alias_type, alias_value in _link_targets(holding):
                security_id = _unique_instrument(index, alias_type, alias_value)
                if security_id is None or security_id == fund_id or security_id not in present:
                    continue
                entry = links.setdefault(
                    security_id,
                    {"alias_type": alias_type, "alias_value": alias_value, "weight_pct": 0.0},
                )
                entry["weight_pct"] += float(holding["pct_of_nav"])
                break  # one resolution per holding line
        for security_id, entry in links.items():
            pub.write_holding_link(
                conn, publication_id, fund_id, security_id,
                alias_type=entry["alias_type"], alias_value=entry["alias_value"],
                weight_pct=entry["weight_pct"],
                coverage={"resolution": "direct_security", "report_date": report_date.isoformat()},
                source_lineage={"engine": "alias_resolution", "series_id": series_id, "as_of": as_of.isoformat()},
            )
            written += 1
    pub.record_checkpoint(conn, publication_id, "linkage", {"written": written})


def _stage_class_factors(
    conn: Any,
    publication_id: uuid.UUID,
    as_of: date,
    index: dict[tuple[str, str], list[uuid.UUID]],
) -> None:
    """Publish governed return-estimated class-factor exposures with evidence."""
    if pub.get_checkpoint(conn, publication_id, "class_factors") is not None:
        return
    rows = conn.execute(
        "SELECT alias_type, alias_value, factor, value, method, measurement_type, "
        "       quality_status, quality_flags, evidence, source_lineage "
        "FROM mixed_quant_class_factor_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    written = 0
    for (alias_type, alias_value, factor, value, method, measurement_type,
         quality_status, quality_flags, evidence, lineage) in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None:
            continue
        clean = validate_class_factor_row({
            "factor": factor, "value": value, "method": method,
            "measurement_type": measurement_type, "quality_status": quality_status,
            "quality_flags": quality_flags, "evidence": evidence, "source_lineage": lineage,
        })
        pub.write_exposure(
            conn, publication_id, instrument_id,
            factor=f"class_factor:{clean['factor']}",
            value=clean["value"],
            method=clean["method"],
            coverage={
                "measurement_type": clean["measurement_type"],
                "quality_status": clean["quality_status"],
                "quality_flags": clean["quality_flags"],
                "evidence": clean["evidence"],
            },
            source_lineage=clean["source_lineage"],
        )
        written += 1
    pub.record_checkpoint(conn, publication_id, "class_factors", {"written": written, "seen": len(rows)})


def _stage_bond_factors(
    conn: Any,
    publication_id: uuid.UUID,
    as_of: date,
    resolved: list[ResolvedInstrument],
    index: dict[tuple[str, str], list[uuid.UUID]],
) -> None:
    """Publish named bond factors ONLY where observed; declare the rest absent.

    Every bond instrument gets an explicit coverage map over the five named
    factors: 'observed' where a value was published, 'absent' otherwise. Absent
    factors carry no exposure row (never a fabricated value).
    """
    if pub.get_checkpoint(conn, publication_id, "bond_factors") is not None:
        return
    bonds = {inst.instrument_id for inst in resolved if inst.instrument_type == "bond"}
    rows = conn.execute(
        "SELECT alias_type, alias_value, factor, value, method, source_lineage "
        "FROM mixed_quant_bond_factor_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    observed: dict[uuid.UUID, set[str]] = {}
    written = 0
    for alias_type, alias_value, factor, value, method, lineage in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None or instrument_id not in bonds:
            continue
        clean = validate_bond_factor_row({
            "factor": factor, "value": value, "method": method, "source_lineage": lineage,
        })
        pub.write_exposure(
            conn, publication_id, instrument_id,
            factor=f"bond_factor:{clean['factor']}",
            value=clean["value"],
            method=clean["method"],
            coverage={"measurement_type": "observed", "named_factor": clean["factor"]},
            source_lineage=clean["source_lineage"],
        )
        observed.setdefault(instrument_id, set()).add(clean["factor"])
        written += 1
    # Declare coverage (observed/absent) for every bond instrument.
    for bond_id in bonds:
        seen = observed.get(bond_id, set())
        pub.merge_instrument_coverage(
            conn, publication_id, bond_id,
            {"bond_factor_coverage": {
                name: ("observed" if name in seen else "absent") for name in NAMED_BOND_FACTORS
            }},
        )
    pub.record_checkpoint(conn, publication_id, "bond_factors", {"written": written, "seen": len(rows)})


def _stage_income(conn: Any, publication_id: uuid.UUID, as_of: date, index: dict[tuple[str, str], list[uuid.UUID]]) -> None:
    if pub.get_checkpoint(conn, publication_id, "income") is not None:
        return
    rows = conn.execute(
        "SELECT alias_type, alias_value, event_date, cash_amount, currency, event_type, source_lineage "
        "FROM mixed_quant_income_observation WHERE as_of=%s ORDER BY observation_id",
        (as_of,),
    ).fetchall()
    written = 0
    for alias_type, alias_value, event_date, cash_amount, currency, event_type, lineage in rows:
        instrument_id = _unique_instrument(index, alias_type, alias_value)
        if instrument_id is None:
            continue
        pub.write_income(conn, publication_id, instrument_id, {
            "event_date": event_date, "cash_amount": cash_amount, "currency": currency,
            "event_type": event_type, "source_lineage": lineage,
        })
        written += 1
    pub.record_checkpoint(conn, publication_id, "income", {"written": written, "seen": len(rows)})


def run(dsn: str | None = None, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, Any]:
    """Build one inactive mixed_quant_v1 publication for ``calc_date`` (or the
    latest observed as_of). Idempotent and restartable; does not promote."""
    product = PRODUCT
    code_revision = os.getenv("CODE_REVISION") or _git_revision() or "dev"
    config_version = os.getenv("MIXED_QUANT_CONFIG_VERSION", "v1")

    with connect(resolve_dsn(dsn)) as conn:
        pub.install_schema(conn)
        conn.commit()
        with advisory_lock(conn, LOCK_MIXED_QUANT_PUBLICATION) as got:
            if not got:
                return {"status": "locked", "product": product}
            as_of = _resolve_as_of(conn, calc_date)
            if as_of is None:
                return {"status": "no_observations", "product": product}
            publication_id = pub.open_publication(
                conn, product=product, as_of=as_of, code_revision=code_revision,
                config_version=config_version, input_watermarks=_watermarks(conn, as_of),
            )
            conn.commit()

            resolved = _stage_identities(conn, publication_id, as_of)
            conn.commit()
            index = _alias_index(resolved)
            _stage_returns(conn, publication_id, as_of, index)
            conn.commit()
            _stage_exposures(conn, publication_id, as_of, resolved)
            conn.commit()
            _stage_linkage(conn, publication_id, as_of, resolved, index)
            conn.commit()
            _stage_class_factors(conn, publication_id, as_of, index)
            conn.commit()
            _stage_bond_factors(conn, publication_id, as_of, resolved, index)
            conn.commit()
            _stage_income(conn, publication_id, as_of, index)
            conn.commit()

            counts = pub.count_rows(conn, publication_id)
            pub.mark_ready(conn, publication_id, counts)
            conn.commit()

    return {
        "status": "ready", "product": product, "as_of": as_of.isoformat(),
        "publication_id": str(publication_id), "code_revision": code_revision,
        "config_version": config_version, **counts,
    }
