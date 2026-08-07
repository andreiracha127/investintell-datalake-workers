"""fund_institutional_reveal — cruzamento N-PORT×13F + rede, materializado em JSONB.

Por série: top-100 CUSIPs (latest N-PORT) × sec_13f_holdings (latest period por
esses CUSIPs), agregado por CIK (manager via sec_managers, maior AUM). Monta
top_holders (20), overlap (50) e holder_network (fundo + 12 securities + 8 inst.),
espelhando _institutional_payload/_build_holder_network do backend. Upsert em
fund_institutional_reveal_artifacts; REFRESH … CONCURRENTLY do _latest_mv fora do lock.
"""
from __future__ import annotations

import json

from src.db import LOCK_FUND_INSTITUTIONAL_REVEAL, advisory_lock, connect

_SCHEMA_VERSION = 1

_13F_SQL = """
WITH matched AS (
    SELECT h.cik,
           COALESCE(mgr.firm_name, 'CIK ' || h.cik) AS manager_name,
           h.report_date AS period, h.report_date,
           h.cusip, h.source_cusip, h.name, h.value_usd, h.shares
    FROM fund_reveal_13f_holdings_mv h
    LEFT JOIN LATERAL (
        SELECT m.firm_name FROM sec_managers m
        WHERE m.cik = h.cik AND m.firm_name IS NOT NULL
        ORDER BY m.aum_total DESC NULLS LAST LIMIT 1
    ) mgr ON true
    WHERE upper(h.cusip) = ANY(%(cusips)s)
),
latest AS (SELECT max(period) AS period FROM matched)
SELECT matched.* FROM matched JOIN latest ON latest.period = matched.period
ORDER BY value_usd DESC NULLS LAST, cik ASC, cusip ASC, source_cusip ASC LIMIT 500
"""


def build_payload(fund_node_id: str, fund_label: str, rows, fund_pct: dict) -> dict:
    holder_map: dict[str, dict] = {}
    overlap_map: dict[str, dict] = {}
    for r in rows:
        h = holder_map.setdefault(r["cik"], {
            "cik": r["cik"], "manager_name": r["manager_name"], "value_usd": 0.0,
            "shares": 0.0, "holding_count": 0,
            "period": str(r["period"]), "report_date": str(r["report_date"]),
        })
        h["value_usd"] += float(r["value_usd"] or 0.0)
        h["shares"] += float(r["shares"] or 0.0)
        h["holding_count"] += 1
        o = overlap_map.setdefault(r["cusip"], {
            "cusip": r["cusip"], "name": r["name"], "value_usd": 0.0,
            "institutions": set(), "managers": [],
        })
        o["value_usd"] += float(r["value_usd"] or 0.0)
        o["institutions"].add(r["cik"])
        if r["manager_name"] not in o["managers"]:
            o["managers"].append(r["manager_name"])

    holders = sorted(holder_map.values(), key=lambda d: d["value_usd"], reverse=True)
    overlap = sorted(
        ({
            "cusip": o["cusip"], "name": o["name"],
            "fund_pct_of_nav": fund_pct.get(o["cusip"]),
            "institutional_value_usd": o["value_usd"],
            "institution_count": len(o["institutions"]),
            "top_managers": o["managers"][:5],
        } for o in overlap_map.values()),
        key=lambda d: d["institutional_value_usd"], reverse=True,
    )
    top_holders = [
        {k: v for k, v in h.items()} for h in holders[:20]
    ]
    overlap_top = overlap[:50]

    nodes = [{"id": fund_node_id, "label": fund_label, "type": "fund"}]
    edges = []
    top12 = overlap_top[:12]
    top_cusips = {o["cusip"] for o in top12}
    for o in top12:
        nodes.append({"id": f"security:{o['cusip']}", "label": o["name"] or o["cusip"],
                      "type": "security", "value": o["institutional_value_usd"]})
        edges.append({"source": fund_node_id, "target": f"security:{o['cusip']}",
                      "weight": o["fund_pct_of_nav"], "label": "fund holding"})
    top8 = top_holders[:8]
    top8_ciks = {h["cik"] for h in top8}
    for h in top8:
        nodes.append({"id": f"institution:{h['cik']}", "label": h["manager_name"],
                      "type": "institution", "value": h["value_usd"]})
    for r in rows:
        if r["cik"] in top8_ciks and r["cusip"] in top_cusips:
            edges.append({"source": f"institution:{r['cik']}", "target": f"security:{r['cusip']}",
                          "weight": float(r["value_usd"] or 0.0), "label": "13F value"})

    period = max((str(r["period"]) for r in rows if r["period"] is not None), default=None)
    return {
        "schema_version": _SCHEMA_VERSION,
        "top_holders": top_holders,
        "overlap": overlap_top,
        "holder_network": {"nodes": nodes, "edges": edges},
        "period": period,
    }


_UPSERT = """
INSERT INTO fund_institutional_reveal_artifacts
    (series_id, as_of, schema_version, payload, organization_id)
VALUES (%(series_id)s, %(as_of)s, %(ver)s, %(payload)s, NULL)
ON CONFLICT (series_id, as_of, organization_id) DO UPDATE SET
    schema_version = EXCLUDED.schema_version, payload = EXCLUDED.payload, computed_at = now()
"""

_REVOKE_SERIES_ARTIFACTS = """
DELETE FROM fund_institutional_reveal_artifacts
WHERE series_id = %s AND organization_id IS NULL
"""


def _refresh_latest_mv(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_institutional_reveal_latest_mv"
            )


def _series_with_holdings(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT series_id FROM fund_top_holdings_mv ORDER BY series_id"
            + (" LIMIT %s" if limit else ""),
            ((limit,) if limit else None),
        )
        return [r[0] for r in cur.fetchall()]


def _reveal_holdings_for_series(conn, series_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT series_id, report_date, rank, cusip, weight, source_row_count, "
            "nonnull_weight_count, null_weight_count, has_unknown_weight "
            "FROM fund_reveal_holdings_mv WHERE series_id=%s ORDER BY rank LIMIT 100",
            (series_id,),
        )
        cols = [c.name for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _prepare_reveal_holdings(series_id, rows):
    ordered_rows = sorted(rows, key=lambda row: row["rank"])
    if not ordered_rows:
        return None, {
            "series_id": series_id,
            "report_date": None,
            "reason": "no_joinable_cusips",
        }
    if any(
        row["weight"] is None
        or bool(row.get("has_unknown_weight"))
        or (row["null_weight_count"] is not None and row["null_weight_count"] > 0)
        for row in ordered_rows
    ):
        report_date = ordered_rows[0]["report_date"] if ordered_rows else None
        return None, {
            "series_id": series_id,
            "report_date": str(report_date) if report_date is not None else None,
            "reason": "unknown_weight",
        }

    cusips = []
    fund_pct = {}
    for row in ordered_rows:
        cusip = row["cusip"]
        if cusip is None:
            continue
        cusip = str(cusip).upper()
        cusips.append(cusip)
        fund_pct[cusip] = float(row["weight"])
    as_of = ordered_rows[0]["report_date"] if ordered_rows else None
    return (cusips, fund_pct, as_of), None


def _revoke_series_artifacts(conn, series_id) -> int:
    with conn.cursor() as cur:
        cur.execute(_REVOKE_SERIES_ARTIFACTS, (series_id,))
        return max(cur.rowcount, 0)


def run(dsn: str, *, limit: int | None = None) -> dict:
    processed = upserted = quarantined = revoked_artifacts = 0
    quarantine_samples = []
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FUND_INSTITUTIONAL_REVEAL) as got:
            if not got:
                return {
                    "processed": 0,
                    "upserted": 0,
                    "quarantined": 0,
                    "revoked_artifacts": 0,
                    "quarantine_samples": [],
                    "skipped": "lock_busy",
                }
            for series_id in _series_with_holdings(conn, limit):
                holdings, quarantine = _prepare_reveal_holdings(
                    series_id, _reveal_holdings_for_series(conn, series_id)
                )
                if quarantine is not None:
                    quarantined += 1
                    revoked_artifacts += _revoke_series_artifacts(conn, series_id)
                    if len(quarantine_samples) < 10:
                        quarantine_samples.append(quarantine)
                    continue
                cusips, fund_pct, as_of = holdings
                if not cusips or as_of is None:
                    continue
                processed += 1
                with conn.cursor() as cur:
                    cur.execute(_13F_SQL, {"cusips": cusips})
                    cols = [c.name for c in cur.description]
                    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
                if not rows:
                    continue
                payload = build_payload(f"series:{series_id}", series_id, rows, fund_pct)
                with conn.cursor() as cur:
                    cur.execute(_UPSERT, {
                        "series_id": series_id, "as_of": as_of,
                        "ver": _SCHEMA_VERSION, "payload": json.dumps(payload),
                    })
                upserted += 1
            conn.commit()
    result = {
        "processed": processed,
        "upserted": upserted,
        "quarantined": quarantined,
        "revoked_artifacts": revoked_artifacts,
        "quarantine_samples": quarantine_samples,
    }
    # Revocations and upserts are not safely visible until this serving snapshot
    # advances. Propagate failures so Railway reports the run as failed instead
    # of serving a stale pre-quarantine artifact behind a green deployment.
    _refresh_latest_mv(dsn)
    result["mv_refreshed"] = True
    return result
