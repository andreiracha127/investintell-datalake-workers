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
           upper(h.cusip) AS cusip, h.issuer_name AS name,
           h.market_value AS value_usd, h.shares
    FROM sec_13f_holdings h
    LEFT JOIN LATERAL (
        SELECT m.firm_name FROM sec_managers m
        WHERE m.cik = h.cik AND m.firm_name IS NOT NULL
        ORDER BY m.aum_total DESC NULLS LAST LIMIT 1
    ) mgr ON true
    WHERE upper(h.cusip) = ANY(%(cusips)s)
),
latest AS (SELECT max(period) AS period FROM matched)
SELECT matched.* FROM matched JOIN latest ON latest.period = matched.period
ORDER BY value_usd DESC NULLS LAST LIMIT 500
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


def _refresh_latest_mv(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY fund_institutional_reveal_latest_mv"
            )


def _series_with_holdings(conn, limit):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT series_id FROM sec_nport_holdings"
            + (" LIMIT %s" if limit else ""),
            ((limit,) if limit else None),
        )
        return [r[0] for r in cur.fetchall()]


def _fund_top_cusips(conn, series_id):
    with conn.cursor() as cur:
        cur.execute(
            "WITH l AS (SELECT max(report_date) rd FROM sec_nport_holdings WHERE series_id=%s) "
            "SELECT upper(cusip) cusip, SUM(pct_of_nav)/100.0 w, report_date "
            "FROM sec_nport_holdings WHERE series_id=%s AND cusip IS NOT NULL "
            "AND report_date=(SELECT rd FROM l) GROUP BY upper(cusip), report_date "
            "ORDER BY w DESC NULLS LAST LIMIT 100",
            (series_id, series_id),
        )
        rows = cur.fetchall()
    cusips = [r[0] for r in rows]
    fund_pct = {r[0]: float(r[1]) for r in rows}
    as_of = rows[0][2] if rows else None
    return cusips, fund_pct, as_of


def run(dsn: str, *, limit: int | None = None) -> dict:
    processed = upserted = 0
    with connect(dsn) as conn:
        with advisory_lock(conn, LOCK_FUND_INSTITUTIONAL_REVEAL) as got:
            if not got:
                return {"processed": 0, "upserted": 0, "skipped": "lock_busy"}
            for series_id in _series_with_holdings(conn, limit):
                cusips, fund_pct, as_of = _fund_top_cusips(conn, series_id)
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
    result = {"processed": processed, "upserted": upserted}
    try:
        _refresh_latest_mv(dsn)
        result["mv_refreshed"] = True
    except Exception as exc:  # noqa: BLE001
        result["mv_refreshed"] = False
        result["mv_refresh_error"] = str(exc)
    return result
