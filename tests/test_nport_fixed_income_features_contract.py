from __future__ import annotations

import hashlib
import json
from pathlib import Path

import psycopg

from test_nport_fixed_income_features import DSN, _seed_fixture


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "contracts" / "nport-fixed-income-features" / "v2" / "contract.json"


def _canonical_digest(document: dict[str, object]) -> str:
    unsigned = {key: value for key, value in document.items() if key != "digest"}
    payload = json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return f"sha256:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"


def test_nport_fixed_income_v2_contract_is_deterministic_and_governed() -> None:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    assert contract["id"] == "nport-fixed-income-features/v2"
    assert contract["version"] == "2.0.0"
    assert contract["digest"] == _canonical_digest(contract)
    assert contract["build_pin"] == {
        "source_commit": "external_runtime_pin",
        "reason": "A source commit embedded in its own contract would be self-referential.",
    }

    relations = {relation["name"]: relation for relation in contract["relations"]}
    assert {"nport_fixed_income_features", "nport_composition_features", "sec_nport_holdings_v2"} <= relations.keys()
    assert relations["sec_nport_holdings_v2"]["grain"] == ["publication_id", "accession_number", "holding_id"]
    assert relations["nport_fixed_income_features"]["semantics"] == {"historical": "immutable_publication", "current": "sec_current_nport_fixed_income_features"}
    v2_relations = {
        "nport_fixed_income_key_rate_sensitivities_v2",
        "nport_fixed_income_credit_spread_sensitivities_v2",
        "nport_fixed_income_balance_sheet_primitives_v2",
        "nport_fixed_income_debt_flag_features_v2",
        "nport_fixed_income_repo_lending_primitives_v2",
        "nport_fixed_income_repo_lending_reported_flags_v2",
    }
    for relation_name in v2_relations:
        relation = relations[relation_name]
        assert relation["identity"]["source_holdings_publication"] == "source_holdings_publication_id"
        assert relation["identity"]["source_run"] == "source_run_id"
        names = {column["name"] for column in relation["columns"]}
        assert {"source_holdings_publication_id", "source_run_id", "methodology_version"} <= names
        assert relation["semantics"]["current"] in contract["compatibility"]["existing_current_relations"]

    supported = {capability["id"]: capability for capability in contract["capabilities"]}
    assert supported["weighted_maturity_statistics"]["state"] == "supported"
    assert supported["holding_country_currency_restriction_fair_value"]["state"] == "supported"
    for capability_id in ("interest_rate_dv01_dv100", "spread_sensitivity", "borrowings_and_commitments", "repo_lending_collateral_counterparty"):
        assert supported[capability_id]["state"] == "supported"
        assert supported[capability_id]["relation"].endswith("_v2")

    unavailable = {item["id"] for item in contract["quality"]["unavailable_metrics"]}
    assert {"ytm", "ytw", "current_yield", "oas", "z_spread", "spread_duration", "wal", "prepayment", "rating", "seniority", "secured", "call", "liquidity"} <= unavailable
    assert "sector" not in json.dumps(contract).lower()


def test_nport_fixed_income_v2_contract_relations_match_disposable_postgres_shape() -> None:
    """Detect unreviewed v2 table/current-view shape drift using real catalog data."""
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    declared = {item["name"]: item for item in contract["relations"]}
    parity = contract["schema_parity"]
    relation_names = parity["relation_names"]

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        schema, *_ = _seed_fixture(cur)
        try:
            cur.execute((ROOT / "schemas" / "nport_composition_features.sql").read_text(encoding="utf-8"))
            for table_name in relation_names:
                cur.execute(
                    """SELECT column_name,data_type,is_nullable FROM information_schema.columns
                       WHERE table_schema=current_schema() AND table_name=%s ORDER BY ordinal_position""",
                    (table_name,),
                )
                actual = {name: (sql_type, nullable == "YES") for name, sql_type, nullable in cur.fetchall()}
                assert actual, table_name
                columns = declared[table_name]["columns"]
                assert len({column["name"] for column in columns}) == len(columns), table_name
                documented = {column["name"]: (column["type"], column["nullable"]) for column in columns}
                allowlist = set(parity["internal_allowlist"][table_name])
                assert not allowlist.intersection(declared[table_name]["keys"]), table_name
                expected = {name: shape for name, shape in actual.items() if name not in allowlist}
                assert set(declared[table_name]["keys"]) | set(documented) == set(expected), table_name
                assert documented == expected, table_name
                cur.execute("""SELECT kcu.column_name FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name
                     AND tc.table_schema=kcu.table_schema
                    WHERE tc.table_schema=current_schema() AND tc.table_name=%s AND tc.constraint_type='PRIMARY KEY'
                    ORDER BY kcu.ordinal_position""", (table_name,))
                assert tuple(row[0] for row in cur.fetchall()) == tuple(declared[table_name]["keys"])
                view_name = declared[table_name]["semantics"]["current"]
                cur.execute("SELECT 1 FROM information_schema.views WHERE table_schema=current_schema() AND table_name=%s", (view_name,))
                assert cur.fetchone() == (1,)
        finally:
            cur.execute(f'DROP SCHEMA "{schema}" CASCADE')
