"""RR1 V2 SQL installation from the frozen contract."""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "schemas" / "rr1_raw_v2.sql"
CONTRACT_PATH = ROOT / "contracts" / "sec-regulatory" / "v1" / "source-tables" / "rr1.json"


def install_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))
        document = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
        rows: list[tuple[object, ...]] = []
        targets: dict[str, str] = {}
        for variant in document["schema_variants"]:
            for table in variant["tables"]:
                target = table["columns"][0]["raw_target"]
                column_specs = {
                    column["name"]: {
                        "parsing_policy": column["parsing_policy"],
                        "required": column["required"],
                        "datatype": column["datatype"],
                    }
                    for column in table["columns"]
                }
                rows.append((variant["metadata_sha256"], table["source_file"], target, table["logical_parents"], table["candidate_primary_key"], [column["name"] for column in table["columns"]], json.dumps(column_specs, sort_keys=True)))
                targets[target] = table["source_file"]
        cur.execute("SELECT metadata_sha256, source_table FROM rr1_contract_tables")
        existing = {(str(metadata), source) for metadata, source in cur.fetchall()}
        missing = [row for row in rows if (row[0], row[1]) not in existing]
        if missing:
            cur.executemany("""INSERT INTO rr1_contract_tables(metadata_sha256,source_table,raw_target,logical_parents,candidate_key,headers,column_specs)
                VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)""", missing)
        cur.executemany("""UPDATE rr1_contract_tables SET column_specs=%s::jsonb
            WHERE metadata_sha256=%s AND source_table=%s AND column_specs='{}'::jsonb""", [(row[6], row[0], row[1]) for row in rows])
        for target, source in targets.items():
            cur.execute(sql.SQL("""CREATE OR REPLACE VIEW {} AS
                SELECT r.* FROM rr1_raw_v2_rows r JOIN sec_validated_raw_runs v ON v.run_id=r.ingestion_run_id
                WHERE r.source_table={} """).format(sql.Identifier(target), sql.Literal(source)))
        cur.execute("""CREATE OR REPLACE VIEW rr1_current_fact_candidates_v2 AS
            SELECT child.*
            FROM rr1_raw_v2_rows child
            JOIN sec_validated_raw_runs run ON run.run_id=child.ingestion_run_id
            WHERE child.source_table IN ('num.tsv','txt.tsv')
              AND child.parse_status='typed'
              AND child.typed_projection->>'ddate' IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM rr1_raw_v2_rows parent
                WHERE parent.ingestion_run_id=child.ingestion_run_id
                  AND parent.source_table='sub.tsv'
                  AND parent.adsh=child.adsh
                  AND parent.parse_status='typed'
                  AND parent.typed_projection->>'effdate' IS NOT NULL
              )""")
        cur.execute("SELECT 1 FROM pg_trigger WHERE tgrelid='rr1_contract_tables'::regclass AND tgname='rr1_contract_tables_immutable'")
        if cur.fetchone() is None:
            cur.execute("CREATE TRIGGER rr1_contract_tables_immutable BEFORE INSERT OR UPDATE OR DELETE ON rr1_contract_tables FOR EACH ROW EXECUTE FUNCTION rr1_contract_catalog_immutable()")
        cur.execute("REVOKE ALL ON rr1_contract_tables FROM PUBLIC")
