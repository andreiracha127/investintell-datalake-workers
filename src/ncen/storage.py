"""N-CEN V2 SQL installation."""
from pathlib import Path
import json
import psycopg
from psycopg import sql
ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = ROOT / "schemas" / "ncen_raw_v2.sql"
def install_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))
        document = json.loads((ROOT / "contracts" / "sec-regulatory" / "v1" / "source-tables" / "ncen.json").read_text(encoding="utf-8"))
        catalog: list[tuple[object, ...]] = []
        views: dict[str, str] = {}
        for variant in document["schema_variants"]:
            metadata_sha = variant["metadata_sha256"]
            for table in variant["tables"]:
                target = table.get("raw_target") or table["columns"][0]["raw_target"]
                catalog.append((metadata_sha, table["source_file"], target, table["logical_parents"], table["candidate_primary_key"], [column["name"] for column in table["columns"]], json.dumps(table["columns"], sort_keys=True)))
                views[target] = table["source_file"]
        # Do not attempt a conflicting INSERT: PostgreSQL runs BEFORE INSERT
        # guards even for ON CONFLICT DO NOTHING.  Existing catalog records
        # stay untouched; a reinstall only fills an empty fresh schema.
        cur.execute("SELECT metadata_sha256, source_table FROM ncen_contract_tables")
        existing = {(str(metadata), source) for metadata, source in cur.fetchall()}
        missing = [row for row in catalog if (row[0], row[1]) not in existing]
        if missing:
            cur.executemany("""INSERT INTO ncen_contract_tables(metadata_sha256,source_table,raw_target,logical_parents,candidate_key,headers,column_contract)
                               VALUES(%s,%s,%s,%s,%s,%s,%s::jsonb)""", missing)
        for target, source_file in views.items():
            cur.execute(sql.SQL("""CREATE OR REPLACE VIEW {} AS
                SELECT r.* FROM ncen_raw_v2_rows r JOIN sec_validated_raw_runs v ON v.run_id=r.ingestion_run_id
                WHERE r.source_table={}""").format(sql.Identifier(target), sql.Literal(source_file)))
        cur.execute("""SELECT 1 FROM pg_trigger WHERE tgrelid='ncen_contract_tables'::regclass AND tgname='ncen_contract_tables_immutable'""")
        if cur.fetchone() is None:
            cur.execute("CREATE TRIGGER ncen_contract_tables_immutable BEFORE INSERT OR UPDATE OR DELETE ON ncen_contract_tables FOR EACH ROW EXECUTE FUNCTION ncen_contract_catalog_immutable()")
        cur.execute("REVOKE ALL ON ncen_contract_tables FROM PUBLIC")
