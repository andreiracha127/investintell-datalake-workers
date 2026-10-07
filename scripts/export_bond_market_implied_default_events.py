"""Read-only export of the market-implied default episodes the coupon-PIT republication needs.

The historical rebuild (``scripts/build_bond_panel_coupon_pit_returns.py --default-events``)
applies carry 0 inside each CONFIRMED market-implied default window of one pinned
``bond_market_implied_rating_v1`` publication. This script exports that input once:

- **Seed, not a producer input.** The file seeds the 2002-2026 republication only. The
  live panel worker keeps reading the pointed implied-rating publication point in time
  (``_default_flat_windows``), and the implied-rating producer reads the panel snapshot
  (prices/spreads), never carry or returns, so nothing consumes this file afterwards and
  no default list is frozen.
- **Pinned publication.** ``--publication-id`` / ``--policy-digest`` / ``--rows-digest``
  are the expected pins; the script refuses when the publication's header disagrees,
  when the app pointer does not serve that publication at the start of the export, or
  when the pointer moved by the time the export finished.
- **One snapshot.** All reads run in ONE ``REPEATABLE READ READ ONLY`` transaction; the
  pointer is read as its first statement. A second pointer read in a fresh transaction
  after commit proves the pointer did not move while exporting.
- **Confirmed defaults only.** Every ``d_confirmed`` row of the publication is exported;
  their count must equal the header's ``d_confirmed_count``. ``d_candidate`` rows that
  never confirm open no window and are not exported. The file always carries, for each
  cured episode, the single row that ends its window (the first witnessed rated row after
  the episode's last D month), computed by the builder's own
  ``panel_resolvers.default_flat_windows``: without it no window could close, and the
  builder refuses a publishable build from a file that lacks them.

Writes ``<out>/bond_market_implied_default_events.parquet`` (the builder's
``DEFAULT_EVENT_COLUMNS`` + identity + ``export_role``; the manifest is also embedded as
parquet key-value metadata) and ``<out>/bond_market_implied_default_events.manifest.json``
(pins, the exact SQL, counts, both pointer reads, the parquet's sha256).

Run outside 06:00-08:30 UTC with the owner's read-only railway recipe:

    railway run --service risk-metrics -- uv run --no-project \\
        --with "psycopg[binary]" --with pandas --with pyarrow --with numpy \\
        python scripts/export_bond_market_implied_default_events.py --out <dir> \\
        --publication-id <uuid> --policy-digest <sha256> --rows-digest <sha256>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402

from src.bonds.panel_resolvers import _IMPLIED_ROW_COLUMNS, default_flat_windows  # noqa: E402

PROXY_NETLOC = "centerbeam.proxy.rlwy.net:36616"
PRODUCT = "bond_market_implied_rating_v1"
OUTPUT_NAME = "bond_market_implied_default_events.parquet"
MANIFEST_NAME = "bond_market_implied_default_events.manifest.json"
EXPORT_SCHEMA = "bond_market_implied_default_events_export_v1"
ROW_LIMIT = 2_000_000
SESSION_OPTIONS = "-c default_transaction_read_only=on -c statement_timeout=30000"
EXPORT_COLUMNS = (*_IMPLIED_ROW_COLUMNS, "policy_version", "policy_digest", "publication_id", "export_role")

POINTER_SQL = (
    "SELECT publication_id::text, changed_at FROM bond_market_implied_rating_app_pointer "
    "WHERE product = %(product)s"
)
HEADER_SQL = (
    "SELECT publication_id::text, publication_status, policy_version, policy_digest, code_revision, "
    "panel_publication_id::text, panel_last_closed_month, first_month, last_month, row_count, "
    "rows_digest, d_confirmed_count, d_candidate_count "
    "FROM bond_market_implied_rating_publications WHERE publication_id = %(publication_id)s"
)
# Every row of every CUSIP that has a confirmed D row in the publication, so that both
# the episodes and their cures are computed from the publication itself. Uses the
# (cusip_id, month) index; LIMIT-bounded and refused when reached.
ROWS_SQL = (
    "SELECT r.cusip_id, r.month, r.implied_bucket, r.witnessed, r.spell_id, r.d_confirmed, "
    "r.d_event_month, r.policy_version, r.policy_digest "
    "FROM bond_market_implied_rating_v1 r "
    "WHERE r.publication_id = %(publication_id)s "
    "AND r.cusip_id IN (SELECT DISTINCT d.cusip_id FROM bond_market_implied_rating_v1 d "
    "WHERE d.publication_id = %(publication_id)s AND d.d_confirmed) "
    "ORDER BY r.cusip_id, r.month LIMIT %(limit)s"
)


# The export must run as a role that cannot write the relations it reads.
ROLE_SQL = (
    "SELECT current_user AS role, "
    "has_table_privilege('bond_market_implied_rating_v1', 'INSERT, UPDATE, DELETE') "
    "OR has_table_privilege('bond_market_implied_rating_v1_builds', 'INSERT, UPDATE, DELETE') "
    "OR has_table_privilege('sec_derived_current_pointers', 'INSERT, UPDATE, DELETE') AS can_write"
)


class ExportError(RuntimeError):
    """A pin, count or pointer check refused the export."""


def _dsn() -> str:
    parsed = urlparse(os.environ["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://"))
    if parsed.hostname and parsed.hostname.endswith(".railway.internal"):
        parsed = parsed._replace(netloc=f"{parsed.username}:{parsed.password}@{PROXY_NETLOC}")
    return urlunparse(parsed._replace(scheme="postgresql"))


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def check_header(header: dict[str, Any], *, publication_id: str, policy_digest: str, rows_digest: str) -> None:
    """Refuse unless the publication header carries exactly the expected pins."""
    if header["publication_id"] != publication_id:
        raise ExportError(f"publication_mismatch:{header['publication_id']}")
    if header["publication_status"] != "validated":
        raise ExportError(f"publication_not_validated:{header['publication_status']}")
    if header["policy_digest"] != policy_digest:
        raise ExportError(f"policy_digest_mismatch:{header['policy_digest']}")
    if header["rows_digest"] != rows_digest:
        raise ExportError(f"rows_digest_mismatch:{header['rows_digest']}")


def select_export_rows(rows: pd.DataFrame, header: dict[str, Any], *, cure_witnesses: bool) -> tuple[pd.DataFrame, dict[str, Any]]:
    """The confirmed-D rows (and optionally each cured episode's closing row), with counts.

    ``rows`` holds every row of the CUSIPs that have a D row. The D-row count must equal
    the header's ``d_confirmed_count`` -- that proves no defaulted CUSIP was missed. The
    windows (and so the cure rows) come from the builder's own ``default_flat_windows``.
    """
    in_default = rows["implied_bucket"].eq("D")
    if not in_default.equals(rows["d_confirmed"].astype(bool)):
        raise ExportError("implied_bucket_d_disagrees_with_d_confirmed")
    d_rows = rows[in_default]
    if len(d_rows) != int(header["d_confirmed_count"]):
        raise ExportError(f"d_confirmed_count_mismatch:{len(d_rows)}!={header['d_confirmed_count']}")
    windows = default_flat_windows(rows.loc[:, list(_IMPLIED_ROW_COLUMNS)])
    parts = [d_rows.assign(export_role="d_confirmed")]
    cured = windows[windows["cure_month"].notna()]
    if cure_witnesses and len(cured):
        keys = pd.DataFrame({
            "cusip_id": cured["cusip_id"].astype(str).to_numpy(),
            "month": pd.to_datetime(cured["cure_month"]).dt.date.to_numpy(),
        }).drop_duplicates()
        cure_rows = rows.merge(keys, on=["cusip_id", "month"], how="inner")
        if len(cure_rows) != len(keys):
            raise ExportError(f"cure_rows_mismatch:{len(cure_rows)}!={len(keys)}")
        parts.append(cure_rows.assign(export_role="cure_witness"))
    out = pd.concat(parts, ignore_index=True).sort_values(["cusip_id", "month"], kind="mergesort").reset_index(drop=True)
    out["publication_id"] = header["publication_id"]
    counts = {
        "rows_of_defaulted_cusips_read": int(len(rows)),
        "defaulted_cusips": int(d_rows["cusip_id"].nunique()),
        "d_confirmed_rows": int(len(d_rows)),
        "header_d_confirmed_count": int(header["d_confirmed_count"]),
        "header_d_candidate_count": int(header["d_candidate_count"]),
        "episodes": int(len(windows)),
        "episodes_cured": int(len(cured)),
        "episodes_open": int(windows["cure_month"].isna().sum()),
        "cure_witness_rows": int((out["export_role"] == "cure_witness").sum()),
        "exported_rows": int(len(out)),
    }
    return out.loc[:, list(EXPORT_COLUMNS)], counts


def write_artifact(out_dir: Path, frame: pd.DataFrame, manifest: dict[str, Any]) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / OUTPUT_NAME
    table = pa.Table.from_pandas(frame, preserve_index=False)
    table = table.replace_schema_metadata({
        **(table.schema.metadata or {}),
        b"investintell.export_manifest": json.dumps(manifest, sort_keys=True).encode("utf-8"),
    })
    pq.write_table(table, path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    full = {**manifest, "artifact": {"path": OUTPUT_NAME, "sha256": sha, "rows": int(len(frame))}}
    (out_dir / MANIFEST_NAME).write_text(json.dumps(full, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (out_dir / f"{OUTPUT_NAME}.sha256").write_text(f"{sha}  {OUTPUT_NAME}\n", encoding="utf-8")
    return full


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--publication-id", required=True)
    parser.add_argument("--policy-digest", required=True)
    parser.add_argument("--rows-digest", required=True)
    args = parser.parse_args(argv)
    import psycopg  # local import: the module stays importable without the driver
    from psycopg.rows import dict_row

    started = datetime.now(UTC).replace(microsecond=0)
    params = {"product": PRODUCT, "publication_id": args.publication_id, "limit": ROW_LIMIT}
    with psycopg.connect(_dsn(), options=SESSION_OPTIONS, row_factory=dict_row) as conn:
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        conn.read_only = True
        with conn.transaction():
            # First statement: fixes the snapshot every read below shares.
            start_pointer = conn.execute(POINTER_SQL, params).fetchone()
            if start_pointer is None or start_pointer["publication_id"] != args.publication_id:
                raise ExportError(f"pointer_not_on_pinned_publication:{start_pointer}")
            isolation = conn.execute("SELECT current_setting('transaction_isolation') AS v").fetchone()["v"]
            read_only = conn.execute("SELECT current_setting('transaction_read_only') AS v").fetchone()["v"]
            if isolation != "repeatable read" or read_only != "on":
                raise ExportError(f"session_not_repeatable_read_read_only:{isolation}/{read_only}")
            role = conn.execute(ROLE_SQL).fetchone()
            if role["can_write"]:
                raise ExportError(f"role_can_write_source_relations:{role['role']}")
            header = conn.execute(HEADER_SQL, params).fetchone()
            if header is None:
                raise ExportError("publication_header_missing")
            check_header(header, publication_id=args.publication_id,
                         policy_digest=args.policy_digest, rows_digest=args.rows_digest)
            rows = pd.DataFrame(conn.execute(ROWS_SQL, params).fetchall())
            if len(rows) >= ROW_LIMIT:
                raise ExportError("row_limit_reached")
        with conn.transaction():
            end_pointer = conn.execute(POINTER_SQL, params).fetchone()
    if end_pointer != start_pointer:
        raise ExportError(f"pointer_moved_during_export:{start_pointer}->{end_pointer}")

    frame, counts = select_export_rows(rows, header, cure_witnesses=True)
    manifest = {
        "schema": EXPORT_SCHEMA,
        "purpose": "one-time seed of the 2002-2026 coupon-PIT republication; not a producer input",
        "exported_at_utc": started.isoformat().replace("+00:00", "Z"),
        "publication_id": header["publication_id"],
        "rows_digest": header["rows_digest"],
        "policy_version": header["policy_version"],
        "policy_digest": header["policy_digest"],
        "code_revision": header["code_revision"],
        "panel_publication_id": header["panel_publication_id"],
        "as_of": _iso(header["panel_last_closed_month"]),
        "first_month": _iso(header["first_month"]),
        "last_month": _iso(header["last_month"]),
        "header_row_count": int(header["row_count"]),
        "cure_witnesses": True,
        "isolation": "repeatable read, read only (one transaction; pointer read first)",
        "role": role["role"],
        "role_can_write_source_relations": bool(role["can_write"]),
        "pointer_start": {k: _iso(v) for k, v in start_pointer.items()},
        "pointer_end": {k: _iso(v) for k, v in end_pointer.items()},
        "sql": {"pointer": POINTER_SQL, "role": ROLE_SQL, "header": HEADER_SQL, "rows": ROWS_SQL},
        "counts": counts,
    }
    full = write_artifact(args.out, frame, manifest)
    print(json.dumps({k: full[k] for k in ("role", "publication_id", "rows_digest", "policy_digest", "code_revision",
                                          "panel_publication_id", "as_of", "pointer_start", "pointer_end",
                                          "counts", "artifact")}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except ExportError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        raise SystemExit(2)
