"""Read-only export of the contractual coupons the coupon-PIT returns artifact needs.

Writes ``bond_reference_terms_coupons.csv`` (``cusip9, coupon_rate, coupon_type,
maturity_date, batch_label, loaded_at, max_loaded_at, exported_at_utc``) plus a
``.sha256`` sidecar from one bounded, read-only session; the builder
(``scripts/build_bond_panel_coupon_pit_returns.py``) pins the CSV's digest into
the artifact manifest.  Nothing is written to the database: the session is
``default_transaction_read_only=on`` with a 30 s statement timeout and the
single query is LIMIT-bounded.

Run from an environment that carries ``DATABASE_URL`` (the owner's railway
recipe), outside the 06:00-08:30 UTC window:

    railway run --service risk-metrics -- uv run --no-project --with "psycopg[binary]" \\
        python scripts/export_bond_reference_terms_coupons.py --out <dir>

The row count printed must equal ``count(*) FROM bond_reference_terms WHERE
coupon_rate IS NOT NULL`` (the script refuses if the LIMIT was reached).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse, urlunparse

PROXY_NETLOC = "centerbeam.proxy.rlwy.net:36616"
ROW_LIMIT = 500_000
OUTPUT_NAME = "bond_reference_terms_coupons.csv"
QUERY = """
SELECT cusip9, coupon_rate, coupon_type, maturity_date, batch_label, loaded_at
FROM bond_reference_terms
WHERE coupon_rate IS NOT NULL
ORDER BY cusip9
LIMIT %s
"""


def _dsn() -> str:
    parsed = urlparse(os.environ["DATABASE_URL"])
    return urlunparse(parsed._replace(scheme="postgresql", netloc=f"{parsed.username}:{parsed.password}@{PROXY_NETLOC}"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, required=True, help="directory that receives the CSV and its .sha256 sidecar")
    args = parser.parse_args(argv)
    import psycopg  # local import: the module stays importable without the driver

    exported_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with psycopg.connect(_dsn(), options="-c default_transaction_read_only=on -c statement_timeout=30000") as conn:
        cur = conn.cursor()
        cur.execute("SET max_parallel_workers_per_gather = 0")
        cur.execute("SELECT count(*) FILTER (WHERE state = 'active' AND xact_start < now() - interval '120 seconds') FROM pg_stat_activity WHERE datname = current_database()")
        if int(cur.fetchone()[0]) > 0:
            print("BACK OFF: long ACTIVE transactions", file=sys.stderr)
            return 2
        cur.execute("SELECT count(*), max(loaded_at) FROM bond_reference_terms WHERE coupon_rate IS NOT NULL")
        expected_rows, max_loaded_at = cur.fetchone()
        cur.execute(QUERY, (ROW_LIMIT,))
        rows = cur.fetchall()
    if len(rows) >= ROW_LIMIT or len(rows) != int(expected_rows):
        print(f"export_incomplete: fetched {len(rows)} of {expected_rows}", file=sys.stderr)
        return 2
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / OUTPUT_NAME
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["cusip9", "coupon_rate", "coupon_type", "maturity_date", "batch_label", "loaded_at", "max_loaded_at", "exported_at_utc"])
        for cusip9, coupon_rate, coupon_type, maturity_date, batch_label, loaded_at in rows:
            writer.writerow([cusip9, coupon_rate, coupon_type or "", maturity_date.isoformat() if maturity_date else "", batch_label, loaded_at.isoformat(), max_loaded_at.isoformat(), exported_at])
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    (args.out / f"{OUTPUT_NAME}.sha256").write_text(f"{digest}  {OUTPUT_NAME}\n", encoding="utf-8")
    print(f"rows={len(rows)} max_loaded_at={max_loaded_at.isoformat()} sha256={digest} path={path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
