"""Read-only verification of ONE ``bond_market_implied_rating_v1`` publication by UUID.

Recomputes ``rows_digest`` from the stored rows with the SAME canonical digest
the producer used (``src.bonds.implied_rating.rows_digest``), and compares it
-- plus row count, D counts, policy digest and the current pointer -- with
the publication's build pin and the operator's expectations. The session is
opened read-only with bounded timeouts (``implied_rating_replay.read_only_connect``);
nothing is written, no DDL is replayed.

    python -m scripts.verify_bond_market_implied_rating_publication \\
        --publication-id <uuid> [--expect-rows-digest <sha256>] \\
        [--expect-policy-digest <sha256>] [--expect-current]

Exit 0 only when every check passes; 1 on any mismatch; 2 on an operational
error. Prints one sanitized JSON document (no DSN).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""} and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bonds import implied_rating as policy  # noqa: E402
from src.bonds.implied_rating_materializer import PRODUCT  # noqa: E402
from src.bonds.implied_rating_replay import read_only_connect  # noqa: E402

BUILD_SQL = (
    "SELECT b.panel_publication_id::text, b.policy_digest, b.code_revision, "
    "b.panel_last_closed_month, b.first_month, b.last_month, b.input_fingerprint, "
    "b.l_anchor, b.row_count, b.rows_digest, b.d_confirmed_count, b.d_candidate_count, "
    "s.lifecycle_state "
    f"FROM {PRODUCT}_builds b JOIN sec_derived_publications s USING (publication_id) "
    "WHERE b.publication_id = %s"
)
ROWS_SQL = (
    f"SELECT {', '.join(policy.PUBLICATION_COLUMNS)} FROM {PRODUCT} "
    "WHERE publication_id = %s ORDER BY month, cusip_id"
)
POINTER_SQL = "SELECT publication_id::text FROM sec_derived_current_pointers WHERE product = %s"


def verify(
    conn: psycopg.Connection, *, publication_id: str, expect_rows_digest: str | None = None,
    expect_policy_digest: str | None = None, expect_current: bool = False,
) -> dict[str, Any]:
    build = conn.execute(BUILD_SQL, (publication_id,)).fetchone()
    if build is None:
        return {"publication_id": publication_id, "ok": False, "reasons": ["build_absent"]}
    with conn.cursor() as cur:
        cur.execute(ROWS_SQL, (publication_id,))
        columns = [column.name for column in cur.description]
        rows = pd.DataFrame(cur.fetchall(), columns=columns)
    pointer_row = conn.execute(POINTER_SQL, (PRODUCT,)).fetchone()
    conn.commit()
    pointer = None if pointer_row is None else str(pointer_row[0])
    recomputed = policy.rows_digest(rows) if not rows.empty else None
    d_confirmed, d_candidate = policy.default_counts(rows)
    stored = {
        "panel_publication_id": build[0], "policy_digest": str(build[1]).strip(),
        "code_revision": build[2], "panel_last_closed_month": build[3].isoformat(),
        "first_month": build[4].isoformat(), "last_month": build[5].isoformat(),
        "input_fingerprint": str(build[6]).strip(), "l_anchor": {
            "repr": repr(float(build[7])), "hex": float(build[7]).hex(),
        },
        "row_count": int(build[8]), "rows_digest": str(build[9]).strip(),
        "d_confirmed_count": int(build[10]), "d_candidate_count": int(build[11]),
        "lifecycle_state": build[12],
    }
    reasons: list[str] = []
    if recomputed != stored["rows_digest"]:
        reasons.append("rows_digest_not_reproduced")
    if len(rows) != stored["row_count"]:
        reasons.append("row_count_mismatch")
    if (d_confirmed, d_candidate) != (stored["d_confirmed_count"], stored["d_candidate_count"]):
        reasons.append("default_counts_mismatch")
    if stored["lifecycle_state"] != "validated":
        reasons.append("not_validated")
    if not rows.empty:
        if rows["policy_digest"].astype(str).str.strip().ne(stored["policy_digest"]).any():
            reasons.append("row_policy_digest_mismatch")
        if pd.to_datetime(rows["month"]).max().date().isoformat() != stored["last_month"]:
            reasons.append("last_month_mismatch")
    if expect_rows_digest is not None and expect_rows_digest != recomputed:
        reasons.append("expected_rows_digest_mismatch")
    if expect_policy_digest is not None and expect_policy_digest != stored["policy_digest"]:
        reasons.append("expected_policy_digest_mismatch")
    if expect_current and pointer != publication_id:
        reasons.append("not_current_pointer")
    return {
        "publication_id": publication_id,
        "ok": not reasons,
        "reasons": reasons,
        "recomputed_rows_digest": recomputed,
        "recomputed_row_count": len(rows),
        "recomputed_d_confirmed_count": d_confirmed,
        "recomputed_d_candidate_count": d_candidate,
        "current_pointer": pointer,
        "is_current": pointer == publication_id,
        "runtime_policy_digest": policy.POLICY_DIGEST,
        "stored": stored,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dsn", default=None, help="datalake DSN; defaults to DATABASE_URL")
    parser.add_argument("--publication-id", required=True)
    parser.add_argument("--expect-rows-digest", default=None, metavar="SHA256")
    parser.add_argument("--expect-policy-digest", default=None, metavar="SHA256")
    parser.add_argument("--expect-current", action="store_true", help="the pointer must name this publication")
    parser.add_argument("--statement-timeout-seconds", type=int, default=1800)
    args = parser.parse_args(argv)
    try:
        with read_only_connect(args.dsn, statement_timeout_s=args.statement_timeout_seconds) as conn:
            result = verify(
                conn, publication_id=args.publication_id,
                expect_rows_digest=args.expect_rows_digest,
                expect_policy_digest=args.expect_policy_digest,
                expect_current=args.expect_current,
            )
    except psycopg.Error as exc:
        print(json.dumps({"ok": False, "error": type(exc).__name__}), file=sys.stderr)
        return 2
    print(json.dumps(result, default=str, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
