"""Public RR1 V2 worker; caller integrates the RR1 advisory-lock constant."""
from __future__ import annotations

import os
from pathlib import Path

from src.db import LOCK_RR1_INGESTION, advisory_lock, connect
from src.rr1.ingestion import ingest_package, source_quarter_from_package
from src.rr1.storage import install_schema
from src.sec_regulatory.manifests import install_schema as install_manifest_schema

SOURCE_ROOT = Path(os.getenv("RR1_SOURCE_ROOT", r"E:\Edgard\RR1"))


_SUCCESS_STATES = {"raw_validated", "duplicate"}


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, object]:
    packages = sorted(path for path in SOURCE_ROOT.iterdir() if path.is_dir())
    if calc_date:
        cutoff = calc_date.upper() if "Q" in calc_date.upper() else calc_date[:4] + "Q" + str((int(calc_date[5:7]) - 1) // 3 + 1)
        packages = [path for path in packages if source_quarter_from_package(path) <= cutoff]
    if limit is not None:
        packages = packages[:limit]
    with connect(dsn) as conn, advisory_lock(conn, LOCK_RR1_INGESTION) as acquired:
        if not acquired:
            return {"state": "locked", "packages": 0}
        install_manifest_schema(conn)
        install_schema(conn)
        results = []
        for package in packages:
            result = ingest_package(conn, package=package, source_root=SOURCE_ROOT)
            results.append(result)
            package_state = result.get("state")
            if package_state in _SUCCESS_STATES:
                conn.commit()
                continue
            if package_state == "failed":
                conn.commit()
            else:
                conn.rollback()
            return {
                "state": "failed",
                "packages": len(results),
                "failed_package": package.name,
                "failed_state": package_state,
                "results": results,
            }
    return {"state": "ok", "packages": len(results), "results": results}
