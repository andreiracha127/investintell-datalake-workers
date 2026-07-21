"""Worker público da landing N-PORT, protegido por advisory lock."""

from __future__ import annotations

import os
from pathlib import Path

from src.db import LOCK_NPORT_INGESTION, advisory_lock, connect
from src.nport.ingestion import ingest_package
from src.nport.storage import install_schema
from src.sec_regulatory.manifests import install_schema as install_manifest_schema


SOURCE_ROOT = Path(os.getenv("NPORT_SOURCE_ROOT", r"E:\Edgard\nport"))


def run(dsn: str, *, calc_date: str | None = None, limit: int | None = None) -> dict[str, object]:
    """Ingere pacotes N-PORT locais em streaming; ``calc_date`` limita até seu trimestre."""
    packages = sorted(path for path in SOURCE_ROOT.iterdir() if path.is_dir())
    if calc_date is not None:
        cutoff = calc_date[:6].upper().replace("-", "Q") if "Q" in calc_date.upper() else calc_date[:4] + "Q" + str(((int(calc_date[5:7]) - 1) // 3) + 1)
        from src.nport.ingestion import source_quarter_from_package
        packages = [path for path in packages if source_quarter_from_package(path) <= cutoff]
    if limit is not None:
        packages = packages[:limit]
    with connect(dsn) as conn, advisory_lock(conn, LOCK_NPORT_INGESTION) as acquired:
        if not acquired:
            return {"state": "locked", "packages": 0}
        install_manifest_schema(conn)
        install_schema(conn)
        results = []
        for package in packages:
            results.append(ingest_package(conn, package=package, source_root=SOURCE_ROOT))
            conn.commit()
    failed = sum(result.get("state") == "failed" for result in results)
    return {"state": "failed" if failed else "ok", "packages": len(results), "failed_packages": failed, "results": results}
