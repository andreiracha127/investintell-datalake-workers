"""Public N-CEN V2 worker; package lock prevents concurrent same-run landing."""
from __future__ import annotations
import os
from pathlib import Path
from src.db import LOCK_NCEN_INGESTION, advisory_lock, connect
from src.ncen.ingestion import ingest_package, source_quarter_from_package
from src.ncen.storage import install_schema
from src.sec_regulatory.manifests import install_schema as install_manifest_schema
SOURCE_ROOT=Path(os.getenv("NCEN_SOURCE_ROOT",r"E:\\Edgard\\ncen"))
def run(dsn: str, *, calc_date: str|None=None, limit: int|None=None)->dict[str,object]:
    packages=sorted(x for x in SOURCE_ROOT.iterdir() if x.is_dir())
    if calc_date:
        cutoff=calc_date.upper() if "Q" in calc_date.upper() else calc_date[:4]+"Q"+str((int(calc_date[5:7])-1)//3+1)
        packages=[x for x in packages if source_quarter_from_package(x)<=cutoff]
    if limit is not None: packages=packages[:limit]
    with connect(dsn) as conn, advisory_lock(conn,LOCK_NCEN_INGESTION) as acquired:
        if not acquired:return {"state":"locked","packages":0}
        install_manifest_schema(conn); install_schema(conn); results=[]
        for package in packages: results.append(ingest_package(conn,package=package,source_root=SOURCE_ROOT)); conn.commit()
    failed=sum(x.get("state")=="failed" for x in results)
    return {"state":"failed" if failed else "ok","packages":len(results),"failed_packages":failed,"results":results}
