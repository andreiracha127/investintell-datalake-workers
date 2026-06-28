"""Railway CI gate: verify the golden certified input pack.

Extracted from a Dockerfile ``RUN`` heredoc so the image builds under both the
BuildKit and legacy Docker builders (heredoc ``RUN`` blocks are BuildKit-only
syntax). Run from the image WORKDIR (``/app``) via ``python
docker/railway-ci/verify_input_pack.py``.
"""

from pathlib import Path
import json

from src.input_packs.verifier import verify_pack

pack = Path("fixtures/input_packs/golden/certified_input_pack")
result = verify_pack(pack)
print(json.dumps({
    "ok": result["ok"],
    "input_pack_sha256_match": result["input_pack_sha256_match"],
    "expected_input_pack_sha256": result["expected_input_pack_sha256"],
    "actual_input_pack_sha256": result["actual_input_pack_sha256"],
}, sort_keys=True))
if not result["ok"] or not result["input_pack_sha256_match"]:
    raise SystemExit(1)
