"""Certified input-pack primitives.

The v1 surface is intentionally small: deterministic JSON hashing, manifest
assembly, and offline verification. Worker migrations and quant-engine runtime
activation live in later waves.
"""

from .hashing import canonical_json_bytes, canonical_json_sha256, file_sha256
from .manifest import build_manifest, compute_input_pack_sha256, write_manifest
from .verifier import verify_pack

__all__ = [
    "build_manifest",
    "canonical_json_bytes",
    "canonical_json_sha256",
    "compute_input_pack_sha256",
    "file_sha256",
    "verify_pack",
    "write_manifest",
]

