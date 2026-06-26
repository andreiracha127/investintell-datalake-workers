# Quant Engine — Supply Chain & Sandbox Hardening

Date: 2026-06-26
Lane: `feat/quant-engine-isolation`
Gate: report "CI/CD e cadeia de suprimentos" + "Segurança, sandbox e observabilidade".

## Done and verified

### Base image pinned by digest

`docker/quant-engine/Dockerfile` pins the base by digest, not the mutable tag:

```dockerfile
FROM python:3.13-slim@sha256:2b7445fb71ca9cb15e9aab053fe8cb3162796f8e1d92ada12a49c766a811bc1e
```

Resolve/update with `docker buildx imagetools inspect python:3.13-slim`. Tags are
mutable; digests are immutable, so the build base can no longer drift silently.

### Hardened execution profile

The engine runs under least privilege, enforced both in `compose.quant-engine.yml`
and in the determinism harness (`scripts/repeatability_matrix.py` container leg),
so the gate exercises exactly the hardened profile:

| Control | Setting | Rationale |
|---|---|---|
| Network | `--network none` | No egress during compute |
| User | `--user 65532:65532` (also `USER` in Dockerfile) | Non-root |
| Root FS | `--read-only` | No writes outside explicit mounts |
| Privilege | `--security-opt no-new-privileges` | No privilege escalation |
| Capabilities | `--cap-drop ALL` | Drop all Linux capabilities |
| Processes | `--pids-limit 256` | Fork-bomb containment |
| Memory | `--memory 4g` | Bounded RAM |
| CPU | `--cpus 2` | Bounded CPU |
| Temp | `--tmpfs /tmp` | Writable scratch without a writable root FS |
| Inputs | bind mount `:ro` / `readonly` | Inputs cannot be mutated |
| Outputs | single writable bind mount `/outputs` | Only sanctioned write target |

**Evidence:** after rebuilding the hardened image, the host-vs-container
determinism matrix is bit-a-bit identical (`mismatch_count=0`, 8 runs). Non-root +
read-only root FS + tmpfs `/tmp` did not change any output.

## Deferred (documented follow-ups)

### Hashed dependency lock (`pip --require-hashes`)

`requirements.quant-engine.lock` pins top-level versions but carries no hashes, and
`--require-hashes` additionally requires every transitive dependency to be pinned
with a hash. Generating that full tree is network-heavy and, more importantly,
risks shifting the exact resolved versions the determinism evidence was captured
against. It must therefore be done deliberately, with a golden re-capture, not
inline here.

Procedure when adopted:

```bash
pip install pip-tools
pip-compile --generate-hashes --output-file=requirements.quant-engine.lock \
    requirements.quant-engine.in
# then in the Dockerfile:
#   RUN pip install --no-cache-dir --require-hashes -r requirements.quant-engine.lock
```

After regenerating, re-run `scripts/repeatability_matrix.py` and re-capture the
golden if any logical hash changes.

### SBOM + provenance attestation (CI)

Build-time SBOM and provenance belong in CI (they need the registry/build runner),
so they are part of the governance wave, not the local build:

```bash
docker buildx build \
  --attest type=sbom \
  --attest type=provenance,mode=max,version=v1 \
  --tag registry.example/quant-engine:git-$GIT_SHA --push .
```

The published image should then be referenced by digest (`@sha256:...`) downstream,
and the digest recorded in the job envelope (`engine_image_digest`, already a
required field in `job-request.schema.json`).
