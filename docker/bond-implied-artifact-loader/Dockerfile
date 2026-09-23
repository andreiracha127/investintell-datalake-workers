# Build only from the private allowlisted context containing this source closure
# and a non-empty artifact/ directory; repository auto-deploys fail closed.
# docker buildx build -f docker/bond-implied-artifact-loader/Dockerfile <private-context>
FROM python:3.13-slim@sha256:2b7445fb71ca9cb15e9aab053fe8cb3162796f8e1d92ada12a49c766a811bc1e

WORKDIR /app

ENV PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    BLIS_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1

COPY docker/bond-implied-artifact-loader/Dockerfile \
     docker/bond-implied-artifact-loader/requirements.lock \
     docker/bond-implied-artifact-loader/railway.toml \
     /app/docker/bond-implied-artifact-loader/
RUN python -m pip install --no-cache-dir --only-binary=:all: \
        --require-hashes -r /app/docker/bond-implied-artifact-loader/requirements.lock \
    && python -m pip check

# Keep the image limited to the loader and the eager src.bonds import closure.
COPY src/__init__.py src/db.py /app/src/
COPY src/bonds/__init__.py \
     src/bonds/cashflows.py \
     src/bonds/debt_mapping.py \
     src/bonds/errors.py \
     src/bonds/identifiers.py \
     src/bonds/implied_rating.py \
     src/bonds/implied_rating_artifact_loader.py \
     src/bonds/implied_rating_materializer.py \
     src/bonds/matching.py \
     src/bonds/oas.py \
     src/bonds/panel_states.py \
     src/bonds/pricing.py \
     src/bonds/states.py \
     /app/src/bonds/
COPY scripts/__init__.py \
     scripts/load_bond_market_implied_rating_artifact.py \
     /app/scripts/
COPY schemas/sec_derived_publications.sql \
     schemas/bond_market_implied_rating_v1.sql \
     /app/schemas/
COPY contracts/bond_market_implied_rating_round002_artifact.json \
     contracts/bond_market_implied_rating_round002_input_identity.json \
     /app/contracts/
RUN python -c "from pathlib import Path; root=Path('/app'); required=('docker/bond-implied-artifact-loader/Dockerfile','docker/bond-implied-artifact-loader/requirements.lock','docker/bond-implied-artifact-loader/railway.toml','contracts/bond_market_implied_rating_round002_artifact.json','contracts/bond_market_implied_rating_round002_input_identity.json'); assert all((root / path).is_file() for path in required)" \
    && python -c "import scripts.load_bond_market_implied_rating_artifact" \
    && command -v timeout >/dev/null

# Root-only startup bootstrap for the mounted /evidence volume. Its reviewed digest is a
# literal here, so a changed wrapper fails the build instead of reaching the root phase.
COPY docker/bond-implied-artifact-loader/bootstrap_evidence.py \
     /app/docker/bond-implied-artifact-loader/
RUN echo "d4ccd3ecfae99ecb64bea69b17ade2893857cc617eb20eca2ab5369d263db678  /app/docker/bond-implied-artifact-loader/bootstrap_evidence.py" | sha256sum -c - \
    && chown 0:0 /app/docker/bond-implied-artifact-loader/bootstrap_evidence.py \
    && chmod 0444 /app/docker/bond-implied-artifact-loader/bootstrap_evidence.py \
    && chown 0:0 /app /app/docker /app/docker/bond-implied-artifact-loader \
    && chmod 0755 /app /app/docker /app/docker/bond-implied-artifact-loader \
    && python -B -I -S -c "import os, stat; paths=('/', '/app', '/app/docker', '/app/docker/bond-implied-artifact-loader', '/usr/bin', '/usr/bin/timeout', '/usr/local/bin/python', '/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py'); facts=[os.stat(p) for p in paths]; assert all(s.st_uid == 0 and not s.st_mode & (stat.S_IWGRP | stat.S_IWOTH | stat.S_ISUID | stat.S_ISGID) for s in facts); assert stat.S_ISREG(os.lstat('/usr/bin/timeout').st_mode); assert stat.S_IMODE(facts[-1].st_mode) == 0o444" \
    && test "$(command -v timeout)" = /usr/bin/timeout \
    && test "$(command -v python)" = /usr/local/bin/python \
    && python -B -I -S -c "import ctypes, hashlib, json, os, re, stat, sys; compile(open('/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py', 'rb').read(), 'bootstrap_evidence.py', 'exec')"

# The private build context supplies artifact/; it is not part of the repository.
COPY artifact/ /artifact/
RUN test -n "$(ls -A /artifact)" \
    && chmod -R a=rX /artifact \
    && mkdir -p /evidence \
    && chown 65532:65532 /evidence

# Only the startup wrapper runs as root. It validates the exact argv and mounted /evidence,
# changes that mount root alone, drops to 65532 behind no_new_privs, then execs the CMD.
# Railway's custom start command replaces ENTRYPOINT, so railway.toml names it explicitly.
USER 0:0

ENTRYPOINT ["/usr/local/bin/python", "-I", "-S", "/app/docker/bond-implied-artifact-loader/bootstrap_evidence.py"]

CMD ["timeout", "--signal=TERM", "--kill-after=30s", "9000s", "python", "-m", "scripts.load_bond_market_implied_rating_artifact", "--artifact-root", "/artifact", "--evidence-dir", "/evidence", "--verify-only"]
