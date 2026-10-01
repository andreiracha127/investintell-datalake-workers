# Dedicated image for the `bond-live-daily` Railway service (WORKER=bond_live_daily).
#
# WHY A DEDICATED IMAGE. The root railway.toml builds every worker service with
# Nixpacks from the UNPINNED root requirements.txt, so the numeric stack a daily
# publication is computed on (numpy / pandas / scipy / pyarrow, and the SIMD
# paths np.log dispatches to) is whatever resolved on build day and is not
# recorded anywhere. The implied-rating product (daily stage 7) is a
# bit-for-bit replayed publication whose rows_digest must be reproducible on
# the exact production stack (DG-4 Phase 4, G1(a)); this image makes that
# stack a reviewed, hash-locked, digest-pinned input instead of an accident.
#
# Build context = repository root:
#   docker buildx build --platform linux/amd64 -f docker/bond-live-daily/Dockerfile .
# Config: railway.bond-live-daily.toml (attach ONLY to the bond-live-daily service).
#
# The base image is pinned by the linux/amd64 MANIFEST digest of
# python:3.13.12-slim (3.13.12-slim-trixie, built 2026-04-07), resolved with
# `docker buildx imagetools inspect python:3.13.12-slim` on 2026-10-01; its
# multi-arch index digest was sha256:f1927c75e81efd1e091dbd64b6c0ecaa5630b38635a3d1c04034ac636e1f94c8.
FROM python:3.13.12-slim@sha256:f1fbe55d40a15f6b118d8688b2c762bb57309b96c30d561435e91a13fd97780c

WORKDIR /app

# Single-threaded BLAS/OpenMP: a reduction's floating-point result must not
# depend on how many threads the host happens to grant the container.
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    OMP_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    BLIS_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1

# The interpreter must be EXACTLY the one the lock was compiled for; a base
# image rebuilt under the same tag with another patch release fails here.
RUN python -c "import sys; assert sys.version_info[:3] == (3, 13, 12), sys.version; assert sys.implementation.name == 'cpython', sys.implementation.name"

# The lock (and its inputs) ship inside the image so the runtime build manifest
# can hash the very file the stack was installed from. The sha256 sidecars are
# computed at build time and re-verified by src/bonds/build_manifest.py.
COPY docker/bond-live-daily/Dockerfile \
     docker/bond-live-daily/requirements.in \
     docker/bond-live-daily/requirements.lock \
     /app/docker/bond-live-daily/
RUN cd /app/docker/bond-live-daily \
    && sha256sum requirements.lock > requirements.lock.sha256 \
    && sha256sum Dockerfile > Dockerfile.sha256 \
    && sha256sum -c requirements.lock.sha256 Dockerfile.sha256

# Wheels only, every artifact hash-verified, nothing resolved at build time.
RUN python -m pip install --require-hashes --only-binary=:all: --no-cache-dir \
        -r /app/docker/bond-live-daily/requirements.lock \
    && python -m pip check

# Runtime closure of the daily service. Nixpacks shipped the whole checkout;
# this set is what the seven daily stages (and the implied-rating backfill /
# determinism CLI) import or read at runtime: the package tree, the DDL files
# the materializers read (schemas/*.sql -- verified by grepping every file read
# in the import closure of src.workers.bond_live_daily), the bond contracts and
# frozen configs some sibling workers pin, and the operator scripts. No .git:
# the code revision comes from RAILWAY_GIT_COMMIT_SHA (the worker refuses to
# publish under "unknown").
COPY src/ /app/src/
COPY schemas/ /app/schemas/
COPY contracts/ /app/contracts/
COPY configs/ /app/configs/
COPY scripts/ /app/scripts/

# Build-time proof that the exact pinned stack is present, the shipped lock
# matches its recorded digest, and the whole daily closure imports on it
# (statsmodels via panel_resolvers included). Operators can re-run the same
# script inside a deployed container.
COPY docker/bond-live-daily/verify_image.py /app/docker/bond-live-daily/
RUN python /app/docker/bond-live-daily/verify_image.py

CMD ["python", "-m", "src.run_worker"]
