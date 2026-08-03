# Fleet image for the datalake workers (dl-* Cloud Run jobs).
# Build (from repo root, records provenance in Cloud Build history):
#   gcloud builds submit --region=southamerica-east1 \
#     --tag southamerica-east1-docker.pkg.dev/investintell-research-analisys/hub/workers:<git-sha> .
# Run contract: WORKER=<name> python -m src.run_worker (see src/run_worker.py).
#
# SCOPE: the COPY set serves the current dl-* fleet vocabulary. A worker that reads
# repo dirs outside it at runtime fails loudly with ImportError/FileNotFoundError on
# this image — extend the COPY set before pointing a new worker here.
#
# open_macro_v03 (job dl-open-macro-v03) is the reason the set is wider than
# src/schemas/contracts: its gates read the pinned pure modules under harness/ and
# scripts/, the ratified Stage B governance artifact under artifacts/a5/, and the
# certified input pack under fixtures/p1_packs/ (~48 MB). Those are RUNTIME inputs of
# the fail-closed gates — a byte of them missing must abort the run, not be shrugged
# off — so they belong in the image, verified by sha256 on every execution.

FROM python:3.12-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# TA-Lib C library — FALLBACK ONLY: on linux/amd64 the TA-Lib python package
# resolves to a manylinux wheel that bundles its own C-lib (verified: the wheel
# ships ta_lib.libs/libta-lib*.so), so this stage is never linked there. It
# exists for platforms/versions where pip falls back to a source build.
ARG TALIB_VERSION=0.6.4
RUN wget -q "https://github.com/ta-lib/ta-lib/releases/download/v${TALIB_VERSION}/ta-lib-${TALIB_VERSION}-src.tar.gz" \
    && tar -xzf "ta-lib-${TALIB_VERSION}-src.tar.gz" \
    && cd "ta-lib-${TALIB_VERSION}" \
    && ./configure --prefix=/usr/local >/dev/null \
    && make -j"$(nproc)" >/dev/null \
    && make install \
    && cd .. && rm -rf "ta-lib-${TALIB_VERSION}" "ta-lib-${TALIB_VERSION}-src.tar.gz"
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

FROM python:3.12-slim
COPY --from=build /usr/local/lib/libta-lib* /usr/local/lib/
COPY --from=build /opt/venv /opt/venv
RUN ldconfig
WORKDIR /app
COPY src/ /app/src/
COPY schemas/ /app/schemas/
COPY contracts/ /app/contracts/
# open_macro_v03 runtime inputs (see the SCOPE note above). Scoped to the ONE
# certified pack and the ONE ratified Stage B artifact directory the worker pins —
# the other packs/artifact bundles are evidence, not runtime inputs, and stay out.
COPY harness/ /app/harness/
COPY scripts/ /app/scripts/
COPY artifacts/a5/open_macro_v03_direct_activation_stage_b_001/ \
     /app/artifacts/a5/open_macro_v03_direct_activation_stage_b_001/
COPY fixtures/p1_packs/open_macro_v03_certified_input_pack_003/ \
     /app/fixtures/p1_packs/open_macro_v03_certified_input_pack_003/
# open_macro_v04 runtime input: the frozen formulation its gate 1 verifies
# (module sha256 pins + formulation_sha256) before any side effect. Same
# scoping rule as the a5 artifact above — the one directory the worker pins.
COPY artifacts/quant/open_macro_v4_formulation_freeze_001/ \
     /app/artifacts/quant/open_macro_v4_formulation_freeze_001/
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1
CMD ["python", "-m", "src.run_worker"]
