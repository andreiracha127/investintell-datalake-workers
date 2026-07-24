# Fleet image for the datalake workers (dl-* Cloud Run jobs).
# Build (from repo root, records provenance in Cloud Build history):
#   gcloud builds submit --region=southamerica-east1 \
#     --tag southamerica-east1-docker.pkg.dev/investintell-research-analisys/hub/workers:<git-sha> .
# Run contract: WORKER=<name> python -m src.run_worker (see src/run_worker.py).
#
# SCOPE: the COPY set (src/ schemas/ contracts/) serves the current dl-* fleet
# vocabulary. Workers that read repo dirs outside it at runtime (open_macro_v03
# imports harness/ and scripts/ and reads committed artifacts/) will fail loudly
# with ImportError on this image — extend the COPY set before pointing them here.

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
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1
CMD ["python", "-m", "src.run_worker"]
