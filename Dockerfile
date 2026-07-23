# Fleet image for the datalake workers (dl-* Cloud Run jobs).
# Build (from repo root, records provenance in Cloud Build history):
#   gcloud builds submit --region=southamerica-east1 \
#     --tag southamerica-east1-docker.pkg.dev/investintell-research-analisys/hub/workers:<git-sha> .
# Run contract: WORKER=<name> python -m src.run_worker (see src/run_worker.py).

FROM python:3.12-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# TA-Lib C library (required by the TA-Lib python package in requirements.txt).
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
