# Railway CI image for quant-engine calibration PR gates.
# This is intentionally separate from production worker Dockerfiles.
FROM python:3.13-slim@sha256:2b7445fb71ca9cb15e9aab053fe8cb3162796f8e1d92ada12a49c766a811bc1e

WORKDIR /app

ENV PYTHONPATH=/app:/app/packages/investintell_quant_core/src:/app/services/quant_engine/src \
    PYTEST_DISABLE_PLUGIN_AUTOLOAD=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.quant-engine.lock /app/requirements.quant-engine.lock
RUN pip install --no-cache-dir -r /app/requirements.quant-engine.lock jsonschema pytest

COPY packages/investintell_quant_core /app/packages/investintell_quant_core
COPY services/quant_engine /app/services/quant_engine
COPY contracts/quant-engine /app/contracts/quant-engine
COPY schemas/input_packs /app/schemas/input_packs
COPY scripts /app/scripts
COPY src /app/src
COPY tests /app/tests
COPY fixtures/input_packs /app/fixtures/input_packs
COPY artifacts/calibration/open_macro_v03_calibration_001 /app/artifacts/calibration/open_macro_v03_calibration_001
COPY artifacts/shadow/open_macro_v03_shadow_001 /app/artifacts/shadow/open_macro_v03_shadow_001
COPY artifacts/shadow/open_macro_v03_shadow_pilot_001 /app/artifacts/shadow/open_macro_v03_shadow_pilot_001
COPY .githooks /app/.githooks
COPY docs/calibration /app/docs/calibration
COPY docs/architecture /app/docs/architecture
COPY docs/shadow /app/docs/shadow
COPY qc-a3-parity /app/qc-a3-parity
COPY docker/railway-ci /app/docker/railway-ci
COPY docker/quant-engine /app/docker/quant-engine
COPY qc_a3_core.py /app/qc_a3_core.py

RUN git init . \
    && git config user.email "ci@investintell.local" \
    && git config user.name "Investintell CI" \
    && git add . \
    && git commit -m "railway ci synthetic checkout" \
    && git rev-parse --verify HEAD

RUN python docker/railway-ci/verify_input_pack.py

RUN python scripts/contract_bundle.py verify
RUN python -m pytest tests/input_packs tests/quant_engine tests/quant_core \
    tests/test_calibration_candidate.py tests/test_qc_a3_core.py \
    tests/test_catalog_hash_path_independence.py tests/test_repeatability_matrix.py \
    tests/test_shadow_readiness.py tests/test_shadow_pilot.py tests/test_remote_ci_runner.py -q
RUN python -m compileall src/calibration_candidate.py src/shadow_pilot.py src/input_packs services/quant_engine packages/investintell_quant_core

RUN python docker/railway-ci/verify_calibration_artifacts.py

CMD ["sh", "-c", "python -m http.server \"${PORT:-8080}\" --bind 0.0.0.0"]
