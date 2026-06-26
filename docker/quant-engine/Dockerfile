# Pin the base image by digest, not by mutable tag, so the build is reproducible.
# Digest resolved with: docker buildx imagetools inspect python:3.13-slim
FROM python:3.13-slim@sha256:2b7445fb71ca9cb15e9aab053fe8cb3162796f8e1d92ada12a49c766a811bc1e

WORKDIR /app

COPY requirements.quant-engine.lock /app/requirements.quant-engine.lock
RUN pip install --no-cache-dir -r /app/requirements.quant-engine.lock

COPY packages/investintell_quant_core /app/packages/investintell_quant_core
COPY services/quant_engine /app/services/quant_engine
COPY contracts/quant-engine /app/contracts/quant-engine
COPY qc_a3_core.py /app/qc_a3_core.py
COPY src /app/src
COPY --chmod=755 docker/quant-engine/entrypoint.sh /app/docker/quant-engine/entrypoint.sh

ENV PYTHONPATH=/app:/app/packages/investintell_quant_core/src:/app/services/quant_engine/src

# Least privilege: run as a non-root uid. Dependencies are installed to
# system site-packages (world-readable) before the switch, and the engine only
# writes to the caller-provided output mount and /tmp, so no app path needs to
# be writable by this user.
USER 65532:65532
ENTRYPOINT ["/app/docker/quant-engine/entrypoint.sh"]
