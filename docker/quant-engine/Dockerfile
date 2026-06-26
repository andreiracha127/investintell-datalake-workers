FROM python:3.13-slim

WORKDIR /app

COPY requirements.quant-engine.lock /app/requirements.quant-engine.lock
RUN pip install --no-cache-dir -r /app/requirements.quant-engine.lock

COPY packages/investintell_quant_core /app/packages/investintell_quant_core
COPY services/quant_engine /app/services/quant_engine
COPY qc_a3_core.py /app/qc_a3_core.py
COPY src /app/src
COPY docker/quant-engine/entrypoint.sh /app/docker/quant-engine/entrypoint.sh

ENV PYTHONPATH=/app:/app/packages/investintell_quant_core/src:/app/services/quant_engine/src
ENTRYPOINT ["/app/docker/quant-engine/entrypoint.sh"]

