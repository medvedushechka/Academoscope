FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY common ./common
COPY services ./services

RUN pip install --no-cache-dir -r services/metrics_worker/requirements.txt

CMD ["python", "services/metrics_worker/worker.py"]
