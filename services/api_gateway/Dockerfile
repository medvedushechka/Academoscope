FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY common ./common
COPY services ./services

RUN pip install --no-cache-dir -r services/api_gateway/requirements.txt

EXPOSE 8000

CMD ["uvicorn", "services.api_gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
