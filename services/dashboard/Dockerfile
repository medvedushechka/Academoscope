FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FLASK_APP=services.dashboard.app

WORKDIR /app

COPY common ./common
COPY services ./services

RUN pip install --no-cache-dir -r services/dashboard/requirements.txt

EXPOSE 5000

CMD ["flask", "run", "--host", "0.0.0.0", "--port", "5000"]
