FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DUCKDB_PATH=/app/data/flock.duckdb
ENV AIRFLOW_API_BASE_URL=http://airflow-webserver:8080/api/v1
ENV AIRFLOW_API_USER=admin
ENV AIRFLOW_API_PASSWORD=admin
ENV PYTHONPATH=/app

COPY src/backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/backend /app
COPY src /app/src

EXPOSE 3001
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "3001"]
