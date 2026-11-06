FROM apache/airflow:2.9.3-python3.12

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=airflow:root requirements.txt /requirements-ul.txt

USER airflow
RUN pip install --no-cache-dir "psycopg2-binary==2.9.10" -r /requirements-ul.txt

ENV UL_PROJECT_ROOT=/opt/airflow/project
ENV PYTHONPATH=/opt/airflow/project
