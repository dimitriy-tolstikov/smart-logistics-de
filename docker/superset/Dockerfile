FROM apache/superset:4.0.2

USER root
COPY requirements.txt /tmp/superset-extra-requirements.txt
RUN pip install --no-cache-dir -r /tmp/superset-extra-requirements.txt

COPY bootstrap.sh /app/docker/bootstrap.sh
COPY create_dashboard.py /app/docker/create_dashboard.py
COPY superset_config.py /app/pythonpath/superset_config.py
RUN chmod +x /app/docker/bootstrap.sh \
    && mkdir -p /app/pythonpath \
    && chown -R superset:superset /app/docker /app/pythonpath

USER superset
ENV PYTHONPATH="/app/pythonpath:${PYTHONPATH}"
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
