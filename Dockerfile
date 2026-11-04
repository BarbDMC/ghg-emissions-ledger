FROM apache/airflow:2.8.1-python3.10

ARG AIRFLOW_VERSION=2.8.1
ARG PYTHON_VERSION=3.10

USER airflow

# Airflow's constraints file pins transitive deps so extra packages can't break Airflow itself.
RUN pip install --no-cache-dir \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
        "apache-airflow==${AIRFLOW_VERSION}" \
        minio \
        psycopg2-binary \
        requests \
        python-dotenv
