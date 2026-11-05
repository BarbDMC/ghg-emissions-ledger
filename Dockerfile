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

# dbt lives in its own venv: its pins would otherwise clash with Airflow's constraints.
# The base image sets PIP_USER=true, which pip rejects inside a venv.
RUN python -m venv /home/airflow/dbt_venv \
    && PIP_USER=false /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core~=1.10.0" \
        "dbt-postgres~=1.9.0"
