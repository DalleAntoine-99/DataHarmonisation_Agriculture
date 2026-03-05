FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y gcc g++ git && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --quiet \
    openpyxl==3.1.5 \
    pyarrow==14.0.2 \
    elasticsearch==8.19.3 \
    duckdb==0.9.2 \
    dbt-core==1.7.4 \
    dbt-duckdb==1.7.2