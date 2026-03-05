from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_DIR = f"{PROJECT}/Data_Harmo_dbt"
PYTHON  = "/usr/local/bin/python"
DBT     = "/home/airflow/.local/bin/dbt"

with DAG(
    dag_id="gaia_pipeline",
    description="DEPHY + AGROVOC -> DBT -> Elasticsearch",
    start_date=datetime(2026, 2, 22),
    schedule_interval=None,
    catchup=False,
    tags=["gaia", "biocontrol"],
) as dag:

    fetch_agrovoc = BashOperator(
        task_id="fetch_agrovoc",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/ingestion/fetch_agrovoc.py {{{{ ds_nodash }}}}",
    )

    normalize_dephy = BashOperator(
        task_id="normalize_dephy",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/formatting/normalize_dephy.py {{{{ ds_nodash }}}}",
    )

    normalize_agrovoc = BashOperator(
        task_id="normalize_agrovoc",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/formatting/normalize_agrovoc.py {{{{ ds_nodash }}}}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} seed --profiles-dir {DBT_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} run --profiles-dir {DBT_DIR} --vars \"{{run_date: '{{{{ ds_nodash }}}}'}}\""
        ),
    )

    export_parquet = BashOperator(
        task_id="export_parquet",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/export/export_parquet.py {{{{ ds_nodash }}}}",
    )

    index_es = BashOperator(
        task_id="index_es",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/indexing/index_alignments.py",
    )

    # ordre d'execution
    # fetch et normalize en parallele -> dbt seed + run -> export -> index
    [fetch_agrovoc, normalize_dephy] >> normalize_agrovoc >> dbt_seed >> dbt_run >> export_parquet >> index_es