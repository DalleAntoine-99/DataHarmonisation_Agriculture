from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = "/opt/airflow/project"
PYTHON  = "/usr/local/bin/python"

with DAG(
    dag_id="gaia_llm_synonyms",
    description="Enrichissement sémantique LLM — mapping DEPHY → AGROVOC",
    start_date=datetime(2026, 2, 22),
    schedule_interval=None,
    catchup=False,
    tags=["gaia", "llm", "enrichment"],
) as dag:

    run_llm_mapping = BashOperator(
        task_id="run_llm_mapping",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/enrichment/llm_synonym_mapping.py",
        execution_timeout=None,
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_gaia_pipeline",
        trigger_dag_id="gaia_pipeline",
        wait_for_completion=False,
    )

    run_llm_mapping >> trigger_pipeline