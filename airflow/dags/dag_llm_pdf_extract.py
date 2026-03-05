from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = "/opt/airflow/project"
PYTHON  = "/usr/local/bin/python"

with DAG(
    dag_id="gaia_llm_pdf_extract",
    description="Extraction LLM depuis fiches système DEPHY (PDF) → enrichissement base consolidée",
    start_date=datetime(2026, 2, 22),
    schedule_interval=None,
    catchup=False,
    tags=["gaia", "llm", "pdf", "enrichment"],
) as dag:

    extract_pdfs = BashOperator(
        task_id="extract_pdfs",
        bash_command=f"{PYTHON} {PROJECT}/pipeline/ingestion/llm_pdf_extract.py",
        execution_timeout=None,   # pas de timeout — peut prendre du temps
    )

    trigger_pipeline = TriggerDagRunOperator(
        task_id="trigger_gaia_pipeline",
        trigger_dag_id="gaia_pipeline",
        wait_for_completion=False,
    )

    extract_pdfs >> trigger_pipeline