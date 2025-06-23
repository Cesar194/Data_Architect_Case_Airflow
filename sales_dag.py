from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

# =============================================================================
# --- CONFIGURACIÓN FINAL Y CORRECTA ---
# =============================================================================
GCP_PROJECT_ID = "directed-gasket-309507"
GCP_REGION = "us-east1"

# --- BUCKETS ---
# Bucket para los DATOS DE ENTRADA (CSVs de ventas, temp y staging)
GCS_DATA_BUCKET = "retail-data-zone"

# Bucket del ambiente de COMPOSER (donde están los scripts .py)
GCS_COMPOSER_BUCKET = "us-east1-composerdynnk-4f139b64-bucket"

# Ruta del script de Beam DENTRO del bucket de Composer
GCS_BEAM_SCRIPT_PATH = "dags/beam_sales_pipeline.py"
# =============================================================================

default_args = {
    "owner": "Data Architecture Team",
    "start_date": pendulum.datetime(2025, 5, 25, tz="UTC"),
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "dataflow_conn_id": "google_cloud_default",
}

with DAG(
    dag_id="daily_sales_pipeline_final",
    default_args=default_args,
    schedule="0 5 * * *",
    catchup=False,
    tags=["sales", "dataflow", "retail"],
    doc_md="DAG final que apunta a los buckets correctos para datos y scripts."
) as dag:

    run_beam_pipeline = DataflowCreatePythonJobOperator(
        task_id="run_sales_beam_pipeline",
        
        # CAMBIO CLAVE: Apuntamos al bucket de COMPOSER para el script de Python
        py_file=f"gs://{GCS_COMPOSER_BUCKET}/{GCS_BEAM_SCRIPT_PATH}",
        
        job_name=f"sales-etl-{{{{ ds_nodash }}}}",
        project_id=GCP_PROJECT_ID,
        location=GCP_REGION,
        options={
            # Las opciones de Dataflow siguen apuntando al bucket de DATOS
            "temp_location": f"gs://{GCS_DATA_BUCKET}/temp/",
            "staging_location": f"gs://{GCS_DATA_BUCKET}/staging/",
            "input_file": f"gs://{GCS_DATA_BUCKET}/sales/{{{{ ds }}}}.csv",
            "output_table": f"{GCP_PROJECT_ID}:retail.analytics_sales_usd",
            "exchange_rates_table": f"{GCP_PROJECT_ID}:retail.ref_exchange_rates",
        },
    )