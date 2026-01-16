"""
MedFlow Analytics - Medallion Architecture Pipeline
Bronze → Silver → Gold healthcare claims data pipeline

This DAG orchestrates the complete data flow:
1. Bronze: Upload CSV files to S3, then ingest to Iceberg
   - Upload: Local filesystem → S3 bucket (claims-raw)
   - Ingest: S3 bucket → Iceberg Bronze table
   - Verify: Data quality checks
2. Silver: Transform and validate data, quarantine bad records
3. Gold: Create business-ready aggregation tables

Author: MedFlow Analytics Team
Updated: 2026-01-13 - Added automated Bronze S3 upload task
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# ============================================================================
# DAG Configuration
# ============================================================================

default_args = {
    'owner': 'medflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'medflow_bronze_silver_gold_pipeline',
    default_args=default_args,
    description='Healthcare claims medallion pipeline: Bronze → Silver → Gold',
    schedule_interval=None,  # Manual trigger for development
    catchup=False,
    max_active_runs=1,
    tags=['healthcare', 'claims', 'medallion', 'iceberg', 'spark'],
)

# ============================================================================
# Task Groups
# ============================================================================

# ----------------------------------------------------------------------------
# Task Group 1: Bronze Ingestion
# ----------------------------------------------------------------------------

with TaskGroup('bronze_ingestion', dag=dag) as bronze_group:
    """
    Bronze layer: Upload raw files to S3, then ingest to Iceberg
    Step 1: Upload CSV files from local filesystem to S3 (NEW)
    Step 2: Ingest raw CSV files from S3 to Iceberg
    Step 3: Verify Bronze layer data
    """

    # Step 1: Upload local CSV files to S3 bucket
    upload_to_s3 = BashOperator(
        task_id='upload_bronze_to_s3',
        bash_command='docker exec medflow-spark-master python3 /opt/scripts/python/upload_bronze_to_s3.py',
        dag=dag,
    )

    # Step 2: Ingest from S3 to Iceberg Bronze table
    run_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/bronze_ingestion.py',
        dag=dag,
    )

    # Step 3: Verify Bronze layer
    verify_bronze = BashOperator(
        task_id='verify_bronze',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/verify_bronze.py',
        dag=dag,
    )

    # Task dependencies: upload → ingest → verify
    upload_to_s3 >> run_bronze >> verify_bronze

# ----------------------------------------------------------------------------
# Task Group 2: Silver Transformation
# ----------------------------------------------------------------------------

with TaskGroup('silver_transformation', dag=dag) as silver_group:
    """
    Silver layer: Transform and validate data
    - Standardize heterogeneous payer schemas
    - Apply data quality validations
    - Quarantine invalid records
    """

    run_silver = BashOperator(
        task_id='transform_silver',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/silver_transformation.py',
        dag=dag,
    )

    verify_silver = BashOperator(
        task_id='verify_silver',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/verify_silver.py',
        dag=dag,
    )

    check_quality = BashOperator(
        task_id='check_quarantine_rate',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/check_silver_quality.py',
        dag=dag,
    )

    run_silver >> verify_silver >> check_quality

# ----------------------------------------------------------------------------
# Task Group 3: Gold Aggregation
# ----------------------------------------------------------------------------

with TaskGroup('gold_aggregation', dag=dag) as gold_group:
    """
    Gold layer: Create business-ready aggregation tables
    - claims_monthly_summary: Monthly metrics by payer
    - claims_approval_funnel: Approval funnel by payer/type/month
    - provider_performance_metrics: Provider performance all-time
    """

    run_gold = BashOperator(
        task_id='aggregate_gold',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/gold_aggregation.py',
        dag=dag,
    )

    verify_gold = BashOperator(
        task_id='verify_gold',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/verify_gold.py',
        dag=dag,
    )

    run_gold >> verify_gold

# ============================================================================
# Pipeline Dependencies
# ============================================================================

# Start marker
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# End marker
pipeline_complete = EmptyOperator(
    task_id='pipeline_complete',
    dag=dag,
)

# Pipeline flow: Bronze → Silver → Gold
start >> bronze_group >> silver_group >> gold_group >> pipeline_complete
