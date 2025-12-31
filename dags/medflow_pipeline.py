"""
MedFlow Analytics - Medallion Architecture Pipeline
Bronze → Silver → Gold healthcare claims data pipeline

This DAG orchestrates the complete data flow:
1. Bronze: Ingest raw CSV files from S3 to Iceberg
2. Silver: Transform and validate data, quarantine bad records
3. Gold: Create business-ready aggregation tables

Author: MedFlow Analytics Team
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
    Bronze layer: Ingest raw CSV files from S3 to Iceberg
    - Preserves original data format
    - Adds metadata columns
    - Registers files in Iceberg table
    """

    run_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/bronze_ingestion.py',
        dag=dag,
    )

    verify_bronze = BashOperator(
        task_id='verify_bronze',
        bash_command='docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/verify_bronze.py',
        dag=dag,
    )

    run_bronze >> verify_bronze

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
