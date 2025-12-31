"""
Cleanup Iceberg Tables - MedFlow Analytics
Drops all Bronze, Silver, and Gold tables for a clean slate
"""

from pyspark.sql import SparkSession

def main():
    print("\n" + "="*70)
    print("ICEBERG CLEANUP - Dropping all tables")
    print("="*70 + "\n")

    # Initialize Spark
    spark = SparkSession.builder.appName("IcebergCleanup").getOrCreate()

    # List of tables to drop
    tables_to_drop = [
        # Bronze tables
        "iceberg.bronze.claims_raw",

        # Silver tables
        "iceberg.silver.claims",
        "iceberg.silver.claims_quarantine",
        "iceberg.silver.processing_log",

        # Gold tables
        "iceberg.gold.claims_monthly_summary",
        "iceberg.gold.claims_approval_funnel",
        "iceberg.gold.provider_performance_metrics",
    ]

    for table in tables_to_drop:
        try:
            # Check if table exists
            namespace, table_name = table.split('.')[1], table.split('.')[2]
            tables = spark.sql(f"SHOW TABLES IN iceberg.{namespace}").collect()
            table_names = [row.tableName for row in tables]

            if table_name in table_names:
                print(f"Dropping {table}...")
                spark.sql(f"DROP TABLE IF EXISTS {table}")
                print(f"✓ Dropped {table}")
            else:
                print(f"⊘ Table {table} does not exist (skipping)")
        except Exception as e:
            print(f"⚠ Error dropping {table}: {e}")

    print("\n" + "="*70)
    print("CLEANUP COMPLETE")
    print("="*70 + "\n")

    print("Next steps:")
    print("1. Recreate Bronze table: run bronze_ingestion.py")
    print("2. Or trigger the Airflow DAG to run the full pipeline")
    print()

    spark.stop()

if __name__ == "__main__":
    main()
