"""
Verify Bronze Layer - MedFlow Analytics
Query the Bronze table to see registered files
"""

from pyspark.sql import SparkSession

def main():
    print("\n" + "="*70)
    print("BRONZE LAYER VERIFICATION - MedFlow Analytics")
    print("="*70 + "\n")

    # Initialize Spark (catalog config comes from spark-submit)
    spark = SparkSession.builder.appName("BronzeVerification").getOrCreate()

    # Check if Bronze table exists
    print("Checking Bronze table...")
    tables = spark.sql("SHOW TABLES IN iceberg.bronze").collect()
    print(f"✓ Tables in bronze namespace: {[row.tableName for row in tables]}\n")

    # Count total files registered
    total_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.bronze.claims_raw").collect()[0]['cnt']
    print(f"✓ Total files registered: {total_count}\n")

    if total_count > 0:
        # Show breakdown by payer
        print("Files by payer:")
        spark.sql("""
            SELECT payer_name, COUNT(*) as file_count, SUM(record_count) as total_records
            FROM iceberg.bronze.claims_raw
            GROUP BY payer_name
            ORDER BY payer_name
        """).show(truncate=False)

        # Show breakdown by year/month
        print("\nFiles by year/month:")
        spark.sql("""
            SELECT file_year, file_month, COUNT(*) as file_count, SUM(record_count) as total_records
            FROM iceberg.bronze.claims_raw
            GROUP BY file_year, file_month
            ORDER BY file_year, file_month
        """).show(truncate=False)

        # Show sample of registered files
        print("\nSample of registered files:")
        spark.sql("""
            SELECT file_name, payer_name, file_year, file_month, record_count, processing_status
            FROM iceberg.bronze.claims_raw
            ORDER BY ingestion_timestamp DESC
            LIMIT 10
        """).show(truncate=False)

        # Show summary statistics
        print("\nSummary statistics:")
        spark.sql("""
            SELECT
                MIN(record_count) as min_records,
                MAX(record_count) as max_records,
                AVG(record_count) as avg_records,
                SUM(record_count) as total_records,
                SUM(file_size_bytes) / 1024 / 1024 as total_size_mb
            FROM iceberg.bronze.claims_raw
        """).show(truncate=False)
    else:
        print("⚠ No files registered in Bronze table yet")

    print("\n" + "="*70)
    print("VERIFICATION COMPLETE")
    print("="*70 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
