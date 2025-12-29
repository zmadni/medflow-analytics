#!/usr/bin/env python3
"""
Bronze Layer Ingestion - MedFlow Analytics

Scans S3 for raw claims CSV files and registers metadata in Iceberg Bronze catalog.

Purpose:
- Track all source files ingested into the data lake
- Provide audit trail for regulatory compliance
- Enable idempotent processing (prevent duplicates)
- Support data lineage tracking

Usage:
    # Run via Spark submit with Iceberg support
    ./scripts/run_spark_iceberg.sh /opt/scripts/bronze_ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import sys


def create_spark_session():
    """Initialize Spark session with Iceberg support"""

    print("Initializing Spark session with Iceberg catalog...")

    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .getOrCreate()

    print(f"✓ Spark {spark.version} initialized")
    print(f"✓ Master: {spark.sparkContext.master}")

    return spark


def create_bronze_database(spark):
    """Create Bronze database if not exists"""

    print("\nCreating Bronze database...")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    # Verify creation
    databases = spark.sql("SHOW NAMESPACES IN iceberg").collect()
    database_names = [row.namespace for row in databases]

    if 'bronze' in database_names:
        print("✓ Bronze database created/verified")
    else:
        raise Exception("Failed to create Bronze database")


def create_bronze_table(spark):
    """Create Bronze metadata table if not exists"""

    print("\nCreating Bronze claims_raw table...")

    # Create table to track file metadata
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.bronze.claims_raw (
            file_path STRING COMMENT 'Full S3 path to CSV file',
            file_name STRING COMMENT 'Filename only',
            payer_name STRING COMMENT 'Insurance payer: BlueCross, Aetna, UnitedHealth',
            file_year INT COMMENT 'Year from partition (YYYY)',
            file_month INT COMMENT 'Month from partition (MM)',
            file_size_bytes BIGINT COMMENT 'File size in bytes',
            record_count BIGINT COMMENT 'Number of claims in file',
            schema_version STRING COMMENT 'Schema version (v1, v2, etc)',
            ingestion_timestamp TIMESTAMP COMMENT 'When file was registered in Bronze',
            processing_status STRING COMMENT 'registered, processing, completed, failed'
        ) USING iceberg
        PARTITIONED BY (payer_name, file_year, file_month)
        COMMENT 'Bronze layer: Tracks raw claims CSV files from S3'
    """)

    print("✓ Bronze claims_raw table created/verified")

    # Show table structure
    print("\nTable schema:")
    spark.sql("DESCRIBE TABLE iceberg.bronze.claims_raw").show(truncate=False)


def list_s3_files(spark, bucket, prefix):
    """
    List all CSV files in S3 bucket with given prefix

    Args:
        spark: SparkSession
        bucket: S3 bucket name (e.g., 'claims-raw')
        prefix: S3 prefix path (e.g., 'claims/')

    Returns:
        List of file paths
    """

    print(f"\nScanning S3: s3a://{bucket}/{prefix}")

    try:
        # Use Spark to list files (handles S3 via Hadoop filesystem)
        s3_path = f"s3a://{bucket}/{prefix}"

        # Read file paths using input_file_name()
        # First, try to read one file to get the pattern
        df = spark.read.option("header", "true").csv(s3_path)

        # Get unique file paths
        file_paths = df.select(F.input_file_name().alias("path")) \
            .distinct() \
            .collect()

        files = [row.path for row in file_paths]

        print(f"✓ Found {len(files)} CSV files")

        return files

    except Exception as e:
        print(f"✗ Error listing S3 files: {e}")
        return []


def extract_file_metadata(spark, file_path):
    """
    Extract metadata from a single CSV file

    Args:
        spark: SparkSession
        file_path: S3 path to CSV file

    Returns:
        Dictionary with file metadata
    """

    # Parse file path to extract payer, year, month
    # Example: s3a://claims-raw/claims/provider=BlueCross/year=2025/month=07/claims_20250702.csv

    path_parts = file_path.split('/')

    # Extract partition values
    payer_name = None
    file_year = None
    file_month = None
    file_name = path_parts[-1]

    for part in path_parts:
        if part.startswith('provider='):
            payer_name = part.split('=')[1]
        elif part.startswith('year='):
            file_year = int(part.split('=')[1])
        elif part.startswith('month='):
            file_month = int(part.split('=')[1])

    # Read file to get row count
    try:
        df = spark.read.option("header", "true").csv(file_path)
        record_count = df.count()
    except Exception as e:
        print(f"  ✗ Error reading {file_name}: {e}")
        record_count = 0

    # Get file size (approximation - Spark doesn't easily expose this)
    # For now, use a placeholder - in production would use boto3
    file_size_bytes = 0  #TODO: Get actual file size via boto3

    # Determine schema version (default to v1 for now)
    schema_version = "v1"

    metadata = {
        'file_path': file_path,
        'file_name': file_name,
        'payer_name': payer_name,
        'file_year': file_year,
        'file_month': file_month,
        'file_size_bytes': file_size_bytes,
        'record_count': record_count,
        'schema_version': schema_version,
        'ingestion_timestamp': datetime.now(),
        'processing_status': 'registered'
    }

    return metadata


def is_file_already_processed(spark, file_path):
    """
    Check if file already exists in Bronze table

    Args:
        spark: SparkSession
        file_path: S3 path to check

    Returns:
        Boolean - True if already processed
    """

    existing = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM iceberg.bronze.claims_raw
        WHERE file_path = '{file_path}'
    """).collect()[0]['cnt']

    return existing > 0


def register_file(spark, metadata):
    """
    Register file metadata in Bronze table

    Args:
        spark: SparkSession
        metadata: Dictionary with file metadata
    """

    # Create DataFrame from metadata
    schema = StructType([
        StructField("file_path", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("payer_name", StringType(), False),
        StructField("file_year", IntegerType(), False),
        StructField("file_month", IntegerType(), False),
        StructField("file_size_bytes", LongType(), True),
        StructField("record_count", LongType(), True),
        StructField("schema_version", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("processing_status", StringType(), False),
    ])

    df = spark.createDataFrame([metadata], schema=schema)

    # Write to Iceberg table (append mode)
    df.writeTo("iceberg.bronze.claims_raw").append()


def run_bronze_ingestion(spark, bucket="claims-raw", prefix="claims/"):
    """
    Main Bronze ingestion logic

    Args:
        spark: SparkSession
        bucket: S3 bucket name
        prefix: S3 prefix path
    """

    print("\n" + "=" * 70)
    print("BRONZE LAYER INGESTION - MedFlow Analytics")
    print("=" * 70)

    # Step 1: Create Bronze database and table
    create_bronze_database(spark)
    create_bronze_table(spark)

    # Step 2: List all files in S3
    files = list_s3_files(spark, bucket, prefix)

    if not files:
        print("\n✗ No files found in S3. Exiting.")
        return

    # Step 3: Process each file
    print(f"\nProcessing {len(files)} files...")
    print("-" * 70)

    files_registered = 0
    files_skipped = 0
    files_failed = 0

    for file_path in files:
        file_name = file_path.split('/')[-1]

        # Check if already processed
        if is_file_already_processed(spark, file_path):
            print(f"  ⊘ Skipping (already registered): {file_name}")
            files_skipped += 1
            continue

        # Extract metadata
        print(f"  → Processing: {file_name}")
        try:
            metadata = extract_file_metadata(spark, file_path)

            # Register in Bronze table
            register_file(spark, metadata)

            print(f"    ✓ Registered: {metadata['payer_name']} | "
                  f"{metadata['file_year']}-{metadata['file_month']:02d} | "
                  f"{metadata['record_count']:,} records")

            files_registered += 1

        except Exception as e:
            print(f"    ✗ Failed: {e}")
            files_failed += 1

    # Step 4: Summary
    print("\n" + "=" * 70)
    print("INGESTION SUMMARY")
    print("=" * 70)
    print(f"Total files found:     {len(files)}")
    print(f"Files registered:      {files_registered}")
    print(f"Files skipped:         {files_skipped} (already registered)")
    print(f"Files failed:          {files_failed}")
    print("=" * 70)

    # Step 5: Show Bronze table contents
    print("\nBronze table contents:")
    spark.sql("""
        SELECT
            payer_name,
            file_year,
            file_month,
            file_name,
            record_count,
            ingestion_timestamp
        FROM iceberg.bronze.claims_raw
        ORDER BY payer_name, file_year, file_month
    """).show(100, truncate=False)

    # Step 6: Statistics
    print("\nBronze statistics by payer:")
    spark.sql("""
        SELECT
            payer_name,
            COUNT(*) as file_count,
            SUM(record_count) as total_records,
            MIN(ingestion_timestamp) as first_ingestion,
            MAX(ingestion_timestamp) as last_ingestion
        FROM iceberg.bronze.claims_raw
        GROUP BY payer_name
        ORDER BY payer_name
    """).show(truncate=False)


def main():
    """Main entry point"""

    try:
        # Initialize Spark
        spark = create_spark_session()

        # Run Bronze ingestion
        run_bronze_ingestion(spark, bucket="claims-raw", prefix="claims/")

        print("\n✅ Bronze ingestion completed successfully!\n")

        # Stop Spark session
        spark.stop()

        return 0

    except Exception as e:
        print(f"\n❌ Bronze ingestion failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
