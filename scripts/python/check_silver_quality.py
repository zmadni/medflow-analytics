"""
Check Silver Layer Data Quality - MedFlow Analytics
Validates that quarantine rate is below threshold
"""

from pyspark.sql import SparkSession
import sys

def main():
    # Initialize Spark
    spark = SparkSession.builder.appName("SilverQualityCheck").getOrCreate()

    # Count records in each table
    claims_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.silver.claims").collect()[0]['cnt']
    quarantine_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.silver.claims_quarantine").collect()[0]['cnt']

    total = claims_count + quarantine_count

    if total == 0:
        print("⚠ No records found in Silver layer")
        spark.stop()
        sys.exit(0)

    quarantine_rate = (quarantine_count / total) * 100

    print(f"\n{'='*70}")
    print(f"SILVER LAYER DATA QUALITY CHECK")
    print(f"{'='*70}")
    print(f"Accepted:        {claims_count:,}")
    print(f"Quarantined:     {quarantine_count:,}")
    print(f"Total:           {total:,}")
    print(f"Quarantine Rate: {quarantine_rate:.2f}%")
    print(f"{'='*70}\n")

    # Threshold check
    THRESHOLD = 10.0
    if quarantine_rate > THRESHOLD:
        print(f"❌ FAILED: Quarantine rate {quarantine_rate:.2f}% exceeds threshold {THRESHOLD}%")
        spark.stop()
        sys.exit(1)

    print(f"✅ PASSED: Data quality check successful (quarantine rate: {quarantine_rate:.2f}%)")
    spark.stop()
    sys.exit(0)

if __name__ == "__main__":
    main()
