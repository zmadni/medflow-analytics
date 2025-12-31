"""
Verify Silver Layer - MedFlow Analytics
Query the Silver tables to see transformed claims and data quality metrics
"""

from pyspark.sql import SparkSession

def main():
    print("\n" + "="*70)
    print("SILVER LAYER VERIFICATION - MedFlow Analytics")
    print("="*70 + "\n")

    # Initialize Spark (catalog config comes from spark-submit)
    spark = SparkSession.builder.appName("SilverVerification").getOrCreate()

    # Check if Silver tables exist
    print("Checking Silver tables...")
    tables = spark.sql("SHOW TABLES IN iceberg.silver").collect()
    table_names = [row.tableName for row in tables]
    print(f"✓ Tables in silver namespace: {table_names}\n")

    # Verify expected tables exist
    expected_tables = ['claims', 'claims_quarantine', 'processing_log']
    for table in expected_tables:
        if table not in table_names:
            print(f"⚠ Missing table: {table}")
            return

    # ========================================================================
    # SECTION 1: Silver Claims Table Statistics
    # ========================================================================

    print("=" * 70)
    print("SILVER CLAIMS TABLE STATISTICS")
    print("=" * 70 + "\n")

    # Count total claims
    total_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.silver.claims").collect()[0]['cnt']
    print(f"✓ Total claims in Silver: {total_count:,}\n")

    if total_count > 0:
        # Show breakdown by payer
        print("Claims by payer:")
        spark.sql("""
            SELECT
                payer_name,
                COUNT(*) as claim_count,
                MIN(claim_date) as earliest_claim,
                MAX(claim_date) as latest_claim
            FROM iceberg.silver.claims
            GROUP BY payer_name
            ORDER BY payer_name
        """).show(truncate=False)

        # Show breakdown by claim status
        print("\nClaims by status:")
        spark.sql("""
            SELECT
                claim_status,
                COUNT(*) as claim_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM iceberg.silver.claims
            GROUP BY claim_status
            ORDER BY claim_count DESC
        """).show(truncate=False)

        # Show breakdown by claim type
        print("\nClaims by type:")
        spark.sql("""
            SELECT
                claim_type,
                COUNT(*) as claim_count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM iceberg.silver.claims
            GROUP BY claim_type
            ORDER BY claim_count DESC
        """).show(truncate=False)

        # Financial summary by payer
        print("\nFinancial summary by payer:")
        spark.sql("""
            SELECT
                payer_name,
                COUNT(*) as claim_count,
                ROUND(SUM(billed_amount), 2) as total_billed,
                ROUND(SUM(allowed_amount), 2) as total_allowed,
                ROUND(SUM(paid_amount), 2) as total_paid,
                ROUND(AVG(billed_amount), 2) as avg_billed
            FROM iceberg.silver.claims
            GROUP BY payer_name
            ORDER BY payer_name
        """).show(truncate=False)

        # Monthly claim volume
        print("\nMonthly claim volume:")
        spark.sql("""
            SELECT
                YEAR(claim_date) as year,
                MONTH(claim_date) as month,
                COUNT(*) as claim_count,
                COUNT(DISTINCT payer_name) as payer_count
            FROM iceberg.silver.claims
            GROUP BY YEAR(claim_date), MONTH(claim_date)
            ORDER BY year, month
        """).show(truncate=False)

        # Top diagnosis codes
        print("\nTop 10 diagnosis codes:")
        spark.sql("""
            SELECT
                diagnosis_code,
                COUNT(*) as claim_count
            FROM iceberg.silver.claims
            GROUP BY diagnosis_code
            ORDER BY claim_count DESC
            LIMIT 10
        """).show(truncate=False)

        # Top procedure codes
        print("\nTop 10 procedure codes:")
        spark.sql("""
            SELECT
                procedure_code,
                COUNT(*) as claim_count,
                ROUND(AVG(billed_amount), 2) as avg_billed
            FROM iceberg.silver.claims
            GROUP BY procedure_code
            ORDER BY claim_count DESC
            LIMIT 10
        """).show(truncate=False)

    else:
        print("⚠ No claims in Silver table yet")

    # ========================================================================
    # SECTION 2: Quarantine Table Statistics
    # ========================================================================

    print("\n" + "=" * 70)
    print("QUARANTINE TABLE STATISTICS")
    print("=" * 70 + "\n")

    quarantine_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.silver.claims_quarantine").collect()[0]['cnt']
    print(f"✓ Total quarantined records: {quarantine_count:,}\n")

    if quarantine_count > 0:
        # Quarantine breakdown by payer
        print("Quarantined records by payer:")
        spark.sql("""
            SELECT
                payer_name,
                COUNT(*) as quarantine_count
            FROM iceberg.silver.claims_quarantine
            GROUP BY payer_name
            ORDER BY payer_name
        """).show(truncate=False)

        # Top rejection reasons
        print("\nTop rejection reasons:")
        spark.sql("""
            SELECT
                explode(rejection_reasons) as rejection_reason,
                COUNT(*) as occurrence_count
            FROM iceberg.silver.claims_quarantine
            GROUP BY rejection_reason
            ORDER BY occurrence_count DESC
            LIMIT 15
        """).show(truncate=False)

        # Rejection reasons by payer
        print("\nRejection patterns by payer:")
        spark.sql("""
            SELECT
                payer_name,
                explode(rejection_reasons) as rejection_reason,
                COUNT(*) as count
            FROM iceberg.silver.claims_quarantine
            GROUP BY payer_name, rejection_reason
            ORDER BY payer_name, count DESC
        """).show(truncate=False)

        # Sample of quarantined records
        print("\nSample quarantined records (first 5):")
        spark.sql("""
            SELECT
                claim_id,
                payer_name,
                rejection_reasons
            FROM iceberg.silver.claims_quarantine
            LIMIT 5
        """).show(truncate=False)

    else:
        print("✓ No quarantined records (100% acceptance rate!)")

    # ========================================================================
    # SECTION 3: Processing Log Statistics
    # ========================================================================

    print("\n" + "=" * 70)
    print("PROCESSING LOG STATISTICS")
    print("=" * 70 + "\n")

    log_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.silver.processing_log").collect()[0]['cnt']
    print(f"✓ Total processing runs: {log_count}\n")

    if log_count > 0:
        # Processing summary
        print("Processing summary by status:")
        spark.sql("""
            SELECT
                status,
                COUNT(*) as run_count
            FROM iceberg.silver.processing_log
            GROUP BY status
            ORDER BY status
        """).show(truncate=False)

        # Processing metrics by payer
        print("\nProcessing metrics by payer:")
        spark.sql("""
            SELECT
                payer_name,
                COUNT(*) as files_processed,
                SUM(records_read) as total_read,
                SUM(records_accepted) as total_accepted,
                SUM(records_quarantined) as total_quarantined,
                ROUND(SUM(records_accepted) * 100.0 / NULLIF(SUM(records_read), 0), 2) as acceptance_rate_pct
            FROM iceberg.silver.processing_log
            WHERE status = 'completed'
            GROUP BY payer_name
            ORDER BY payer_name
        """).show(truncate=False)

        # Recent processing runs
        print("\nRecent processing runs (last 10):")
        spark.sql("""
            SELECT
                processing_id,
                payer_name,
                status,
                records_read,
                records_accepted,
                records_quarantined,
                processing_start_timestamp
            FROM iceberg.silver.processing_log
            ORDER BY processing_start_timestamp DESC
            LIMIT 10
        """).show(truncate=False)

        # Failed runs (if any)
        failed_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.silver.processing_log WHERE status = 'failed'").collect()[0]['cnt']
        if failed_count > 0:
            print(f"\n⚠ Found {failed_count} failed processing runs:")
            spark.sql("""
                SELECT
                    processing_id,
                    payer_name,
                    bronze_file_path,
                    error_message,
                    processing_start_timestamp
                FROM iceberg.silver.processing_log
                WHERE status = 'failed'
                ORDER BY processing_start_timestamp DESC
            """).show(truncate=False)

    # ========================================================================
    # SECTION 4: Overall Data Quality Summary
    # ========================================================================

    print("\n" + "=" * 70)
    print("OVERALL DATA QUALITY SUMMARY")
    print("=" * 70 + "\n")

    total_processed = total_count + quarantine_count

    if total_processed > 0:
        acceptance_rate = (total_count / total_processed) * 100
        quarantine_rate = (quarantine_count / total_processed) * 100

        print(f"Total records processed:   {total_processed:,}")
        print(f"Records accepted (Silver): {total_count:,} ({acceptance_rate:.2f}%)")
        print(f"Records quarantined:       {quarantine_count:,} ({quarantine_rate:.2f}%)")
        print()

        if acceptance_rate >= 95:
            print("✅ Excellent data quality (≥95% acceptance)")
        elif acceptance_rate >= 90:
            print("✓ Good data quality (≥90% acceptance)")
        elif acceptance_rate >= 80:
            print("⚠ Fair data quality (≥80% acceptance)")
        else:
            print("❌ Poor data quality (<80% acceptance)")

    # ========================================================================
    # SECTION 5: Partition Information
    # ========================================================================

    print("\n" + "=" * 70)
    print("PARTITION INFORMATION")
    print("=" * 70 + "\n")

    if total_count > 0:
        print("Silver claims partitions (by month):")
        spark.sql("""
            SELECT
                YEAR(claim_date) as year,
                MONTH(claim_date) as month,
                COUNT(*) as record_count,
                COUNT(DISTINCT payer_name) as payers
            FROM iceberg.silver.claims
            GROUP BY YEAR(claim_date), MONTH(claim_date)
            ORDER BY year, month
        """).show(truncate=False)

    print("\n" + "=" * 70)
    print("VERIFICATION COMPLETE")
    print("=" * 70 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
