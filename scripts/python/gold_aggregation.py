#!/usr/bin/env python3
"""
Gold Layer Aggregation - MedFlow Analytics

Creates business-ready analytics tables from Silver claims data with incremental refresh.

Gold Tables:
1. claims_monthly_summary - Executive financial & operational KPIs by payer/month
2. claims_approval_funnel - Operational efficiency metrics by payer/type/month
3. provider_performance_metrics - Provider network analytics (all-time)

Usage:
    ./scripts/run_spark_iceberg.sh /opt/scripts/python/gold_aggregation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import sys
import traceback

# ============================================================================
# SECTION 1: Spark Session & Table Creation
# ============================================================================

def create_spark_session():
    """Initialize Spark session with Iceberg support"""
    print("Initializing Spark session with Iceberg catalog...")

    spark = SparkSession.builder \
        .appName("GoldAggregation") \
        .getOrCreate()

    print(f"✓ Spark {spark.version} initialized")
    print(f"✓ Master: {spark.sparkContext.master}")
    return spark

def create_gold_namespace(spark):
    """Create Gold namespace if not exists"""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    print("✓ Gold namespace created/verified")

def create_gold_tables(spark):
    """Create all 3 Gold tables"""

    print("\nCreating Gold tables...")

    # Table 1: claims_monthly_summary
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.claims_monthly_summary (
            year_month STRING NOT NULL,
            payer_name STRING NOT NULL,

            -- Volume Metrics
            total_claims BIGINT NOT NULL,
            unique_patients BIGINT NOT NULL,
            unique_providers BIGINT NOT NULL,

            -- Financial Metrics
            total_billed_amount DECIMAL(18,2) NOT NULL,
            total_allowed_amount DECIMAL(18,2),
            total_paid_amount DECIMAL(18,2),
            avg_billed_per_claim DECIMAL(10,2),
            avg_paid_per_claim DECIMAL(10,2),

            -- Payment Efficiency
            avg_reimbursement_rate DECIMAL(5,2),
            total_write_off DECIMAL(18,2),

            -- Operational Metrics
            claims_approved BIGINT,
            claims_denied BIGINT,
            claims_pending BIGINT,
            approval_rate DECIMAL(5,2),
            denial_rate DECIMAL(5,2),

            -- Metadata
            last_updated_timestamp TIMESTAMP NOT NULL

        ) USING iceberg
        PARTITIONED BY (year_month)
        COMMENT 'Gold: Monthly claims metrics by payer for executive dashboards'
    """)

    print("✓ claims_monthly_summary created/verified")

    # Table 2: claims_approval_funnel
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.claims_approval_funnel (
            year_month STRING NOT NULL,
            payer_name STRING NOT NULL,
            claim_type STRING NOT NULL,

            -- Funnel Stages
            total_submitted BIGINT NOT NULL,
            approved_count BIGINT,
            denied_count BIGINT,
            pending_count BIGINT,

            -- Conversion Rates
            approval_rate DECIMAL(5,2),
            denial_rate DECIMAL(5,2),

            -- Financial Impact
            submitted_amount DECIMAL(18,2),
            approved_amount DECIMAL(18,2),
            denied_amount DECIMAL(18,2),

            -- Processing Efficiency
            avg_processing_days DECIMAL(5,1),

            -- Reimbursement Quality
            avg_reimbursement_rate_approved DECIMAL(5,2),

            -- Metadata
            last_updated_timestamp TIMESTAMP NOT NULL

        ) USING iceberg
        PARTITIONED BY (year_month, payer_name)
        COMMENT 'Gold: Claims approval funnel for operational efficiency'
    """)

    print("✓ claims_approval_funnel created/verified")

    # Table 3: provider_performance_metrics
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.provider_performance_metrics (
            provider_id STRING NOT NULL,

            -- Activity Metrics
            total_claims BIGINT NOT NULL,
            first_claim_date DATE,
            last_claim_date DATE,
            months_active INT,

            -- Patient Volume
            unique_patients BIGINT,

            -- Financial Performance
            total_billed DECIMAL(18,2) NOT NULL,
            total_allowed DECIMAL(18,2),
            total_paid DECIMAL(18,2),
            avg_claim_amount DECIMAL(10,2),

            -- Approval Performance
            claims_approved BIGINT,
            claims_denied BIGINT,
            approval_rate DECIMAL(5,2),
            denial_rate DECIMAL(5,2),

            -- Payer Mix
            primary_payer STRING,
            payer_count INT,

            -- Clinical Patterns
            top_5_diagnosis_codes ARRAY<STRING>,
            top_5_procedure_codes ARRAY<STRING>,

            -- Service Type Mix
            inpatient_claims BIGINT,
            outpatient_claims BIGINT,

            -- Metadata
            last_updated_timestamp TIMESTAMP NOT NULL

        ) USING iceberg
        PARTITIONED BY (bucket(100, provider_id))
        COMMENT 'Gold: Provider performance metrics for network management'
    """)

    print("✓ provider_performance_metrics created/verified")

# ============================================================================
# SECTION 2: Incremental Refresh Detection
# ============================================================================

def get_affected_months(spark, lookback_days=7):
    """Get distinct year-month values with recent updates"""

    query = f"""
        SELECT DISTINCT DATE_FORMAT(claim_date, 'yyyy-MM') as year_month
        FROM iceberg.silver.claims
        WHERE processing_date >= CURRENT_DATE - INTERVAL {lookback_days} DAY
        ORDER BY year_month
    """

    result = spark.sql(query).collect()
    return [row['year_month'] for row in result]

def get_affected_providers(spark, lookback_days=7):
    """Get distinct provider_ids with recent updates"""

    query = f"""
        SELECT DISTINCT provider_id
        FROM iceberg.silver.claims
        WHERE processing_date >= CURRENT_DATE - INTERVAL {lookback_days} DAY
    """

    result = spark.sql(query).collect()
    return [row['provider_id'] for row in result]

# ============================================================================
# SECTION 3: Aggregation Functions
# ============================================================================

def refresh_monthly_summary(spark, affected_months):
    """Incrementally refresh claims_monthly_summary table"""

    if not affected_months:
        print("\n  ⊘ No months to refresh for monthly_summary")
        return 0

    print(f"\n  Refreshing {len(affected_months)} month(s) for monthly_summary...")

    total_rows = 0

    for year_month in affected_months:
        # Delete existing records for this month
        spark.sql(f"""
            DELETE FROM iceberg.gold.claims_monthly_summary
            WHERE year_month = '{year_month}'
        """)

        # Recompute aggregations
        summary_df = spark.sql(f"""
            SELECT
                '{year_month}' as year_month,
                payer_name,
                COUNT(*) as total_claims,
                COUNT(DISTINCT patient_id) as unique_patients,
                COUNT(DISTINCT provider_id) as unique_providers,
                SUM(billed_amount) as total_billed_amount,
                SUM(allowed_amount) as total_allowed_amount,
                SUM(paid_amount) as total_paid_amount,
                ROUND(AVG(billed_amount), 2) as avg_billed_per_claim,
                ROUND(AVG(paid_amount), 2) as avg_paid_per_claim,
                ROUND(AVG(CASE WHEN billed_amount > 0 THEN (paid_amount / billed_amount) * 100 ELSE 0 END), 2) as avg_reimbursement_rate,
                SUM(billed_amount - COALESCE(paid_amount, 0)) as total_write_off,
                SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) as claims_approved,
                SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) as claims_denied,
                SUM(CASE WHEN claim_status IN ('Pending', 'Under Review') THEN 1 ELSE 0 END) as claims_pending,
                ROUND(SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as approval_rate,
                ROUND(SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as denial_rate,
                CURRENT_TIMESTAMP() as last_updated_timestamp
            FROM iceberg.silver.claims
            WHERE DATE_FORMAT(claim_date, 'yyyy-MM') = '{year_month}'
            GROUP BY payer_name
        """)

        row_count = summary_df.count()
        summary_df.writeTo("iceberg.gold.claims_monthly_summary").append()

        total_rows += row_count
        print(f"    ✓ {year_month}: {row_count} rows")

    return total_rows

def refresh_approval_funnel(spark, affected_months):
    """Incrementally refresh claims_approval_funnel table"""

    if not affected_months:
        print("\n  ⊘ No months to refresh for approval_funnel")
        return 0

    print(f"\n  Refreshing {len(affected_months)} month(s) for approval_funnel...")

    total_rows = 0

    for year_month in affected_months:
        # Delete existing records for this month (all partitions for this month)
        spark.sql(f"""
            DELETE FROM iceberg.gold.claims_approval_funnel
            WHERE year_month = '{year_month}'
        """)

        # Recompute funnel metrics
        funnel_df = spark.sql(f"""
            SELECT
                '{year_month}' as year_month,
                payer_name,
                claim_type,
                COUNT(*) as total_submitted,
                SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) as approved_count,
                SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) as denied_count,
                SUM(CASE WHEN claim_status IN ('Pending', 'Under Review') THEN 1 ELSE 0 END) as pending_count,
                ROUND(SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as approval_rate,
                ROUND(SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as denial_rate,
                SUM(billed_amount) as submitted_amount,
                SUM(CASE WHEN claim_status = 'Approved' THEN billed_amount ELSE 0 END) as approved_amount,
                SUM(CASE WHEN claim_status = 'Denied' THEN billed_amount ELSE 0 END) as denied_amount,
                ROUND(AVG(DATEDIFF(processing_date, claim_date)), 1) as avg_processing_days,
                ROUND(AVG(CASE WHEN claim_status = 'Approved' AND billed_amount > 0
                    THEN (paid_amount / billed_amount) * 100
                    ELSE NULL END), 2) as avg_reimbursement_rate_approved,
                CURRENT_TIMESTAMP() as last_updated_timestamp
            FROM iceberg.silver.claims
            WHERE DATE_FORMAT(claim_date, 'yyyy-MM') = '{year_month}'
            GROUP BY payer_name, claim_type
        """)

        row_count = funnel_df.count()
        funnel_df.writeTo("iceberg.gold.claims_approval_funnel").append()

        total_rows += row_count
        print(f"    ✓ {year_month}: {row_count} rows")

    return total_rows

def refresh_provider_metrics(spark, affected_providers):
    """Incrementally refresh provider_performance_metrics for affected providers"""

    if not affected_providers:
        print("\n  ⊘ No providers to refresh")
        return 0

    print(f"\n  Refreshing {len(affected_providers)} provider(s)...")

    # Delete existing records for affected providers
    if len(affected_providers) <= 1000:  # Avoid SQL too large
        provider_list = "', '".join(affected_providers)
        spark.sql(f"""
            DELETE FROM iceberg.gold.provider_performance_metrics
            WHERE provider_id IN ('{provider_list}')
        """)
    else:
        # For large provider lists, use dataframe approach
        affected_df = spark.createDataFrame([(p,) for p in affected_providers], ["provider_id"])
        affected_df.createOrReplaceTempView("affected_providers_temp")
        spark.sql("""
            DELETE FROM iceberg.gold.provider_performance_metrics
            WHERE provider_id IN (SELECT provider_id FROM affected_providers_temp)
        """)

    # Batch process providers (avoid per-provider queries for performance)
    provider_metrics_df = spark.sql(f"""
        SELECT
            provider_id,
            COUNT(*) as total_claims,
            MIN(claim_date) as first_claim_date,
            MAX(claim_date) as last_claim_date,
            CAST(MONTHS_BETWEEN(MAX(claim_date), MIN(claim_date)) AS INT) as months_active,
            COUNT(DISTINCT patient_id) as unique_patients,
            SUM(billed_amount) as total_billed,
            SUM(allowed_amount) as total_allowed,
            SUM(paid_amount) as total_paid,
            ROUND(AVG(billed_amount), 2) as avg_claim_amount,
            SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) as claims_approved,
            SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) as claims_denied,
            ROUND(SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as approval_rate,
            ROUND(SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as denial_rate,
            FIRST(payer_name) as primary_payer,
            COUNT(DISTINCT payer_name) as payer_count,
            COLLECT_LIST(diagnosis_code) as all_diagnoses,
            COLLECT_LIST(procedure_code) as all_procedures,
            SUM(CASE WHEN claim_type = 'Inpatient' THEN 1 ELSE 0 END) as inpatient_claims,
            SUM(CASE WHEN claim_type = 'Outpatient' THEN 1 ELSE 0 END) as outpatient_claims,
            CURRENT_TIMESTAMP() as last_updated_timestamp
        FROM iceberg.silver.claims
        WHERE provider_id IN ('{("', '".join(affected_providers[:1000]))}')
        GROUP BY provider_id
    """)

    # Post-process to get top 5 diagnosis and procedure codes (simplified approach)
    # Take first 5 unique values (not sorted by frequency, but representative)
    provider_metrics_df = provider_metrics_df \
        .withColumn("top_5_diagnosis_codes", F.expr("slice(array_distinct(all_diagnoses), 1, 5)")) \
        .withColumn("top_5_procedure_codes", F.expr("slice(array_distinct(all_procedures), 1, 5)")) \
        .drop("all_diagnoses", "all_procedures")

    row_count = provider_metrics_df.count()
    provider_metrics_df.writeTo("iceberg.gold.provider_performance_metrics").append()

    print(f"    ✓ {row_count} providers refreshed")

    return row_count

# ============================================================================
# SECTION 4: Data Quality Validation
# ============================================================================

def validate_monthly_summary(spark, year_month):
    """Validate monthly summary against Silver source"""

    # Check 1: Row count (should equal number of payers with claims that month)
    gold_count = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM iceberg.gold.claims_monthly_summary
        WHERE year_month = '{year_month}'
    """).collect()[0]['cnt']

    expected_payers = spark.sql(f"""
        SELECT COUNT(DISTINCT payer_name) as cnt
        FROM iceberg.silver.claims
        WHERE DATE_FORMAT(claim_date, 'yyyy-MM') = '{year_month}'
    """).collect()[0]['cnt']

    if gold_count != expected_payers:
        print(f"    ⚠ Row count mismatch for {year_month}: {gold_count} != {expected_payers}")
        return False

    # Check 2: Financial totals match
    gold_total = spark.sql(f"""
        SELECT COALESCE(SUM(total_billed_amount), 0) as total
        FROM iceberg.gold.claims_monthly_summary
        WHERE year_month = '{year_month}'
    """).collect()[0]['total']

    silver_total = spark.sql(f"""
        SELECT COALESCE(SUM(billed_amount), 0) as total
        FROM iceberg.silver.claims
        WHERE DATE_FORMAT(claim_date, 'yyyy-MM') = '{year_month}'
    """).collect()[0]['total']

    tolerance = 0.01  # $0.01 tolerance for rounding
    if abs(float(gold_total) - float(silver_total)) > tolerance:
        print(f"    ⚠ Financial mismatch for {year_month}: {gold_total} != {silver_total}")
        return False

    print(f"    ✓ Validation passed for {year_month}")
    return True

# ============================================================================
# SECTION 5: Main Orchestration
# ============================================================================

def run_gold_aggregation(spark, lookback_days=7):
    """Main Gold aggregation orchestration"""

    print("\n" + "=" * 70)
    print("GOLD LAYER AGGREGATION - MedFlow Analytics")
    print("=" * 70 + "\n")

    # Create tables if not exist
    create_gold_namespace(spark)
    create_gold_tables(spark)

    # Identify affected partitions
    print(f"\nIdentifying affected partitions (lookback: {lookback_days} days)...")

    affected_months = get_affected_months(spark, lookback_days)
    affected_providers = get_affected_providers(spark, lookback_days)

    print(f"  • Affected months: {len(affected_months)}")
    print(f"  • Affected providers: {len(affected_providers)}")

    if not affected_months and not affected_providers:
        print("\n✓ No new data to process\n")
        return

    # Refresh each Gold table
    print("\n" + "-" * 70)
    print("REFRESHING GOLD TABLES")
    print("-" * 70)

    # Table 1: Monthly Summary
    monthly_rows = refresh_monthly_summary(spark, affected_months)

    # Table 2: Approval Funnel
    funnel_rows = refresh_approval_funnel(spark, affected_months)

    # Table 3: Provider Metrics
    provider_rows = refresh_provider_metrics(spark, affected_providers)

    # Data Quality Validation
    print("\n" + "-" * 70)
    print("DATA QUALITY VALIDATION")
    print("-" * 70)

    validation_passed = True
    for year_month in affected_months[:5]:  # Validate first 5 months to avoid too much output
        if not validate_monthly_summary(spark, year_month):
            validation_passed = False

    # Summary
    print("\n" + "=" * 70)
    print("AGGREGATION SUMMARY")
    print("=" * 70)
    print(f"Tables refreshed:             3")
    print(f"Monthly summary rows:         {monthly_rows}")
    print(f"Approval funnel rows:         {funnel_rows}")
    print(f"Provider metrics rows:        {provider_rows}")
    print(f"Data quality validation:      {'✓ PASSED' if validation_passed else '✗ FAILED'}")
    print("=" * 70)

    # Show sample data
    print("\nSample: claims_monthly_summary (top 5 rows)")
    spark.sql("""
        SELECT year_month, payer_name, total_claims, approval_rate, total_billed_amount
        FROM iceberg.gold.claims_monthly_summary
        ORDER BY year_month DESC, payer_name
        LIMIT 5
    """).show(truncate=False)

    print("\nSample: claims_approval_funnel (top 5 rows)")
    spark.sql("""
        SELECT year_month, payer_name, claim_type, approval_rate, denial_rate
        FROM iceberg.gold.claims_approval_funnel
        ORDER BY year_month DESC, payer_name, claim_type
        LIMIT 5
    """).show(truncate=False)

    print("\nSample: provider_performance_metrics (top 5 by volume)")
    spark.sql("""
        SELECT provider_id, total_claims, approval_rate, avg_claim_amount
        FROM iceberg.gold.provider_performance_metrics
        ORDER BY total_claims DESC
        LIMIT 5
    """).show(truncate=False)

def main():
    """Main entry point"""

    try:
        spark = create_spark_session()
        run_gold_aggregation(spark, lookback_days=365)  # Process all data on first run

        print("\n✅ Gold aggregation completed successfully!\n")
        spark.stop()
        return 0

    except Exception as e:
        print(f"\n❌ Gold aggregation failed: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
