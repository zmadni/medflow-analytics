"""
Verify Gold Layer - MedFlow Analytics
Query the Gold tables to see business metrics and aggregations
"""

from pyspark.sql import SparkSession

def main():
    print("\n" + "="*70)
    print("GOLD LAYER VERIFICATION - MedFlow Analytics")
    print("="*70 + "\n")

    # Initialize Spark (catalog config comes from spark-submit)
    spark = SparkSession.builder.appName("GoldVerification").getOrCreate()

    # Check if Gold tables exist
    print("Checking Gold tables...")
    tables = spark.sql("SHOW TABLES IN iceberg.gold").collect()
    table_names = [row.tableName for row in tables]
    print(f"✓ Tables in gold namespace: {table_names}\n")

    # Verify expected tables exist
    expected_tables = ['claims_monthly_summary', 'claims_approval_funnel', 'provider_performance_metrics']
    for table in expected_tables:
        if table not in table_names:
            print(f"⚠ Missing table: {table}")
            return

    # ========================================================================
    # SECTION 1: Claims Monthly Summary
    # ========================================================================

    print("=" * 70)
    print("TABLE 1: CLAIMS_MONTHLY_SUMMARY")
    print("=" * 70 + "\n")

    total_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.gold.claims_monthly_summary").collect()[0]['cnt']
    print(f"✓ Total rows: {total_count}\n")

    if total_count > 0:
        # Show all monthly summary data
        print("Monthly summary by payer:")
        spark.sql("""
            SELECT
                year_month,
                payer_name,
                total_claims,
                unique_patients,
                unique_providers,
                ROUND(total_billed_amount, 2) as total_billed,
                ROUND(total_paid_amount, 2) as total_paid,
                approval_rate,
                denial_rate
            FROM iceberg.gold.claims_monthly_summary
            ORDER BY year_month, payer_name
        """).show(100, truncate=False)

        # Financial trends by month
        print("\nFinancial trends by month (all payers):")
        spark.sql("""
            SELECT
                year_month,
                SUM(total_claims) as total_claims,
                ROUND(SUM(total_billed_amount), 2) as total_billed,
                ROUND(SUM(total_paid_amount), 2) as total_paid,
                ROUND(AVG(approval_rate), 2) as avg_approval_rate
            FROM iceberg.gold.claims_monthly_summary
            GROUP BY year_month
            ORDER BY year_month
        """).show(truncate=False)

        # Payer comparison
        print("\nPayer comparison (all-time totals):")
        spark.sql("""
            SELECT
                payer_name,
                SUM(total_claims) as total_claims,
                ROUND(SUM(total_billed_amount), 2) as total_billed,
                ROUND(SUM(total_paid_amount), 2) as total_paid,
                ROUND(AVG(approval_rate), 2) as avg_approval_rate,
                ROUND(AVG(avg_reimbursement_rate), 2) as avg_reimbursement_rate
            FROM iceberg.gold.claims_monthly_summary
            GROUP BY payer_name
            ORDER BY total_claims DESC
        """).show(truncate=False)

        # Best and worst performing months
        print("\nBest performing month (by approval rate):")
        spark.sql("""
            SELECT year_month, payer_name, approval_rate, total_claims
            FROM iceberg.gold.claims_monthly_summary
            ORDER BY approval_rate DESC
            LIMIT 3
        """).show(truncate=False)

        print("\nWorst performing month (by approval rate):")
        spark.sql("""
            SELECT year_month, payer_name, approval_rate, total_claims
            FROM iceberg.gold.claims_monthly_summary
            ORDER BY approval_rate ASC
            LIMIT 3
        """).show(truncate=False)

    # ========================================================================
    # SECTION 2: Claims Approval Funnel
    # ========================================================================

    print("\n" + "=" * 70)
    print("TABLE 2: CLAIMS_APPROVAL_FUNNEL")
    print("=" * 70 + "\n")

    funnel_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.gold.claims_approval_funnel").collect()[0]['cnt']
    print(f"✓ Total rows: {funnel_count}\n")

    if funnel_count > 0:
        # Funnel by payer and type
        print("Approval funnel by payer and claim type:")
        spark.sql("""
            SELECT
                payer_name,
                claim_type,
                SUM(total_submitted) as total_submitted,
                SUM(approved_count) as approved,
                SUM(denied_count) as denied,
                SUM(pending_count) as pending,
                ROUND(AVG(approval_rate), 2) as avg_approval_rate,
                ROUND(AVG(denial_rate), 2) as avg_denial_rate
            FROM iceberg.gold.claims_approval_funnel
            GROUP BY payer_name, claim_type
            ORDER BY payer_name, claim_type
        """).show(truncate=False)

        # Inpatient vs Outpatient comparison
        print("\nInpatient vs Outpatient comparison:")
        spark.sql("""
            SELECT
                claim_type,
                SUM(total_submitted) as total_submitted,
                ROUND(AVG(approval_rate), 2) as avg_approval_rate,
                ROUND(AVG(avg_processing_days), 1) as avg_processing_days,
                ROUND(SUM(submitted_amount), 2) as total_submitted_amount,
                ROUND(SUM(approved_amount), 2) as total_approved_amount
            FROM iceberg.gold.claims_approval_funnel
            GROUP BY claim_type
        """).show(truncate=False)

        # Monthly funnel trends
        print("\nMonthly funnel trends (all payers, all types):")
        spark.sql("""
            SELECT
                year_month,
                SUM(total_submitted) as total_submitted,
                SUM(approved_count) as approved,
                SUM(denied_count) as denied,
                ROUND(AVG(approval_rate), 2) as avg_approval_rate
            FROM iceberg.gold.claims_approval_funnel
            GROUP BY year_month
            ORDER BY year_month
        """).show(truncate=False)

        # Processing efficiency
        print("\nProcessing efficiency by payer:")
        spark.sql("""
            SELECT
                payer_name,
                ROUND(AVG(avg_processing_days), 1) as avg_processing_days,
                ROUND(AVG(avg_reimbursement_rate_approved), 2) as avg_reimbursement_rate
            FROM iceberg.gold.claims_approval_funnel
            GROUP BY payer_name
            ORDER BY avg_processing_days
        """).show(truncate=False)

    # ========================================================================
    # SECTION 3: Provider Performance Metrics
    # ========================================================================

    print("\n" + "=" * 70)
    print("TABLE 3: PROVIDER_PERFORMANCE_METRICS")
    print("=" * 70 + "\n")

    provider_count = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.gold.provider_performance_metrics").collect()[0]['cnt']
    print(f"✓ Total providers: {provider_count}\n")

    if provider_count > 0:
        # Top providers by volume
        print("Top 20 providers by claim volume:")
        spark.sql("""
            SELECT
                provider_id,
                total_claims,
                unique_patients,
                months_active,
                ROUND(total_billed, 2) as total_billed,
                ROUND(total_paid, 2) as total_paid,
                approval_rate,
                primary_payer,
                payer_count
            FROM iceberg.gold.provider_performance_metrics
            ORDER BY total_claims DESC
            LIMIT 20
        """).show(truncate=False)

        # Provider performance distribution
        print("\nProvider performance distribution:")
        spark.sql("""
            SELECT
                COUNT(*) as provider_count,
                ROUND(AVG(total_claims), 1) as avg_claims_per_provider,
                ROUND(AVG(approval_rate), 2) as avg_approval_rate,
                ROUND(AVG(avg_claim_amount), 2) as avg_claim_amount
            FROM iceberg.gold.provider_performance_metrics
        """).show(truncate=False)

        # High performing providers (approval rate >= 90%)
        high_performers = spark.sql("""
            SELECT COUNT(*) as cnt
            FROM iceberg.gold.provider_performance_metrics
            WHERE approval_rate >= 90
        """).collect()[0]['cnt']

        print(f"\n✓ High performing providers (approval rate ≥ 90%): {high_performers}")

        # Low performing providers (approval rate < 80%)
        low_performers = spark.sql("""
            SELECT COUNT(*) as cnt
            FROM iceberg.gold.provider_performance_metrics
            WHERE approval_rate < 80
        """).collect()[0]['cnt']

        print(f"⚠ Low performing providers (approval rate < 80%): {low_performers}\n")

        # Provider payer mix
        print("Provider payer mix distribution:")
        spark.sql("""
            SELECT
                payer_count,
                COUNT(*) as provider_count
            FROM iceberg.gold.provider_performance_metrics
            GROUP BY payer_count
            ORDER BY payer_count
        """).show(truncate=False)

        # Inpatient specialists
        print("\nTop inpatient specialists (providers with >50% inpatient claims):")
        spark.sql("""
            SELECT
                provider_id,
                total_claims,
                inpatient_claims,
                outpatient_claims,
                ROUND(inpatient_claims * 100.0 / total_claims, 2) as inpatient_pct,
                approval_rate
            FROM iceberg.gold.provider_performance_metrics
            WHERE inpatient_claims * 100.0 / total_claims > 50
            ORDER BY inpatient_claims DESC
            LIMIT 10
        """).show(truncate=False)

        # Sample provider details with clinical patterns
        print("\nSample provider details (showing clinical patterns):")
        spark.sql("""
            SELECT
                provider_id,
                total_claims,
                approval_rate,
                primary_payer,
                top_5_diagnosis_codes,
                top_5_procedure_codes
            FROM iceberg.gold.provider_performance_metrics
            ORDER BY total_claims DESC
            LIMIT 5
        """).show(truncate=False)

    # ========================================================================
    # SECTION 4: Cross-Table Insights
    # ========================================================================

    print("\n" + "=" * 70)
    print("CROSS-TABLE INSIGHTS")
    print("=" * 70 + "\n")

    # Data freshness check
    print("Data freshness (last update timestamps):")
    spark.sql("""
        SELECT 'claims_monthly_summary' as table_name, MAX(last_updated_timestamp) as last_update
        FROM iceberg.gold.claims_monthly_summary
        UNION ALL
        SELECT 'claims_approval_funnel', MAX(last_updated_timestamp)
        FROM iceberg.gold.claims_approval_funnel
        UNION ALL
        SELECT 'provider_performance_metrics', MAX(last_updated_timestamp)
        FROM iceberg.gold.provider_performance_metrics
    """).show(truncate=False)

    # Overall summary
    print("\nOverall Gold layer summary:")
    total_providers_with_claims = spark.sql("SELECT COUNT(*) as cnt FROM iceberg.gold.provider_performance_metrics").collect()[0]['cnt']
    total_months_data = spark.sql("SELECT COUNT(DISTINCT year_month) as cnt FROM iceberg.gold.claims_monthly_summary").collect()[0]['cnt']
    total_payers = spark.sql("SELECT COUNT(DISTINCT payer_name) as cnt FROM iceberg.gold.claims_monthly_summary").collect()[0]['cnt']

    print(f"Total providers tracked:      {total_providers_with_claims}")
    print(f"Months of data:               {total_months_data}")
    print(f"Payers tracked:               {total_payers}")
    print(f"Total aggregated rows:        {total_count + funnel_count + provider_count}")

    print("\n" + "=" * 70)
    print("VERIFICATION COMPLETE")
    print("=" * 70 + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
