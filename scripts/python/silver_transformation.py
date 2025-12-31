#!/usr/bin/env python3
"""
Silver Layer Transformation - MedFlow Analytics

Transforms Bronze raw claims data to Silver standardized schema with data quality validation.

Purpose:
- Read from Bronze registry (iceberg.bronze.claims_raw)
- Apply payer-specific schema mappings (BlueCross, Aetna, UnitedHealth)
- Validate data quality (required fields, dates, amounts, codes)
- Write clean records to iceberg.silver.claims
- Quarantine invalid records to iceberg.silver.claims_quarantine
- Track processing state for idempotency

Usage:
    ./scripts/run_spark_iceberg.sh /opt/scripts/python/silver_transformation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import sys
import traceback
import uuid

# ============================================================================
# SECTION 1: Spark Session & Table Creation
# ============================================================================

def create_spark_session():
    """Initialize Spark session with Iceberg support"""
    print("Initializing Spark session with Iceberg catalog...")

    spark = SparkSession.builder \
        .appName("SilverTransformation") \
        .getOrCreate()

    print(f"✓ Spark {spark.version} initialized")
    print(f"✓ Master: {spark.sparkContext.master}")
    return spark

def create_silver_namespace(spark):
    """Create Silver namespace if not exists"""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    print("✓ Silver namespace created/verified")

def create_silver_tables(spark):
    """Create Silver claims, quarantine, and processing_log tables"""

    print("\nCreating Silver tables...")

    # Create main Silver claims table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.claims (
            claim_id STRING NOT NULL,
            patient_id STRING NOT NULL,
            provider_id STRING NOT NULL,
            claim_date DATE NOT NULL,
            service_from_date DATE NOT NULL,
            service_to_date DATE,
            admission_date DATE,
            discharge_date DATE,
            diagnosis_code STRING NOT NULL,
            procedure_code STRING NOT NULL,
            billed_amount DECIMAL(10,2) NOT NULL,
            allowed_amount DECIMAL(10,2),
            paid_amount DECIMAL(10,2),
            claim_status STRING NOT NULL,
            claim_type STRING NOT NULL,
            payer_name STRING NOT NULL,
            source_file_path STRING NOT NULL,
            ingestion_timestamp TIMESTAMP NOT NULL,
            processing_date DATE NOT NULL
        ) USING iceberg
        PARTITIONED BY (months(claim_date))
        COMMENT 'Silver layer: Standardized claims from all payers'
    """)

    print("✓ Silver claims table created/verified")

    # Create quarantine table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.claims_quarantine (
            claim_id STRING,
            patient_id STRING,
            provider_id STRING,
            claim_date STRING,
            service_from_date STRING,
            service_to_date STRING,
            admission_date STRING,
            discharge_date STRING,
            diagnosis_code STRING,
            procedure_code STRING,
            billed_amount STRING,
            allowed_amount STRING,
            paid_amount STRING,
            claim_status STRING,
            claim_type STRING,
            payer_name STRING NOT NULL,
            source_file_path STRING NOT NULL,
            rejection_timestamp TIMESTAMP NOT NULL,
            rejection_reasons ARRAY<STRING> NOT NULL,
            validation_details STRING,
            raw_record STRING
        ) USING iceberg
        PARTITIONED BY (payer_name, days(rejection_timestamp))
        COMMENT 'Quarantine: Invalid records from Silver transformation'
    """)

    print("✓ Silver quarantine table created/verified")

    # Create processing log table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.processing_log (
            processing_id STRING NOT NULL,
            bronze_file_path STRING NOT NULL,
            payer_name STRING NOT NULL,
            processing_start_timestamp TIMESTAMP NOT NULL,
            processing_end_timestamp TIMESTAMP,
            status STRING NOT NULL,
            records_read BIGINT,
            records_accepted BIGINT,
            records_quarantined BIGINT,
            error_message STRING
        ) USING iceberg
        PARTITIONED BY (payer_name, days(processing_start_timestamp))
    """)

    print("✓ Silver processing_log table created/verified")

# ============================================================================
# SECTION 2: Payer Schema Transformation Functions
# ============================================================================

def parse_currency(col):
    """Remove $ and , from currency strings, convert to DECIMAL(10,2)"""
    cleaned = F.regexp_replace(col, r'[\$,]', '')
    return cleaned.cast(DecimalType(10, 2))

def standardize_status(col):
    """Standardize claim status values to canonical forms"""
    return F.when(F.upper(col).isin(['APPROVED', 'PAID', 'PROCESSED']), 'Approved') \
            .when(F.upper(col).isin(['DENIED', 'REJECTED']), 'Denied') \
            .when(F.upper(col).isin(['PENDING', 'PROCESSING']), 'Pending') \
            .otherwise('Under Review')

def parse_bluecross_schema(df):
    """Transform BlueCross CSV to canonical schema"""
    return df.select(
        F.col("ClaimID").alias("claim_id"),
        F.col("MemberID").alias("patient_id"),
        F.col("ProviderNPI").alias("provider_id"),
        F.to_date(F.col("ClaimDate"), "yyyy-MM-dd").alias("claim_date"),
        F.to_date(F.col("ServiceFromDate"), "yyyy-MM-dd").alias("service_from_date"),
        F.to_date(F.col("ServiceToDate"), "yyyy-MM-dd").alias("service_to_date"),
        F.lit(None).cast(DateType()).alias("admission_date"),
        F.lit(None).cast(DateType()).alias("discharge_date"),
        F.upper(F.trim(F.col("DiagnosisCode"))).alias("diagnosis_code"),
        F.trim(F.col("ProcedureCode")).alias("procedure_code"),
        F.col("BilledAmount").cast(DecimalType(10,2)).alias("billed_amount"),
        F.col("AllowedAmount").cast(DecimalType(10,2)).alias("allowed_amount"),
        F.col("PaidAmount").cast(DecimalType(10,2)).alias("paid_amount"),
        standardize_status(F.col("Status")).alias("claim_status"),
        F.col("Type").alias("claim_type"),
        F.lit("BlueCross").alias("payer_name")
    )

def parse_aetna_schema(df):
    """Transform Aetna CSV to canonical schema"""
    return df.select(
        F.col("claim_number").alias("claim_id"),
        F.col("patient_id").alias("patient_id"),
        F.col("provider_tax_id").alias("provider_id"),
        F.to_date(F.col("claim_received_date"), "MM/dd/yyyy").alias("claim_date"),
        F.to_date(F.col("service_date"), "MM/dd/yyyy").alias("service_from_date"),
        F.to_date(F.col("service_date"), "MM/dd/yyyy").alias("service_to_date"),
        F.to_date(F.col("admit_date"), "MM/dd/yyyy").alias("admission_date"),
        F.to_date(F.col("discharge_date"), "MM/dd/yyyy").alias("discharge_date"),
        F.upper(F.trim(F.col("primary_diagnosis"))).alias("diagnosis_code"),
        F.trim(F.col("procedure")).alias("procedure_code"),
        parse_currency(F.col("charge_amount")).alias("billed_amount"),
        parse_currency(F.col("approved_amount")).alias("allowed_amount"),
        parse_currency(F.col("payment_amount")).alias("paid_amount"),
        standardize_status(F.col("claim_status")).alias("claim_status"),
        F.col("service_category").alias("claim_type"),
        F.lit("Aetna").alias("payer_name")
    )

def parse_unitedhealth_schema(df):
    """Transform UnitedHealth CSV to canonical schema"""
    return df.select(
        F.col("CLAIM_ID").alias("claim_id"),
        F.col("SUBSCRIBER_ID").alias("patient_id"),
        F.col("RENDERING_PROVIDER_NPI").alias("provider_id"),
        F.to_date(F.col("RECEIVED_DT"), "yyyyMMdd").alias("claim_date"),
        F.to_date(F.col("SVC_FROM_DT"), "yyyyMMdd").alias("service_from_date"),
        F.to_date(F.col("SVC_TO_DT"), "yyyyMMdd").alias("service_to_date"),
        F.lit(None).cast(DateType()).alias("admission_date"),
        F.lit(None).cast(DateType()).alias("discharge_date"),
        F.upper(F.trim(F.col("DIAG_CD"))).alias("diagnosis_code"),
        F.trim(F.col("PROC_CD")).alias("procedure_code"),
        F.col("BILLED_AMT").cast(DecimalType(10,2)).alias("billed_amount"),
        F.col("ALLOWED_AMT").cast(DecimalType(10,2)).alias("allowed_amount"),
        F.col("PAID_AMT").cast(DecimalType(10,2)).alias("paid_amount"),
        standardize_status(F.col("CLM_STATUS")).alias("claim_status"),
        F.col("CLM_TYPE").alias("claim_type"),
        F.lit("UnitedHealth").alias("payer_name")
    )

# ============================================================================
# SECTION 3: Data Quality Validation Functions
# ============================================================================

def validate_required_fields(df):
    """Add validation column for required field nulls"""

    required_fields = [
        'claim_id', 'patient_id', 'provider_id',
        'claim_date', 'service_from_date',
        'diagnosis_code', 'procedure_code',
        'billed_amount', 'claim_status', 'claim_type'
    ]

    # Create array of missing required fields
    missing_fields = [
        F.when(F.col(field).isNull(), F.lit(field))
        for field in required_fields
    ]

    # Filter out NULLs and create rejection reason
    df = df.withColumn(
        "_validation_required_nulls",
        F.when(
            F.size(F.array_remove(F.array(*missing_fields), None)) > 0,
            F.array(F.concat(
                F.lit("REQUIRED_FIELD_NULL: "),
                F.array_join(F.array_remove(F.array(*missing_fields), None), ", ")
            ))
        )
    )

    return df

def validate_amounts(df):
    """Validate financial amounts"""

    validations = []

    # Rule: Billed amount must be positive (except denied claims can be 0)
    validations.append(
        F.when(
            (F.col("billed_amount") < 0) |
            ((F.col("billed_amount") == 0) & (F.col("claim_status") != "Denied")),
            F.lit("INVALID_BILLED_AMOUNT: Must be > 0 (or = 0 for Denied)")
        )
    )

    # Rule: Allowed amount cannot exceed billed amount
    validations.append(
        F.when(
            (F.col("allowed_amount").isNotNull()) &
            (F.col("allowed_amount") > F.col("billed_amount")),
            F.lit("AMOUNT_LOGIC_ERROR: allowed > billed")
        )
    )

    # Rule: Paid amount cannot exceed allowed amount
    validations.append(
        F.when(
            (F.col("paid_amount").isNotNull()) &
            (F.col("allowed_amount").isNotNull()) &
            (F.col("paid_amount") > F.col("allowed_amount")),
            F.lit("AMOUNT_LOGIC_ERROR: paid > allowed")
        )
    )

    # Combine all amount validations
    df = df.withColumn(
        "_validation_amounts",
        F.array_remove(F.array(*validations), None)
    )

    return df

def validate_dates(df):
    """Validate date ranges and logic"""

    validations = []

    # Rule: Claim date cannot be in the future
    validations.append(
        F.when(
            F.col("claim_date") > F.current_date(),
            F.lit("FUTURE_CLAIM_DATE")
        )
    )

    # Rule: Claim date should be >= 2020 (data sanity check)
    validations.append(
        F.when(
            F.col("claim_date") < F.lit("2020-01-01"),
            F.lit("CLAIM_DATE_TOO_OLD")
        )
    )

    # Rule: Service date cannot be more than 60 days after claim date (allow late submission)
    validations.append(
        F.when(
            F.datediff(F.col("claim_date"), F.col("service_from_date")) < -60,
            F.lit("SERVICE_TOO_FAR_AFTER_CLAIM")
        )
    )

    # Rule: Service to_date >= from_date
    validations.append(
        F.when(
            (F.col("service_to_date").isNotNull()) &
            (F.col("service_to_date") < F.col("service_from_date")),
            F.lit("SERVICE_DATE_RANGE_INVALID")
        )
    )

    # Rule: Discharge date >= admission date (if both present)
    validations.append(
        F.when(
            (F.col("admission_date").isNotNull()) &
            (F.col("discharge_date").isNotNull()) &
            (F.col("discharge_date") < F.col("admission_date")),
            F.lit("DISCHARGE_BEFORE_ADMISSION")
        )
    )

    df = df.withColumn(
        "_validation_dates",
        F.array_remove(F.array(*validations), None)
    )

    return df

def validate_medical_codes(df):
    """Validate ICD-10 and CPT code formats"""

    validations = []

    # ICD-10 format: Letter + 2 digits + optional decimal + 1-3 digits
    icd10_pattern = r'^[A-Z][0-9]{2}(\.[0-9]{1,3})?$'

    validations.append(
        F.when(
            ~F.col("diagnosis_code").rlike(icd10_pattern),
            F.lit("INVALID_ICD10_FORMAT")
        )
    )

    # CPT/HCPCS format: 5 digits OR letter + 4 digits
    cpt_pattern = r'^([0-9]{5}|[A-Z][0-9]{4})$'

    validations.append(
        F.when(
            ~F.col("procedure_code").rlike(cpt_pattern),
            F.lit("INVALID_CPT_FORMAT")
        )
    )

    df = df.withColumn(
        "_validation_codes",
        F.array_remove(F.array(*validations), None)
    )

    return df

def validate_approved_claims(df):
    """Special validations for Approved claims"""

    validation = F.when(
        (F.col("claim_status") == "Approved") &
        ((F.col("paid_amount").isNull()) | (F.col("paid_amount") == 0)),
        F.array(F.lit("APPROVED_WITH_ZERO_PAYMENT"))
    )

    df = df.withColumn("_validation_approved", validation)

    return df

def apply_all_validations(df):
    """
    Apply all validation rules and create combined rejection_reasons array
    Returns: (valid_df, quarantine_df)
    """

    # Apply all validation functions
    df = validate_required_fields(df)
    df = validate_amounts(df)
    df = validate_dates(df)
    df = validate_medical_codes(df)
    df = validate_approved_claims(df)

    # Combine all validation columns into single array
    df = df.withColumn(
        "rejection_reasons",
        F.flatten(F.array(
            F.coalesce(F.col("_validation_required_nulls"), F.array()),
            F.coalesce(F.col("_validation_amounts"), F.array()),
            F.coalesce(F.col("_validation_dates"), F.array()),
            F.coalesce(F.col("_validation_codes"), F.array()),
            F.coalesce(F.col("_validation_approved"), F.array())
        ))
    )

    # Split into valid and quarantine DataFrames
    valid_df = df.filter(F.size(F.col("rejection_reasons")) == 0) \
                  .drop("rejection_reasons", "_validation_required_nulls", "_validation_amounts",
                        "_validation_dates", "_validation_codes", "_validation_approved")

    quarantine_df = df.filter(F.size(F.col("rejection_reasons")) > 0)

    return valid_df, quarantine_df

# ============================================================================
# SECTION 4: State Management & Incremental Processing
# ============================================================================

def get_last_processing_timestamp(spark):
    """Get timestamp of last successful Silver processing run"""

    result = spark.sql("""
        SELECT COALESCE(MAX(processing_end_timestamp), CAST('1970-01-01 00:00:00' AS TIMESTAMP)) as last_timestamp
        FROM iceberg.silver.processing_log
        WHERE status = 'completed'
    """).collect()

    return result[0]['last_timestamp']

def is_file_already_processed(spark, file_path):
    """Check if Bronze file already processed to Silver"""

    existing = spark.sql(f"""
        SELECT COUNT(*) as cnt
        FROM iceberg.silver.processing_log
        WHERE bronze_file_path = '{file_path}'
          AND status = 'completed'
    """).collect()[0]['cnt']

    return existing > 0

def get_unprocessed_bronze_files(spark, last_timestamp):
    """Query Bronze for files that haven't been processed to Silver yet"""

    query = f"""
        SELECT
            file_path,
            file_name,
            payer_name,
            file_year,
            file_month,
            record_count,
            ingestion_timestamp
        FROM iceberg.bronze.claims_raw
        WHERE processing_status = 'registered'
          AND ingestion_timestamp > CAST('{last_timestamp}' AS TIMESTAMP)
        ORDER BY payer_name, file_year, file_month, file_path
    """

    return spark.sql(query)

def log_processing_start(spark, processing_id, file_path, payer_name):
    """Insert processing start record to log"""

    spark.sql(f"""
        INSERT INTO iceberg.silver.processing_log
        VALUES (
            '{processing_id}',
            '{file_path}',
            '{payer_name}',
            CURRENT_TIMESTAMP(),
            NULL,
            'running',
            NULL,
            NULL,
            NULL,
            NULL
        )
    """)

def log_processing_complete(spark, processing_id, records_read, records_accepted, records_quarantined):
    """Update processing log with completion status"""

    # Iceberg doesn't support UPDATE directly, so we use MERGE
    spark.sql(f"""
        MERGE INTO iceberg.silver.processing_log t
        USING (SELECT
            '{processing_id}' as processing_id,
            CURRENT_TIMESTAMP() as processing_end_timestamp,
            'completed' as status,
            CAST({records_read} AS BIGINT) as records_read,
            CAST({records_accepted} AS BIGINT) as records_accepted,
            CAST({records_quarantined} AS BIGINT) as records_quarantined
        ) s
        ON t.processing_id = s.processing_id
        WHEN MATCHED THEN UPDATE SET
            t.processing_end_timestamp = s.processing_end_timestamp,
            t.status = s.status,
            t.records_read = s.records_read,
            t.records_accepted = s.records_accepted,
            t.records_quarantined = s.records_quarantined
    """)

def log_processing_failed(spark, processing_id, error_msg):
    """Update processing log with failure status"""

    # Escape single quotes in error message
    error_msg = error_msg.replace("'", "''")

    spark.sql(f"""
        MERGE INTO iceberg.silver.processing_log t
        USING (SELECT
            '{processing_id}' as processing_id,
            CURRENT_TIMESTAMP() as processing_end_timestamp,
            'failed' as status,
            '{error_msg}' as error_message
        ) s
        ON t.processing_id = s.processing_id
        WHEN MATCHED THEN UPDATE SET
            t.processing_end_timestamp = s.processing_end_timestamp,
            t.status = s.status,
            t.error_message = s.error_message
    """)

def prepare_quarantine_records(quarantine_df, file_path):
    """Prepare quarantine records for writing"""

    # Convert all columns to STRING to preserve original values
    quarantine_cols = [
        F.col("claim_id").cast("string"),
        F.col("patient_id").cast("string"),
        F.col("provider_id").cast("string"),
        F.col("claim_date").cast("string"),
        F.col("service_from_date").cast("string"),
        F.col("service_to_date").cast("string"),
        F.col("admission_date").cast("string"),
        F.col("discharge_date").cast("string"),
        F.col("diagnosis_code").cast("string"),
        F.col("procedure_code").cast("string"),
        F.col("billed_amount").cast("string"),
        F.col("allowed_amount").cast("string"),
        F.col("paid_amount").cast("string"),
        F.col("claim_status").cast("string"),
        F.col("claim_type").cast("string"),
        F.col("payer_name"),
        F.lit(file_path).alias("source_file_path"),
        F.current_timestamp().alias("rejection_timestamp"),
        F.col("rejection_reasons"),
        F.lit(None).cast("string").alias("validation_details"),
        F.lit(None).cast("string").alias("raw_record")
    ]

    return quarantine_df.select(*quarantine_cols)

# ============================================================================
# SECTION 5: Main Processing Logic
# ============================================================================

def process_bronze_to_silver_incremental(spark):
    """
    Incremental processing with checkpoint/resume capability
    """

    # Get last successful processing timestamp
    last_timestamp = get_last_processing_timestamp(spark)

    print(f"Last successful processing: {last_timestamp}")

    # Get unprocessed Bronze files
    bronze_files = get_unprocessed_bronze_files(spark, last_timestamp)

    file_list = bronze_files.collect()

    print(f"Found {len(file_list)} unprocessed files\n")

    if len(file_list) == 0:
        return {
            'files_processed': 0,
            'records_accepted': 0,
            'records_quarantined': 0
        }

    total_accepted = 0
    total_quarantined = 0
    files_processed = 0

    for file_info in file_list:
        file_path = file_info['file_path']
        payer_name = file_info['payer_name']

        # Idempotency check
        if is_file_already_processed(spark, file_path):
            print(f"  ⊘ Skipping {file_path} (already processed)")
            continue

        # Generate unique processing ID
        processing_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{payer_name}_{str(uuid.uuid4())[:8]}"

        try:
            # Log processing start
            log_processing_start(spark, processing_id, file_path, payer_name)

            # Read raw CSV
            raw_df = spark.read.option("header", "true").csv(file_path)
            records_read = raw_df.count()

            # Apply payer-specific transformation
            if payer_name == "BlueCross":
                transformed_df = parse_bluecross_schema(raw_df)
            elif payer_name == "Aetna":
                transformed_df = parse_aetna_schema(raw_df)
            elif payer_name == "UnitedHealth":
                transformed_df = parse_unitedhealth_schema(raw_df)
            else:
                raise ValueError(f"Unknown payer: {payer_name}")

            # Add metadata columns
            transformed_df = transformed_df \
                .withColumn("source_file_path", F.lit(file_path)) \
                .withColumn("ingestion_timestamp", F.current_timestamp()) \
                .withColumn("processing_date", F.current_date())

            # Apply validations
            valid_df, quarantine_df = apply_all_validations(transformed_df)

            records_accepted = valid_df.count()
            records_quarantined = quarantine_df.count()

            # Write valid records to Silver
            if records_accepted > 0:
                valid_df.writeTo("iceberg.silver.claims").append()

            # Write quarantined records
            if records_quarantined > 0:
                quarantine_df_prepared = prepare_quarantine_records(quarantine_df, file_path)
                quarantine_df_prepared.writeTo("iceberg.silver.claims_quarantine").append()

            # Log success
            log_processing_complete(
                spark, processing_id,
                records_read, records_accepted, records_quarantined
            )

            total_accepted += records_accepted
            total_quarantined += records_quarantined
            files_processed += 1

            print(f"  ✓ {payer_name}/{file_info['file_name']}: {records_accepted} accepted, {records_quarantined} quarantined")

        except Exception as e:
            # Log failure
            log_processing_failed(spark, processing_id, str(e))
            print(f"  ✗ {file_path}: FAILED - {e}")
            # Continue with next file (don't fail entire job)
            continue

    return {
        'files_processed': files_processed,
        'records_accepted': total_accepted,
        'records_quarantined': total_quarantined
    }

def run_silver_transformation(spark):
    """Main Silver transformation orchestration"""

    print("\n" + "=" * 70)
    print("SILVER LAYER TRANSFORMATION - MedFlow Analytics")
    print("=" * 70 + "\n")

    # Create tables if not exist
    create_silver_namespace(spark)
    create_silver_tables(spark)

    # Process Bronze files
    results = process_bronze_to_silver_incremental(spark)

    if results['files_processed'] == 0:
        print("\n✓ No new Bronze files to process\n")
        return

    # Summary
    print("\n" + "=" * 70)
    print("TRANSFORMATION SUMMARY")
    print("=" * 70)
    print(f"Files processed:       {results['files_processed']}")
    print(f"Records accepted:      {results['records_accepted']:,}")
    print(f"Records quarantined:   {results['records_quarantined']:,}")

    total_records = results['records_accepted'] + results['records_quarantined']
    if total_records > 0:
        success_rate = (results['records_accepted'] / total_records) * 100
        print(f"Success rate:          {success_rate:.2f}%")

    print("=" * 70)

    # Show Silver table stats
    print("\nSilver table statistics:")
    spark.sql("""
        SELECT
            payer_name,
            COUNT(*) as total_claims,
            MIN(claim_date) as earliest_claim,
            MAX(claim_date) as latest_claim
        FROM iceberg.silver.claims
        GROUP BY payer_name
        ORDER BY payer_name
    """).show(truncate=False)

def main():
    """Main entry point"""

    try:
        spark = create_spark_session()
        run_silver_transformation(spark)

        print("\n✅ Silver transformation completed successfully!\n")
        spark.stop()
        return 0

    except Exception as e:
        print(f"\n❌ Silver transformation failed: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
