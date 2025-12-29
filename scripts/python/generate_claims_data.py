#!/usr/bin/env python3
"""
Healthcare Claims Data Generator

Generates synthetic healthcare claims data with realistic attributes:
- Multiple insurance payers with different CSV schemas
- Realistic ICD-10 diagnosis codes and CPT procedure codes
- Variable claim amounts and approval rates
- 6 months of historical data
- Uploads to LocalStack S3

Usage:
    python scripts/generate_claims_data.py --claims 100000 --months 6
"""

import random
import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import boto3
from faker import Faker
from decimal import Decimal
import sys

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)


# ============================================================================
# REALISTIC MEDICAL CODES
# ============================================================================

# Common ICD-10 diagnosis codes (International Classification of Diseases)
ICD10_CODES = [
    # Diabetes
    ('E11.9', 'Type 2 diabetes mellitus without complications'),
    ('E11.65', 'Type 2 diabetes with hyperglycemia'),
    ('E10.9', 'Type 1 diabetes mellitus without complications'),

    # Hypertension
    ('I10', 'Essential (primary) hypertension'),
    ('I11.0', 'Hypertensive heart disease with heart failure'),

    # Respiratory
    ('J44.1', 'Chronic obstructive pulmonary disease with acute exacerbation'),
    ('J18.9', 'Pneumonia, unspecified organism'),
    ('J45.909', 'Unspecified asthma, uncomplicated'),

    # Cardiovascular
    ('I21.9', 'Acute myocardial infarction, unspecified'),
    ('I50.9', 'Heart failure, unspecified'),
    ('I63.9', 'Cerebral infarction, unspecified'),

    # Musculoskeletal
    ('M25.511', 'Pain in right shoulder'),
    ('M54.5', 'Low back pain'),
    ('M17.9', 'Osteoarthritis of knee, unspecified'),

    # Mental Health
    ('F41.1', 'Generalized anxiety disorder'),
    ('F32.9', 'Major depressive disorder, single episode, unspecified'),

    # Cancer
    ('C50.919', 'Malignant neoplasm of unspecified site of unspecified female breast'),
    ('C34.90', 'Malignant neoplasm of unspecified part of unspecified bronchus or lung'),

    # Other Common
    ('N18.3', 'Chronic kidney disease, stage 3'),
    ('K21.9', 'Gastro-esophageal reflux disease without esophagitis'),
    ('Z23', 'Encounter for immunization'),
]

# Common CPT procedure codes (Current Procedural Terminology)
CPT_CODES = [
    # Office Visits
    ('99213', 'Office visit, established patient, moderate complexity', 150.00),
    ('99214', 'Office visit, established patient, high complexity', 200.00),
    ('99215', 'Office visit, established patient, very high complexity', 280.00),
    ('99203', 'Office visit, new patient, moderate complexity', 175.00),

    # Emergency
    ('99284', 'Emergency department visit, high severity', 450.00),
    ('99285', 'Emergency department visit, very high severity', 650.00),

    # Lab Tests
    ('80053', 'Comprehensive metabolic panel', 35.00),
    ('85025', 'Complete blood count with differential', 25.00),
    ('83036', 'Hemoglobin A1C level', 30.00),

    # Imaging
    ('71020', 'Chest X-ray, 2 views', 120.00),
    ('70450', 'CT scan head without contrast', 800.00),
    ('72148', 'MRI lumbar spine without contrast', 1200.00),

    # Surgery
    ('27447', 'Total knee replacement', 18000.00),
    ('33533', 'Coronary artery bypass, single graft', 45000.00),
    ('43235', 'Esophagogastroduodenoscopy (EGD)', 1500.00),

    # Therapy
    ('97110', 'Physical therapy, therapeutic exercises', 85.00),
    ('90834', 'Psychotherapy, 45 minutes', 120.00),

    # Preventive
    ('90471', 'Immunization administration', 25.00),
    ('G0438', 'Annual wellness visit, initial', 180.00),
]


# ============================================================================
# PAYER CONFIGURATIONS
# ============================================================================

class PayerConfig:
    """Configuration for each insurance payer with different CSV schemas"""

    @staticmethod
    def get_config(payer_name):
        """Return schema configuration for specified payer"""

        if payer_name == "BlueCross":
            return {
                'name': 'BlueCross',
                'columns': [
                    'ClaimID',
                    'MemberID',
                    'ProviderNPI',
                    'ClaimDate',
                    'ServiceFromDate',
                    'ServiceToDate',
                    'DiagnosisCode',
                    'ProcedureCode',
                    'BilledAmount',
                    'AllowedAmount',
                    'PaidAmount',
                    'Status',
                    'Type'
                ],
                'date_format': '%Y-%m-%d',
                'amount_format': lambda x: f"{x:.2f}",
                'approval_rate': 0.88,
            }

        elif payer_name == "Aetna":
            return {
                'name': 'Aetna',
                'columns': [
                    'claim_number',
                    'patient_id',
                    'provider_tax_id',
                    'claim_received_date',
                    'service_date',
                    'admit_date',
                    'discharge_date',
                    'primary_diagnosis',
                    'procedure',
                    'charge_amount',
                    'approved_amount',
                    'payment_amount',
                    'claim_status',
                    'service_category'
                ],
                'date_format': '%m/%d/%Y',
                'amount_format': lambda x: f"${x:,.2f}",
                'approval_rate': 0.85,
            }

        elif payer_name == "UnitedHealth":
            return {
                'name': 'UnitedHealth',
                'columns': [
                    'CLAIM_ID',
                    'SUBSCRIBER_ID',
                    'RENDERING_PROVIDER_NPI',
                    'RECEIVED_DT',
                    'SVC_FROM_DT',
                    'SVC_TO_DT',
                    'DIAG_CD',
                    'PROC_CD',
                    'BILLED_AMT',
                    'ALLOWED_AMT',
                    'PAID_AMT',
                    'CLM_STATUS',
                    'CLM_TYPE'
                ],
                'date_format': '%Y%m%d',
                'amount_format': lambda x: f"{x:.2f}",
                'approval_rate': 0.82,
            }

        else:
            raise ValueError(f"Unknown payer: {payer_name}")


# ============================================================================
# CLAIM GENERATOR
# ============================================================================

class ClaimGenerator:
    """Generate realistic synthetic healthcare claims"""

    def __init__(self, seed=42):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        self.claim_counter = 100000000
        self.patient_counter = 100000

    def generate_claim_id(self, payer_prefix):
        """Generate unique claim ID"""
        claim_id = f"{payer_prefix}-{self.claim_counter}"
        self.claim_counter += 1
        return claim_id

    def generate_patient_id(self):
        """Generate patient ID"""
        # Allow patient IDs to repeat (same patient can have multiple claims)
        patient_id = f"PAT{random.randint(100000, 999999)}"
        return patient_id

    def generate_provider_id(self):
        """Generate NPI (National Provider Identifier)"""
        return f"{random.randint(1000000000, 9999999999)}"

    def generate_diagnosis(self):
        """Select random ICD-10 code"""
        code, description = random.choice(ICD10_CODES)
        return code

    def generate_procedure(self):
        """Select random CPT code with base cost"""
        code, description, base_cost = random.choice(CPT_CODES)
        # Add random variation (±20%)
        variation = random.uniform(0.8, 1.2)
        actual_cost = base_cost * variation
        return code, actual_cost

    def generate_claim_dates(self, start_date, end_date):
        """Generate claim and service dates"""
        # Service date: random date in range
        service_date = fake.date_between(start_date=start_date, end_date=end_date)

        # Claim received: 1-45 days after service
        days_delay = random.randint(1, 45)
        claim_date = service_date + timedelta(days=days_delay)

        # For inpatient: admission and discharge dates
        is_inpatient = random.random() < 0.15  # 15% inpatient
        if is_inpatient:
            admit_date = service_date
            length_of_stay = random.randint(1, 10)
            discharge_date = admit_date + timedelta(days=length_of_stay)
            return claim_date, service_date, admit_date, discharge_date, 'Inpatient'
        else:
            return claim_date, service_date, service_date, service_date, 'Outpatient'

    def generate_amounts(self, base_cost, approval_rate):
        """Generate billed, allowed, and paid amounts"""
        # Billed amount: base cost with 20-50% markup
        markup = random.uniform(1.2, 1.5)
        billed_amount = base_cost * markup

        # High-cost claims (5% chance)
        if random.random() < 0.05:
            billed_amount *= random.uniform(5, 20)

        # Claim status based on approval rate
        is_approved = random.random() < approval_rate

        if is_approved:
            # Allowed amount: typically 60-80% of billed
            allowed_amount = billed_amount * random.uniform(0.6, 0.8)
            # Paid amount: usually same as allowed (assuming met deductible)
            paid_amount = allowed_amount * random.uniform(0.95, 1.0)
            status = 'Approved'
        else:
            allowed_amount = 0
            paid_amount = 0
            status = random.choice(['Denied', 'Pending', 'Under Review'])

        return (
            round(billed_amount, 2),
            round(allowed_amount, 2),
            round(paid_amount, 2),
            status
        )

    def generate_claim(self, payer_config, start_date, end_date):
        """Generate a single claim record"""
        payer_name = payer_config['name']

        # Generate IDs
        claim_id = self.generate_claim_id(payer_name[:3].upper())
        patient_id = self.generate_patient_id()
        provider_id = self.generate_provider_id()

        # Generate dates
        claim_date, service_from, service_to, discharge_date, claim_type = self.generate_claim_dates(start_date, end_date)

        # Generate medical codes
        diagnosis_code = self.generate_diagnosis()
        procedure_code, base_cost = self.generate_procedure()

        # Generate amounts
        billed, allowed, paid, status = self.generate_amounts(
            base_cost,
            payer_config['approval_rate']
        )

        # Format dates according to payer format
        date_fmt = payer_config['date_format']
        amount_fmt = payer_config['amount_format']

        # Map to payer's schema
        columns = payer_config['columns']

        # Create row data based on column names
        row = {}
        for col in columns:
            col_lower = col.lower()

            # Claim ID
            if 'claim' in col_lower and 'id' in col_lower or col_lower == 'claim_number':
                row[col] = claim_id

            # Patient ID
            elif 'patient' in col_lower or 'member' in col_lower or 'subscriber' in col_lower:
                row[col] = patient_id

            # Provider ID
            elif 'provider' in col_lower or 'npi' in col_lower or 'tax_id' in col_lower:
                row[col] = provider_id

            # Claim received date
            elif 'claim' in col_lower and 'date' in col_lower or 'received' in col_lower:
                row[col] = claim_date.strftime(date_fmt)

            # Service from date
            elif 'service' in col_lower and 'from' in col_lower or col_lower == 'service_date' or 'svc_from' in col_lower:
                row[col] = service_from.strftime(date_fmt)

            # Service to date
            elif 'service' in col_lower and 'to' in col_lower or 'svc_to' in col_lower:
                row[col] = service_to.strftime(date_fmt)

            # Admission date
            elif 'admit' in col_lower:
                row[col] = service_from.strftime(date_fmt)

            # Discharge date
            elif 'discharge' in col_lower:
                row[col] = discharge_date.strftime(date_fmt)

            # Diagnosis
            elif 'diag' in col_lower:
                row[col] = diagnosis_code

            # Procedure
            elif 'proc' in col_lower:
                row[col] = procedure_code

            # Billed amount
            elif 'billed' in col_lower or 'charge' in col_lower:
                row[col] = amount_fmt(billed)

            # Allowed amount
            elif 'allowed' in col_lower or 'approved' in col_lower:
                row[col] = amount_fmt(allowed)

            # Paid amount
            elif 'paid' in col_lower or 'payment' in col_lower:
                row[col] = amount_fmt(paid)

            # Status
            elif 'status' in col_lower:
                row[col] = status

            # Type
            elif 'type' in col_lower or 'category' in col_lower:
                row[col] = claim_type

            else:
                row[col] = ''  # Unknown column

        return row


# ============================================================================
# FILE GENERATION
# ============================================================================

def generate_claims_file(payer_name, num_claims, start_date, end_date, output_dir):
    """Generate CSV file with claims for a specific payer"""

    payer_config = PayerConfig.get_config(payer_name)
    generator = ClaimGenerator()

    # Create output directory
    output_path = Path(output_dir) / f"provider={payer_name}" / f"year={start_date.year}" / f"month={start_date.month:02d}"
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = output_path / f"claims_{start_date.strftime('%Y%m%d')}_{timestamp}.csv"

    print(f"Generating {num_claims:,} claims for {payer_name}...")
    print(f"Output: {filename}")

    # Write CSV
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=payer_config['columns'])
        writer.writeheader()

        for i in range(num_claims):
            claim = generator.generate_claim(payer_config, start_date, end_date)
            writer.writerow(claim)

            # Progress indicator
            if (i + 1) % 10000 == 0:
                print(f"Generated {i+1:,} / {num_claims:,} claims...")

    print(f"Complete: {filename}")
    return filename


# ============================================================================
# S3 UPLOAD
# ============================================================================

def upload_to_s3(local_file, bucket, s3_key, endpoint_url='http://localhost:4566'):
    """Upload file to LocalStack S3"""

    print(f"  Uploading to s3://{bucket}/{s3_key}...")

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id='test',
            aws_secret_access_key='test',
            region_name='us-east-1'
        )

        s3_client.upload_file(str(local_file), bucket, s3_key)
        print(f"Uploaded successfully")
        return True

    except Exception as e:
        print(f"Upload failed: {e}")
        return False


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic healthcare claims data')
    parser.add_argument('--claims', type=int, default=100000, help='Total number of claims to generate')
    parser.add_argument('--months', type=int, default=6, help='Number of historical months')
    parser.add_argument('--output-dir', type=str, default='data/raw/claims', help='Output directory for CSV files')
    parser.add_argument('--upload-s3', action='store_true', help='Upload to LocalStack S3')
    parser.add_argument('--s3-bucket', type=str, default='claims-raw', help='S3 bucket name')
    parser.add_argument('--s3-endpoint', type=str, default='http://localhost:4566', help='S3 endpoint URL')

    args = parser.parse_args()

    print("=" * 70)
    print("Healthcare Claims Data Generator")
    print("=" * 70)
    print(f"Total claims: {args.claims:,}")
    print(f"Historical months: {args.months}")
    print(f"Output directory: {args.output_dir}")
    print(f"Upload to S3: {args.upload_s3}")
    print("=" * 70)
    print()

    # Payers to generate
    payers = ['BlueCross', 'Aetna', 'UnitedHealth']
    claims_per_payer = args.claims // len(payers)

    # Date range for claims
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30 * args.months)

    # Generate data month by month
    current_date = start_date
    month_count = 0

    all_files = []

    while current_date < end_date:
        month_count += 1
        month_end = current_date + timedelta(days=30)
        if month_end > end_date:
            month_end = end_date

        print(f"\n📅 Month {month_count}/{args.months}: {current_date.strftime('%B %Y')}")
        print("-" * 70)

        for payer in payers:
            # Generate CSV file
            filename = generate_claims_file(
                payer,
                claims_per_payer // args.months,
                current_date,
                month_end,
                args.output_dir
            )

            # Upload to S3 if requested
            if args.upload_s3:
                # S3 key: claims/provider=BlueCross/year=2024/month=07/claims_20240701_123456.csv
                s3_key = f"claims/{filename.relative_to(args.output_dir)}"
                upload_to_s3(filename, args.s3_bucket, str(s3_key), args.s3_endpoint)

            all_files.append(filename)

        current_date = month_end

    print("\n" + "=" * 70)
    print("✅ Data Generation Complete!")
    print("=" * 70)
    print(f"Total files generated: {len(all_files)}")
    print(f"Total claims: {args.claims:,}")
    print(f"Files location: {args.output_dir}")

    if args.upload_s3:
        print(f"S3 bucket: s3://{args.s3_bucket}/claims/")

    # print("\nNext steps:")
    # print("  1. Verify files: ls -lh", args.output_dir)
    # print("  2. Check S3 (if uploaded): aws --endpoint-url=http://localhost:4566 s3 ls s3://claims-raw/claims/ --recursive")
    # print("  3. Run Bronze ingestion: python scripts/bronze_ingestion.py")
    # print()


if __name__ == '__main__':
    main()
