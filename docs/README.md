# MedFlow Analytics

A production-quality data engineering portfolio project demonstrating modern healthcare claims analytics using Apache Iceberg, Spark, Airflow, and LocalStack (AWS S3).

## Tech Stack
- **Storage**: LocalStack (AWS S3 emulation for local development)
- **Table Format**: Apache Iceberg
- **Processing**: Apache Spark
- **Orchestration**: Airflow
- **Business Domain**: Healthcare claims processing & fraud detection

## Project Status

### Infrastructure Setup
- **12/16/2025**: Phase 1 - PostgreSQL database implementation
- **12/20/2025**: Phase 2 - Airflow orchestration setup
- **12/22/2025**: Phase 3 - LocalStack (AWS S3 emulation) integration
- **12/26/2025**: Phase 4 - Apache Spark cluster deployment

### Medallion Architecture Implementation
- **12/31/2025**: ✅ **Complete Bronze → Silver → Gold Pipeline**
  - **Bronze Layer**: Raw data ingestion from S3 to Iceberg tables
  - **Silver Layer**: Data quality validation, schema standardization, quarantine pattern
  - **Gold Layer**: 3 business aggregation tables (monthly summary, approval funnel, provider performance)
  - **Airflow DAG**: End-to-end orchestration with 3 task groups and data quality gates
  - **Custom Docker Image**: Extended Airflow image with Docker CLI for container orchestration
  - **Production Patterns**: Idempotent processing, partition strategies, ACID transactions

### Pipeline Execution
- **Total Data Volume**: ~100K healthcare claims across 3 payers (BlueCross, Aetna, UnitedHealth)
- **Data Quality**: 5-category validation framework with automated quarantine
- **Processing Time**: ~25-30 minutes for complete Bronze → Silver → Gold execution
- **Tables Created**: 7 Iceberg tables (1 Bronze, 3 Silver, 3 Gold)

## Key Components

### Data Pipeline Scripts
- **Bronze Layer**: `scripts/python/bronze_ingestion.py` - S3 to Iceberg ingestion
- **Silver Layer**: `scripts/python/silver_transformation.py` - Multi-payer schema standardization
- **Gold Layer**: `scripts/python/gold_aggregation.py` - Business metrics aggregation
- **Verification**: `verify_bronze.py`, `verify_silver.py`, `verify_gold.py` - Data quality checks
- **Quality Gate**: `scripts/python/check_silver_quality.py` - Automated quarantine rate validation
- **Cleanup**: `scripts/python/cleanup_iceberg.py` - Table cleanup utility

### Orchestration
- **Airflow DAG**: `dags/medflow_pipeline.py` - Complete Bronze → Silver → Gold workflow
  - 3 Task Groups (Bronze, Silver, Gold)
  - 9 Tasks total with automated dependencies
  - Data quality gates with 10% quarantine threshold
  - Manual trigger for development/testing

### Infrastructure
- **Docker Image**: `Dockerfile.airflow` - Custom Airflow image with Docker CLI
- **Spark Wrapper**: `scripts/run_spark_iceberg.sh` - Intelligent Spark job execution
- **Docker Compose**: Multi-service orchestration (PostgreSQL, Airflow, LocalStack, Spark)

### Documentation
- **DOCKER_AIRFLOW_CHALLENGE.md** - Complete breakdown of Docker-in-Docker solution
- **ICEBERG_SETUP.md** - Iceberg configuration and troubleshooting
- **PIPELINE_DESIGN.md** - Medallion architecture design decisions
- **PHASES_GUIDE.md** - Step-by-step implementation guide

## Architecture
Please refer to ARCHITECTURE.md for detailed system design.

## Getting Started

### Prerequisites
- Docker & Docker Compose
- 8GB+ RAM recommended
- ~20GB disk space for containers and data

### Required JAR Files
The following JAR files must be downloaded and placed in `jars/iceberg/` directory. These are too large to commit to GitHub:

```bash
# Create JAR directory
mkdir -p jars/iceberg

# Download required JARs (place in jars/iceberg/)
```

**Apache Iceberg & Spark:**
- `iceberg-spark-runtime-3.5_2.12-1.5.0.jar`
  - Download: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/
  - Size: ~60 MB
  - Purpose: Core Iceberg + Spark integration

**AWS S3 Support:**
- `iceberg-aws-bundle-1.5.0.jar`
  - Download: https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/
  - Size: ~20 MB
  - Purpose: Iceberg AWS integrations (S3FileIO)

- `aws-java-sdk-bundle-1.12.262.jar`
  - Download: https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/
  - Size: ~280 MB
  - Purpose: AWS SDK for S3 operations

- `hadoop-aws-3.3.4.jar`
  - Download: https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/
  - Size: ~1 MB
  - Purpose: Hadoop S3A filesystem implementation

**Database Connectivity:**
- `postgresql-42.7.1.jar`
  - Download: https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
  - Size: ~1 MB
  - Purpose: Iceberg JDBC catalog backend

**Quick Download Script:**
```bash
cd jars/iceberg

# Iceberg Spark Runtime
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

# Iceberg AWS Bundle
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar

# AWS SDK Bundle
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Hadoop AWS
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# PostgreSQL JDBC
wget https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Verify downloads
ls -lh
```

**Total size:** ~362 MB

### Services
The following services run in containers:
- **PostgreSQL**: Airflow metadata & Iceberg catalog
- **Airflow**: Workflow orchestration (scheduler + webserver)
- **LocalStack**: AWS S3 emulation
- **Apache Spark**: Distributed processing (master + worker)

### Quick Start
```bash
# 1. Download JAR files (see above)

# 2. Start all services
docker compose up -d

# 3. Access Airflow UI
# http://localhost:8088 (airflow/airflow)

# 4. Trigger the pipeline
# Navigate to DAG: medflow_bronze_silver_gold_pipeline
# Click "Trigger DAG"

# 5. Monitor execution
# Watch task groups execute: Bronze → Silver → Gold
# Total runtime: ~25-30 minutes
```

### Verification
```bash
# View Gold layer results
docker exec medflow-spark-master /opt/scripts/run_spark_iceberg.sh /opt/scripts/python/verify_gold.py
```

## Author
Zeeshan Madni

## License
Copyright © 2024 Zeeshan Madni. All Rights Reserved.

This project is made public for portfolio demonstration purposes only. No part of this repository may be reproduced, distributed, or used without prior written permission from the copyright holder.
