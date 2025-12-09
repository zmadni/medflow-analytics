# MedFlow Analytics - Architecture Documentation

This document provides detailed architecture diagrams and component descriptions for the MedFlow Analytics platform.

## Table of Contents
1. [System Overview](#system-overview)
2. [Component Architecture](#component-architecture)
3. [Data Flow](#data-flow)
4. [Network Architecture](#network-architecture)
5. [Data Lake Architecture](#data-lake-architecture)
6. [Pipeline Architecture](#pipeline-architecture)
7. [Technology Stack](#technology-stack)
8. [Deployment Architecture](#deployment-architecture)

---

## System Overview

### High-Level Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        A1[Healthcare Claims API]
        A2[Provider Database]
        A3[Patient Records]
    end

    subgraph "Ingestion Layer"
        B1[Airflow DAGs]
        B2[Apache Spark]
    end

    subgraph "Storage Layer"
        C1[MinIO S3 Storage]
        C2[Apache Iceberg Tables]
        C3[PostgreSQL Metadata]
    end

    subgraph "Transformation Layer"
        D1[dbt Models]
        D2[Spark Jobs]
        D3[Data Quality Checks]
    end

    subgraph "Analytics Layer"
        E1[Fraud Detection Models]
        E2[Claims Analytics]
        E3[Provider Insights]
    end

    subgraph "Visualization Layer"
        F1[BI Dashboard]
        F2[Jupyter Notebooks]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    B1 --> B2
    B2 --> C1
    C1 --> C2
    C2 --> D1
    D1 --> E1
    D1 --> E2
    D1 --> E3
    E1 --> F1
    E2 --> F1
    E3 --> F2
    B1 -.metadata.-> C3
    D1 -.metadata.-> C3
```

---

## Component Architecture

### Docker Services Architecture

```mermaid
graph LR
    subgraph "Docker Network: medflow-network"
        subgraph "Orchestration"
            A1[Airflow Webserver<br/>:8088]
            A2[Airflow Scheduler]
        end

        subgraph "Processing"
            B1[Spark Master<br/>:8080]
            B2[Spark Worker<br/>2GB RAM, 2 Cores]
        end

        subgraph "Storage"
            C1[MinIO API<br/>:9000]
            C2[MinIO Console<br/>:9001]
            C3[PostgreSQL<br/>:5432]
        end

        subgraph "Development"
            D1[Jupyter Lab<br/>:8888]
        end

        A1 --> C3
        A2 --> C3
        A2 --> B1
        B1 --> B2
        B2 --> C1
        D1 --> C1
        D1 --> B1
    end

    subgraph "Persistent Volumes"
        V1[postgres-db-volume]
        V2[minio-data]
    end

    C3 -.-> V1
    C1 -.-> V2
```

### Component Details

| Component | Image | Purpose | Resources |
|-----------|-------|---------|-----------|
| **Airflow Webserver** | apache/airflow:2.8.1 | Web UI for workflow management | 1 core |
| **Airflow Scheduler** | apache/airflow:2.8.1 | DAG scheduling and task execution | 1 core |
| **Spark Master** | bitnami/spark:3.5.0 | Distributed processing coordinator | 1 core |
| **Spark Worker** | bitnami/spark:3.5.0 | Distributed processing executor | 2 cores, 2GB RAM |
| **MinIO** | minio/minio:latest | S3-compatible object storage | Unlimited |
| **PostgreSQL** | postgres:15 | Metadata and analytics database | 1 core |
| **Jupyter Lab** | jupyter/pyspark-notebook | Interactive data exploration | 1 core |

---

## Data Flow

### End-to-End Data Pipeline

```mermaid
flowchart TD
    subgraph "1. Data Ingestion"
        A1[Raw Claims Data] --> A2[Data Validation]
        A2 --> A3[Upload to MinIO<br/>claims-raw bucket]
    end

    subgraph "2. Bronze Layer - Raw Data"
        A3 --> B1[Iceberg Bronze Tables<br/>Immutable Raw Data]
        B1 --> B2[Schema: claims_raw<br/>providers_raw<br/>patients_raw]
    end

    subgraph "3. Silver Layer - Cleaned Data"
        B2 --> C1[Spark Processing]
        C1 --> C2[Data Cleaning<br/>Deduplication<br/>Standardization]
        C2 --> C3[Iceberg Silver Tables]
        C3 --> C4[Schema: claims_clean<br/>providers_clean<br/>patients_clean]
    end

    subgraph "4. Gold Layer - Analytics"
        C4 --> D1[dbt Transformations]
        D1 --> D2[Business Logic<br/>Aggregations<br/>Metrics]
        D2 --> D3[Iceberg Gold Tables]
        D3 --> D4[Schema: claims_analytics<br/>fraud_scores<br/>provider_metrics]
    end

    subgraph "5. Analytics & ML"
        D4 --> E1[Fraud Detection<br/>ML Model]
        D4 --> E2[Claims Analytics<br/>Dashboard]
        D4 --> E3[Provider Insights<br/>Reports]
    end

    style A1 fill:#f9f,stroke:#333,stroke-width:2px
    style B1 fill:#bbf,stroke:#333,stroke-width:2px
    style C3 fill:#bfb,stroke:#333,stroke-width:2px
    style D3 fill:#ffb,stroke:#333,stroke-width:2px
    style E1 fill:#fbb,stroke:#333,stroke-width:2px
```

### Medallion Architecture Layers

**Bronze Layer (Raw)**
- Immutable source data
- Exact copy from source systems
- No transformations
- Full history preserved

**Silver Layer (Cleaned)**
- Validated and cleaned data
- Deduplication applied
- Standardized formats
- Business keys enforced

**Gold Layer (Analytics)**
- Business-level aggregations
- Denormalized for performance
- Metrics and KPIs
- Ready for consumption

---

## Network Architecture

### Service Communication

```mermaid
graph TB
    subgraph "External Access"
        EXT[User Browser]
    end

    subgraph "Docker Network: medflow-network"
        subgraph "Web Layer"
            W1[Airflow UI :8088]
            W2[MinIO Console :9001]
            W3[Spark UI :8080]
            W4[Jupyter :8888]
        end

        subgraph "Application Layer"
            APP1[Airflow Scheduler]
            APP2[Spark Master :7077]
        end

        subgraph "Data Layer"
            DATA1[MinIO API :9000]
            DATA2[PostgreSQL :5432]
        end
    end

    EXT --> W1
    EXT --> W2
    EXT --> W3
    EXT --> W4

    W1 --> APP1
    APP1 --> APP2
    APP2 --> DATA1
    APP1 --> DATA2
    W4 --> APP2
    W4 --> DATA1
```

### Network Ports

| Port | Service | Protocol | Access |
|------|---------|----------|--------|
| 8088 | Airflow Web UI | HTTP | External |
| 8080 | Spark Master UI | HTTP | External |
| 9000 | MinIO API | HTTP | Internal/External |
| 9001 | MinIO Console | HTTP | External |
| 8888 | Jupyter Lab | HTTP | External |
| 5432 | PostgreSQL | TCP | Internal |
| 7077 | Spark Master | TCP | Internal |

---

## Data Lake Architecture

### MinIO Bucket Structure

```
MinIO Object Storage
│
├── claims-raw/                    # Raw ingested data
│   ├── 2024/
│   │   ├── 01/
│   │   │   ├── 01/
│   │   │   │   ├── claims_20240101_001.parquet
│   │   │   │   └── claims_20240101_002.parquet
│   │   │   └── 02/
│   │   └── 02/
│   ├── providers/
│   │   └── providers_snapshot_20240101.parquet
│   └── patients/
│       └── patients_snapshot_20240101.parquet
│
├── claims-processed/              # Transformed data
│   ├── bronze/
│   │   ├── claims/
│   │   ├── providers/
│   │   └── patients/
│   ├── silver/
│   │   ├── claims_clean/
│   │   ├── providers_clean/
│   │   └── patients_clean/
│   └── gold/
│       ├── claims_analytics/
│       ├── fraud_scores/
│       └── provider_metrics/
│
├── iceberg-warehouse/             # Iceberg metadata
│   ├── metadata/
│   │   ├── claims_raw.metadata.json
│   │   └── claims_clean.metadata.json
│   └── data/
│       ├── claims_raw/
│       └── claims_clean/
│
└── dbt-artifacts/                 # dbt outputs
    ├── manifest.json
    ├── run_results.json
    └── catalog.json
```

### Iceberg Table Architecture

```mermaid
graph TB
    subgraph "Iceberg Catalog"
        CAT[PostgreSQL Catalog<br/>Table Metadata]
    end

    subgraph "MinIO Storage"
        subgraph "Metadata Layer"
            M1[Metadata Files<br/>Schema, Snapshots]
            M2[Manifest Lists]
            M3[Manifest Files]
        end

        subgraph "Data Layer"
            D1[Parquet Data Files<br/>Partition 1]
            D2[Parquet Data Files<br/>Partition 2]
            D3[Parquet Data Files<br/>Partition 3]
        end
    end

    CAT --> M1
    M1 --> M2
    M2 --> M3
    M3 --> D1
    M3 --> D2
    M3 --> D3

    style CAT fill:#f9f,stroke:#333,stroke-width:2px
    style M1 fill:#bbf,stroke:#333,stroke-width:2px
    style D1 fill:#bfb,stroke:#333,stroke-width:2px
```

---

## Pipeline Architecture

### Airflow DAG Structure

```mermaid
flowchart LR
    subgraph "Daily Claims Processing DAG"
        A[Start] --> B[Extract Claims<br/>from API]
        B --> C[Validate Data<br/>Quality]
        C --> D{Data Valid?}
        D -->|Yes| E[Upload to<br/>MinIO Raw]
        D -->|No| F[Send Alert]
        E --> G[Trigger Spark<br/>Processing]
        G --> H[Load to Bronze<br/>Iceberg Table]
        H --> I[Run dbt<br/>Transformations]
        I --> J[Data Quality<br/>Tests]
        J --> K{Tests Pass?}
        K -->|Yes| L[Update Gold<br/>Tables]
        K -->|No| M[Send Alert]
        L --> N[Run Fraud<br/>Detection]
        N --> O[End]
        F --> O
        M --> O
    end

    style A fill:#9f9,stroke:#333,stroke-width:2px
    style O fill:#f99,stroke:#333,stroke-width:2px
    style D fill:#ff9,stroke:#333,stroke-width:2px
    style K fill:#ff9,stroke:#333,stroke-width:2px
```

### Data Processing Flow

```mermaid
sequenceDiagram
    participant Airflow
    participant Spark
    participant MinIO
    participant Iceberg
    participant dbt
    participant PostgreSQL

    Airflow->>MinIO: Upload raw claims data
    Airflow->>Spark: Trigger processing job
    Spark->>MinIO: Read raw data
    Spark->>Spark: Clean & transform
    Spark->>Iceberg: Write to Bronze table
    Iceberg->>MinIO: Store data files
    Iceberg->>PostgreSQL: Update metadata
    Airflow->>dbt: Run transformations
    dbt->>Iceberg: Read Bronze tables
    dbt->>Iceberg: Write Silver tables
    dbt->>Iceberg: Write Gold tables
    dbt->>PostgreSQL: Update lineage
    Airflow->>Airflow: Mark success
```

---

## Technology Stack

### Stack Overview

```mermaid
graph TB
    subgraph "Presentation Layer"
        P1[Jupyter Notebooks]
        P2[BI Dashboard]
    end

    subgraph "Analytics Layer"
        A1[Python ML Models]
        A2[SQL Analytics]
    end

    subgraph "Transformation Layer"
        T1[dbt Core 1.7]
        T2[Spark SQL]
    end

    subgraph "Processing Layer"
        PR1[Apache Spark 3.5.0]
        PR2[Python 3.11]
    end

    subgraph "Orchestration Layer"
        O1[Apache Airflow 2.8.1]
    end

    subgraph "Storage Layer"
        S1[Apache Iceberg 0.5.1]
        S2[MinIO Latest]
        S3[PostgreSQL 15]
    end

    subgraph "Infrastructure Layer"
        I1[Docker Compose]
        I2[Linux Ubuntu]
    end

    P1 --> A1
    P2 --> A2
    A1 --> T1
    A2 --> T1
    T1 --> PR1
    T2 --> PR1
    PR1 --> O1
    O1 --> S1
    S1 --> S2
    O1 --> S3
    S1 --> S3
    S2 --> I1
    S3 --> I1
    O1 --> I1
    I1 --> I2
```

### Technology Choices & Rationale

| Component | Technology | Why Chosen |
|-----------|-----------|------------|
| **Orchestration** | Apache Airflow | Industry standard for data pipelines, visual DAG management |
| **Processing** | Apache Spark | Distributed processing, handles large datasets, integrates with Iceberg |
| **Table Format** | Apache Iceberg | ACID transactions, time travel, schema evolution, upserts |
| **Object Storage** | MinIO | S3-compatible, self-hosted, cost-effective for portfolio |
| **Transformations** | dbt | SQL-based transformations, version control, testing framework |
| **Database** | PostgreSQL | Reliable, supports Iceberg catalog, familiar SQL interface |
| **Containers** | Docker | Reproducible environments, easy deployment, isolated services |
| **Language** | Python 3.11 | Data science ecosystem, Airflow native, extensive libraries |

---

## Deployment Architecture

### Local Development Setup

```mermaid
graph TB
    subgraph "Ubuntu Server - 32GB RAM, 8 Cores"
        subgraph "Docker Host"
            D1[Docker Engine]
            D2[Docker Compose]
        end

        subgraph "Running Containers"
            C1[Airflow<br/>2 containers]
            C2[Spark<br/>2 containers]
            C3[MinIO<br/>1 container]
            C4[PostgreSQL<br/>1 container]
            C5[Jupyter<br/>1 container]
        end

        subgraph "Persistent Storage"
            V1[/var/lib/docker/volumes/<br/>postgres-db-volume]
            V2[/var/lib/docker/volumes/<br/>minio-data]
        end

        D1 --> D2
        D2 --> C1
        D2 --> C2
        D2 --> C3
        D2 --> C4
        D2 --> C5
        C4 -.-> V1
        C3 -.-> V2
    end

    subgraph "Project Directory"
        P1[~/Projects/medflow-analytics/]
        P2[dags/]
        P3[plugins/]
        P4[data/]
    end

    C1 -.mount.-> P2
    C1 -.mount.-> P3
    C1 -.mount.-> P4
```

### Resource Allocation

```
Total System Resources:
├── CPU: 8 cores @ i7
├── RAM: 32 GB
└── Disk: 50+ GB available

Allocated to Containers:
├── Airflow Webserver:     1 core,  2 GB RAM
├── Airflow Scheduler:     1 core,  2 GB RAM
├── Spark Master:          1 core,  2 GB RAM
├── Spark Worker:          2 cores, 2 GB RAM
├── MinIO:                 1 core,  2 GB RAM
├── PostgreSQL:            1 core,  2 GB RAM
├── Jupyter:               1 core,  2 GB RAM
└── Reserved for OS:       2 cores, 18 GB RAM
```

### Volume Mounts

```
Host Directory                              Container Mount
─────────────────────────────────────────────────────────────────
~/Projects/medflow-analytics/dags/      →  /opt/airflow/dags
~/Projects/medflow-analytics/plugins/   →  /opt/airflow/plugins
~/Projects/medflow-analytics/config/    →  /opt/airflow/config
~/Projects/medflow-analytics/data/      →  /opt/airflow/data
~/Projects/medflow-analytics/logs/      →  /opt/airflow/logs
~/Projects/medflow-analytics/scripts/   →  /opt/airflow/scripts
~/Projects/medflow-analytics/notebooks/ →  /home/jovyan/work
```

---

## Data Models

### Entity Relationship Diagram

```mermaid
erDiagram
    CLAIMS ||--o{ CLAIM_LINES : contains
    CLAIMS }o--|| PATIENTS : submitted_by
    CLAIMS }o--|| PROVIDERS : billed_by
    PROVIDERS ||--o{ PROVIDER_SPECIALTIES : has
    PATIENTS ||--o{ PATIENT_CONDITIONS : diagnosed_with
    CLAIM_LINES }o--|| PROCEDURES : references
    CLAIM_LINES }o--|| DIAGNOSES : references

    CLAIMS {
        string claim_id PK
        string patient_id FK
        string provider_id FK
        date service_date
        date submission_date
        decimal total_amount
        string claim_status
        string claim_type
    }

    CLAIM_LINES {
        string line_id PK
        string claim_id FK
        string procedure_code FK
        string diagnosis_code FK
        decimal line_amount
        int units
    }

    PATIENTS {
        string patient_id PK
        date date_of_birth
        string gender
        string zip_code
        date enrollment_date
    }

    PROVIDERS {
        string provider_id PK
        string provider_name
        string npi
        string tax_id
        string address
    }

    PROCEDURES {
        string procedure_code PK
        string description
        string category
    }

    DIAGNOSES {
        string diagnosis_code PK
        string description
        string icd_version
    }
```

---

## Security Architecture

### Access Control

```mermaid
graph TB
    subgraph "External Users"
        U1[Data Engineers]
        U2[Data Analysts]
        U3[Auditors]
    end

    subgraph "Authentication Layer"
        A1[Airflow Basic Auth]
        A2[MinIO IAM]
        A3[PostgreSQL Roles]
    end

    subgraph "Authorization Layer"
        Z1[Airflow RBAC]
        Z2[MinIO Policies]
        Z3[Row Level Security]
    end

    subgraph "Data Layer"
        D1[Encrypted Data at Rest]
        D2[Encrypted Data in Transit]
        D3[Audit Logs]
    end

    U1 --> A1
    U1 --> A2
    U2 --> A1
    U3 --> A3
    A1 --> Z1
    A2 --> Z2
    A3 --> Z3
    Z1 --> D1
    Z2 --> D1
    Z3 --> D1
    D1 --> D3
    D2 --> D3
```

### Security Layers

1. **Network Security**
   - Docker network isolation
   - Port exposure control
   - Internal service communication

2. **Application Security**
   - Airflow authentication (basic auth)
   - MinIO access keys
   - PostgreSQL password authentication

3. **Data Security**
   - PHI data anonymization
   - Encryption at rest (volume encryption)
   - Encryption in transit (TLS for production)

4. **Compliance**
   - HIPAA-compliant architecture (with additional hardening)
   - Audit logging
   - Data lineage tracking

---

## Monitoring & Observability

### Monitoring Architecture

```mermaid
graph TB
    subgraph "Application Metrics"
        M1[Airflow Task Status]
        M2[Spark Job Metrics]
        M3[Data Quality Metrics]
    end

    subgraph "Infrastructure Metrics"
        I1[Docker Container Stats]
        I2[CPU/Memory Usage]
        I3[Disk I/O]
    end

    subgraph "Logging"
        L1[Airflow Logs]
        L2[Spark Logs]
        L3[Application Logs]
    end

    subgraph "Alerting"
        A1[Task Failures]
        A2[Data Quality Issues]
        A3[Resource Alerts]
    end

    M1 --> A1
    M3 --> A2
    I2 --> A3
    L1 --> A1
    L2 --> A1
```

### Key Metrics to Monitor

**Pipeline Metrics:**
- DAG success/failure rate
- Task execution duration
- Data processing volume
- Data quality check results

**System Metrics:**
- Container health status
- CPU and memory utilization
- Disk space usage
- Network throughput

**Business Metrics:**
- Claims processed per day
- Fraud detection rate
- Data freshness
- SLA compliance

---

## Scalability Considerations

### Horizontal Scaling

```
Current Setup (Local Dev):
└── 1 Spark Worker (2 cores, 2GB)

Production Scaling:
├── 5 Spark Workers (4 cores, 8GB each)
├── Multiple Airflow Workers
└── Read Replicas for PostgreSQL
```

### Vertical Scaling

```
Component            Current    Production
─────────────────────────────────────────
Spark Worker Memory  2 GB       16 GB
Spark Worker Cores   2          8
PostgreSQL Memory    2 GB       32 GB
MinIO Cluster        Single     Multi-node
```

### Future Enhancements

1. **Kubernetes Migration**
   - Container orchestration
   - Auto-scaling
   - High availability

2. **Cloud Deployment**
   - AWS S3 instead of MinIO
   - AWS Glue/EMR instead of self-hosted Spark
   - Managed Airflow (MWAA)

3. **Performance Optimization**
   - Iceberg partition optimization
   - Query result caching
   - Materialized views

---

## Appendix

### Glossary

- **DAG**: Directed Acyclic Graph - Airflow workflow definition
- **Iceberg**: Open table format for huge analytic datasets
- **Medallion**: Bronze/Silver/Gold data architecture pattern
- **MinIO**: S3-compatible object storage
- **dbt**: Data build tool for transformations
- **HIPAA**: Health Insurance Portability and Accountability Act

### References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [MinIO Documentation](https://min.io/docs/)
- [dbt Documentation](https://docs.getdbt.com/)

---

**Document Version:** 1.0
**Last Updated:** December 2024
**Project:** MedFlow Analytics
**Architecture Type:** Data Lakehouse with Medallion Pattern
