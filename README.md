# MedFlow Analytics

A production-quality data engineering portfolio project demonstrating modern healthcare claims analytics using Apache Iceberg, Spark, Airflow, and LocalStack (AWS S3).

## Tech Stack
- **Storage**: LocalStack (AWS S3 emulation for local development)
- **Table Format**: Apache Iceberg
- **Processing**: Apache Spark
- **Orchestration**: Airflow
- **Business Domain**: Healthcare claims processing & fraud detection

## Project Status
- **Docker Compose (12/16/2025)**: Implementing the first phase of docker-compose.yml (postgres db)
- **Docker Compose (12/20/2025)**: Implementing the second phase of docker-compose.yml (Airflow)
- **Docker Compose (12/22/2025)**: Implementing the third phase of docker-compose.yml (Localstack)

## Architecture
Please refer to ARCHITECTURE.md

## Getting Started
The following services will be in containers:
- Postgresql
- Airflow
- LocalStack
- Apache Spark
- Jupyter Notebook (optional)

## Author
Zeeshan Madni

## License
Copyright © 2024 Zeeshan Madni. All Rights Reserved.

This project is made public for portfolio demonstration purposes only. No part of this repository may be reproduced, distributed, or used without prior written permission from the copyright holder.
