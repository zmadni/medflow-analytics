#!/bin/bash
#
# Helper script to run Spark jobs with Iceberg support
# Usage: ./scripts/run_spark_iceberg.sh <python_script>
#
# Example: ./scripts/run_spark_iceberg.sh /opt/scripts/test_iceberg.py
#

if [ -z "$1" ]; then
  echo "Usage: $0 <python_script>"
  echo "Example: $0 /opt/scripts/test_iceberg.py"
  exit 1
fi

SCRIPT_PATH="$1"

echo "Running Spark job with Iceberg support..."
echo "Script: $SCRIPT_PATH"
echo ""

# Check if we're already inside a container (e.g., called via docker exec)
if [ -f /.dockerenv ] || grep -q docker /proc/1/cgroup 2>/dev/null; then
  # Already inside container - run spark-submit directly
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/iceberg/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/spark/jars/iceberg/iceberg-aws-bundle-1.5.0.jar,/opt/spark/jars/iceberg/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/iceberg/hadoop-aws-3.3.4.jar,/opt/spark/jars/iceberg/postgresql-42.7.1.jar \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=jdbc \
  --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://postgres:5432/airflow \
  --conf spark.sql.catalog.iceberg.jdbc.user=airflow \
  --conf spark.sql.catalog.iceberg.jdbc.password=airflow \
  --conf spark.sql.catalog.iceberg.warehouse=s3://iceberg-warehouse/ \
  --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg.client.region=us-east-1 \
  --conf spark.sql.catalog.iceberg.s3.endpoint=http://localstack:4566 \
  --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
  --conf spark.sql.catalog.iceberg.s3.access-key-id=test \
  --conf spark.sql.catalog.iceberg.s3.secret-access-key=test \
  "$SCRIPT_PATH"
else
  # Running from host - use docker exec
  docker exec medflow-spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/jars/iceberg/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/spark/jars/iceberg/iceberg-aws-bundle-1.5.0.jar,/opt/spark/jars/iceberg/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/iceberg/hadoop-aws-3.3.4.jar,/opt/spark/jars/iceberg/postgresql-42.7.1.jar \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=jdbc \
    --conf spark.sql.catalog.iceberg.uri=jdbc:postgresql://postgres:5432/airflow \
    --conf spark.sql.catalog.iceberg.jdbc.user=airflow \
    --conf spark.sql.catalog.iceberg.jdbc.password=airflow \
    --conf spark.sql.catalog.iceberg.warehouse=s3://iceberg-warehouse/ \
    --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
    --conf spark.hadoop.fs.s3a.access.key=test \
    --conf spark.hadoop.fs.s3a.secret.key=test \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg.client.region=us-east-1 \
    --conf spark.sql.catalog.iceberg.s3.endpoint=http://localstack:4566 \
    --conf spark.sql.catalog.iceberg.s3.path-style-access=true \
    --conf spark.sql.catalog.iceberg.s3.access-key-id=test \
    --conf spark.sql.catalog.iceberg.s3.secret-access-key=test \
    "$SCRIPT_PATH"
fi
