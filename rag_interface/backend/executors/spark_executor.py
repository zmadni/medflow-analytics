"""
Spark Query Executor for RAG System
Executes validated SQL queries against Apache Iceberg tables

Author: Zeeshan Madni
Created: 2025-12-31
"""

from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
import os

class SparkExecutor:
    """Executes SQL queries via Apache Spark against Iceberg tables"""

    def __init__(self, app_name: str = "MedFlow_RAG_Query_Executor"):
        """
        Initialize Spark executor

        Args:
            app_name: Name for Spark application
        """
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        self._initialize_spark()

    def _initialize_spark(self):
        """Initialize Spark session with Iceberg configuration"""
        try:
            # Get configuration from environment variables
            postgres_host = os.getenv("POSTGRES_HOST", "postgres")
            postgres_port = os.getenv("POSTGRES_PORT", "5432")
            postgres_db = os.getenv("POSTGRES_DB", "iceberg_catalog")
            postgres_user = os.getenv("POSTGRES_USER", "admin")
            postgres_password = os.getenv("POSTGRES_PASSWORD", "admin123")

            jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"

            # Iceberg JAR paths
            jar_dir = "/app/jars/iceberg"
            iceberg_jars = ",".join([
                f"{jar_dir}/iceberg-spark-runtime-3.5_2.12-1.5.0.jar",
                f"{jar_dir}/iceberg-aws-bundle-1.5.0.jar",
                f"{jar_dir}/postgresql-42.7.1.jar",
                f"{jar_dir}/aws-java-sdk-bundle-1.12.262.jar",
                f"{jar_dir}/hadoop-aws-3.3.4.jar"
            ])

            # Warehouse path for Iceberg data files (using S3/LocalStack)
            warehouse_path = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse/")

            # LocalStack S3 configuration
            localstack_endpoint = os.getenv("LOCALSTACK_ENDPOINT", "http://localstack:4566")
            s3_access_key = os.getenv("S3_ACCESS_KEY", "test")
            s3_secret_key = os.getenv("S3_SECRET_KEY", "test")

            # Build Spark session with multiple Iceberg catalogs
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.jars", iceberg_jars) \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.iceberg.type", "jdbc") \
                .config("spark.sql.catalog.iceberg.uri", jdbc_url) \
                .config("spark.sql.catalog.iceberg.jdbc.user", postgres_user) \
                .config("spark.sql.catalog.iceberg.jdbc.password", postgres_password) \
                .config("spark.sql.catalog.iceberg.warehouse", warehouse_path) \
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                .config("spark.sql.catalog.iceberg.client.region", "us-east-1") \
                .config("spark.sql.catalog.iceberg.s3.endpoint", localstack_endpoint) \
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
                .config("spark.sql.catalog.iceberg.s3.access-key-id", s3_access_key) \
                .config("spark.sql.catalog.iceberg.s3.secret-access-key", s3_secret_key) \
                .config("spark.hadoop.fs.s3a.endpoint", localstack_endpoint) \
                .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.sql.catalog.healthcare", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.healthcare.type", "jdbc") \
                .config("spark.sql.catalog.healthcare.uri", jdbc_url) \
                .config("spark.sql.catalog.healthcare.jdbc.user", postgres_user) \
                .config("spark.sql.catalog.healthcare.jdbc.password", postgres_password) \
                .config("spark.sql.catalog.healthcare.warehouse", warehouse_path) \
                .config("spark.sql.catalog.healthcare.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                .config("spark.sql.catalog.healthcare.client.region", "us-east-1") \
                .config("spark.sql.catalog.healthcare.s3.endpoint", localstack_endpoint) \
                .config("spark.sql.catalog.healthcare.s3.path-style-access", "true") \
                .config("spark.sql.catalog.healthcare.s3.access-key-id", s3_access_key) \
                .config("spark.sql.catalog.healthcare.s3.secret-access-key", s3_secret_key) \
                .config("spark.sql.catalog.healthcare_gold", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.healthcare_gold.type", "jdbc") \
                .config("spark.sql.catalog.healthcare_gold.uri", jdbc_url) \
                .config("spark.sql.catalog.healthcare_gold.jdbc.user", postgres_user) \
                .config("spark.sql.catalog.healthcare_gold.jdbc.password", postgres_password) \
                .config("spark.sql.catalog.healthcare_gold.warehouse", warehouse_path) \
                .config("spark.sql.catalog.healthcare_gold.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
                .config("spark.sql.catalog.healthcare_gold.client.region", "us-east-1") \
                .config("spark.sql.catalog.healthcare_gold.s3.endpoint", localstack_endpoint) \
                .config("spark.sql.catalog.healthcare_gold.s3.path-style-access", "true") \
                .config("spark.sql.catalog.healthcare_gold.s3.access-key-id", s3_access_key) \
                .config("spark.sql.catalog.healthcare_gold.s3.secret-access-key", s3_secret_key) \
                .getOrCreate()

            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")

            print("✅ Spark session initialized successfully")

        except Exception as e:
            print(f"❌ Error initializing Spark session: {e}")
            self.spark = None

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute SQL query and return results as list of dictionaries

        Args:
            sql: Validated SQL query string

        Returns:
            List of dictionaries representing query results

        Raises:
            RuntimeError: If Spark session not initialized
            Exception: If query execution fails
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")

        try:
            print(f"Executing SQL: {sql[:100]}...")

            # Execute query
            df = self.spark.sql(sql)

            # Convert to list of dictionaries
            results = df.toPandas().to_dict('records')

            print(f"✅ Query executed successfully. Returned {len(results)} rows")

            return results

        except Exception as e:
            print(f"❌ Error executing query: {e}")
            raise Exception(f"Query execution failed: {str(e)}")

    def execute_with_dataframe(self, sql: str) -> DataFrame:
        """
        Execute SQL and return Spark DataFrame (for advanced processing)

        Args:
            sql: SQL query string

        Returns:
            Spark DataFrame

        Raises:
            RuntimeError: If Spark session not initialized
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")

        return self.spark.sql(sql)

    def verify_connection(self) -> bool:
        """
        Verify Spark connection by running a simple query

        Returns:
            True if connection successful, False otherwise
        """
        if not self.spark:
            return False

        try:
            # Try a simple query
            test_df = self.spark.sql("SHOW CATALOGS")
            catalogs = [row.catalog for row in test_df.collect()]

            # Check if our catalogs exist
            required_catalogs = ['iceberg', 'healthcare', 'healthcare_gold']
            missing = [c for c in required_catalogs if c not in catalogs]

            if missing:
                print(f"⚠️  Missing catalogs: {missing}")
                return False

            print(f"✅ Spark connection verified. Available catalogs: {catalogs}")
            return True

        except Exception as e:
            print(f"❌ Spark connection verification failed: {e}")
            return False

    def is_connected(self) -> bool:
        """
        Check if Spark session is active

        Returns:
            True if Spark session exists and is active
        """
        return self.spark is not None

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata about a specific table

        Args:
            table_name: Fully qualified table name (e.g., healthcare.claims_clean)

        Returns:
            Dictionary with table metadata
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")

        try:
            # Get table schema
            schema_df = self.spark.sql(f"DESCRIBE TABLE {table_name}")
            columns = [
                {"name": row.col_name, "type": row.data_type}
                for row in schema_df.collect()
            ]

            # Get row count (can be slow on large tables)
            # Commenting out for now - can be enabled if needed
            # count_df = self.spark.sql(f"SELECT COUNT(*) as count FROM {table_name}")
            # row_count = count_df.collect()[0].count

            return {
                "table_name": table_name,
                "columns": columns,
                # "row_count": row_count
            }

        except Exception as e:
            raise Exception(f"Error getting table info: {str(e)}")

    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()
            self.spark = None
            print("✅ Spark session closed")

    def __del__(self):
        """Cleanup on deletion"""
        self.close()
