#!/bin/bash
#
# Cleanup stale Iceberg catalog entries on container restart
# Prevents "Location does not exist" errors when S3 data is missing
#

echo "🧹 Checking Iceberg catalog for stale entries..."

# Wait for Postgres to be ready
until PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -c '\q' 2>/dev/null; do
  echo "⏳ Waiting for Postgres..."
  sleep 2
done

# Check if any Iceberg tables exist
TABLE_COUNT=$(PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -t -c "SELECT COUNT(*) FROM iceberg_tables;" 2>/dev/null | tr -d ' ')

if [ "$TABLE_COUNT" -gt 0 ]; then
  echo "⚠️  Found $TABLE_COUNT Iceberg tables in catalog"
  echo "   These may be stale if S3 data was cleared"
  echo ""
  echo "   To clean manually, run:"
  echo "   docker exec medflow-postgres psql -U airflow -d airflow -c \"DELETE FROM iceberg_tables;\""
  echo ""
  echo "   Or set CLEAN_ICEBERG_ON_STARTUP=true in .env to auto-clean"

  # Auto-clean if environment variable is set
  if [ "$CLEAN_ICEBERG_ON_STARTUP" = "true" ]; then
    echo "🧹 Auto-cleaning Iceberg catalog (CLEAN_ICEBERG_ON_STARTUP=true)..."
    PGPASSWORD=airflow psql -h postgres -U airflow -d airflow -c "DELETE FROM iceberg_tables;" 2>/dev/null
    echo "✅ Iceberg catalog cleaned"
  fi
else
  echo "✅ Iceberg catalog is clean (0 tables)"
fi
