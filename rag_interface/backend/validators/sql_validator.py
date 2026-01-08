"""
SQL Validator for RAG System
Validates generated SQL for safety and correctness

Author: Zeeshan Madni
Created: 2025-12-31
"""

import re
import sqlparse
from typing import Dict, List, Tuple

class SQLValidator:
    """Validates SQL queries for safety, syntax, and performance"""

    # Forbidden SQL keywords that could be destructive
    FORBIDDEN_KEYWORDS = [
        'DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT',
        'UPDATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE'
    ]

    # Allowed tables (whitelist approach)
    ALLOWED_TABLES = [
        # Silver layer tables (primary data source)
        'iceberg.silver.claims',
        'iceberg.silver.claims_quarantine',
        'iceberg.silver.processing_log',
        # Bronze layer tables
        'iceberg.bronze.claims_raw',
        # Gold layer tables
        'iceberg.gold.claims_monthly_summary',
        'iceberg.gold.claims_approval_funnel',
        'iceberg.gold.provider_performance_metrics',
        # Legacy healthcare catalog tables (kept for backward compatibility)
        'healthcare.claims_raw',
        'healthcare.claims_clean',
        'healthcare.claims_quarantined',
        'healthcare.data_quality_log',
        'healthcare_gold.claims_monthly_summary',
        'healthcare_gold.claims_approval_funnel',
        'healthcare_gold.provider_performance_metrics'
    ]

    def __init__(self):
        """Initialize SQL validator"""
        pass

    def validate(self, sql: str) -> Dict:
        """
        Comprehensive SQL validation

        Args:
            sql: SQL query string to validate

        Returns:
            Dict with validation results:
            {
                "is_valid": bool,
                "sql": str (potentially modified with LIMIT),
                "issues": List[str],
                "warnings": List[str],
                "suggestions": List[str]
            }
        """
        issues = []
        warnings = []
        suggestions = []

        # Clean SQL
        sql = sql.strip()

        # Remove markdown code blocks if present
        sql = self._remove_markdown(sql)

        # Validation checks
        safety_ok, safety_issues = self._check_safety(sql)
        if not safety_ok:
            issues.extend(safety_issues)

        syntax_ok, syntax_issues = self._check_syntax(sql)
        if not syntax_ok:
            issues.extend(syntax_issues)

        table_ok, table_issues = self._check_tables(sql)
        if not table_ok:
            issues.extend(table_issues)

        # Performance checks (warnings only)
        perf_warnings = self._check_performance(sql)
        warnings.extend(perf_warnings)

        # Add LIMIT if missing (auto-fix)
        sql, limit_added = self._add_limit_if_missing(sql)
        if limit_added:
            suggestions.append("Added LIMIT 100 to prevent large result sets")

        # Overall validation result
        is_valid = len(issues) == 0

        return {
            "is_valid": is_valid,
            "sql": sql,
            "issues": issues,
            "warnings": warnings,
            "suggestions": suggestions
        }

    def _remove_markdown(self, sql: str) -> str:
        """Remove markdown code blocks if present"""
        # Remove ```sql ... ```
        sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```\s*$', '', sql)
        return sql.strip()

    def _check_safety(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Check for dangerous SQL operations

        Returns:
            (is_safe, list_of_issues)
        """
        issues = []
        sql_upper = sql.upper()

        # Check for forbidden keywords
        for keyword in self.FORBIDDEN_KEYWORDS:
            if re.search(r'\b' + keyword + r'\b', sql_upper):
                issues.append(f"Forbidden SQL operation: {keyword}")

        # Ensure it's a SELECT statement
        if not sql_upper.strip().startswith('SELECT'):
            issues.append("Only SELECT queries are allowed")

        # Check for SQL injection patterns
        if self._contains_injection_patterns(sql):
            issues.append("Potential SQL injection detected")

        return len(issues) == 0, issues

    def _check_syntax(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Check SQL syntax validity

        Returns:
            (is_valid, list_of_issues)
        """
        issues = []

        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)

            if not parsed:
                issues.append("Could not parse SQL query")
                return False, issues

            # Check for multiple statements (should only be one SELECT)
            if len(parsed) > 1:
                issues.append("Multiple SQL statements not allowed")

            # Basic syntax validation
            statement = parsed[0]
            if not statement.tokens:
                issues.append("Empty SQL statement")

        except Exception as e:
            issues.append(f"Syntax error: {str(e)}")

        return len(issues) == 0, issues

    def _check_tables(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Verify that only allowed tables are referenced

        Returns:
            (is_valid, list_of_issues)
        """
        issues = []

        # Extract table names from SQL
        # Simple regex approach (can be enhanced)
        table_pattern = r'\bFROM\s+([a-zA-Z0-9_\.]+)|JOIN\s+([a-zA-Z0-9_\.]+)'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)

        referenced_tables = set()
        for match in matches:
            # match is tuple (from_table, join_table)
            table = match[0] if match[0] else match[1]
            if table:
                referenced_tables.add(table.lower())

        # Check if all tables are allowed
        for table in referenced_tables:
            if table not in [t.lower() for t in self.ALLOWED_TABLES]:
                issues.append(f"Table not allowed: {table}")

        return len(issues) == 0, issues

    def _check_performance(self, sql: str) -> List[str]:
        """
        Check for potential performance issues

        Returns:
            List of warnings
        """
        warnings = []
        sql_upper = sql.upper()

        # Check for missing WHERE clause on large tables
        if 'FROM HEALTHCARE.CLAIMS_CLEAN' in sql_upper or 'FROM HEALTHCARE.CLAIMS_RAW' in sql_upper:
            if 'WHERE' not in sql_upper:
                warnings.append("Consider adding WHERE clause for better performance on large tables")

        # Check for SELECT *
        if re.search(r'SELECT\s+\*', sql_upper):
            warnings.append("Using SELECT * may return unnecessary columns. Consider specifying columns.")

        # Check for CROSS JOIN
        if 'CROSS JOIN' in sql_upper:
            warnings.append("CROSS JOIN can be very expensive. Ensure this is intentional.")

        # Check for missing LIMIT
        if 'LIMIT' not in sql_upper and 'COUNT(' not in sql_upper:
            warnings.append("No LIMIT clause found. Query may return large result set.")

        return warnings

    def _add_limit_if_missing(self, sql: str, default_limit: int = 100) -> Tuple[str, bool]:
        """
        Add LIMIT clause if missing and not an aggregation

        Args:
            sql: SQL query
            default_limit: Default limit to add

        Returns:
            (modified_sql, limit_was_added)
        """
        sql_upper = sql.upper()

        # Don't add LIMIT if already present
        if 'LIMIT' in sql_upper:
            return sql, False

        # Don't add LIMIT for pure aggregations (single row results)
        if self._is_pure_aggregation(sql):
            return sql, False

        # Add LIMIT
        sql = sql.rstrip(';').rstrip()
        sql = f"{sql} LIMIT {default_limit}"

        return sql, True

    def _is_pure_aggregation(self, sql: str) -> bool:
        """Check if query is a pure aggregation (returns single row)"""
        sql_upper = sql.upper()

        # Has aggregation functions and no GROUP BY
        has_aggregation = any(func in sql_upper for func in ['COUNT(', 'SUM(', 'AVG(', 'MAX(', 'MIN('])
        has_group_by = 'GROUP BY' in sql_upper

        # Pure aggregation if has aggregate functions but no GROUP BY
        return has_aggregation and not has_group_by

    def _contains_injection_patterns(self, sql: str) -> bool:
        """
        Check for common SQL injection patterns

        Returns:
            True if potential injection detected
        """
        injection_patterns = [
            r';\s*DROP',
            r';\s*DELETE',
            r';\s*INSERT',
            r';\s*UPDATE',
            r'--\s*',  # SQL comments
            r'/\*.*\*/',  # Multi-line comments
            r'UNION\s+SELECT',  # Union-based injection
            r'OR\s+1\s*=\s*1',  # Always true conditions
            r'OR\s+\'.*\'\s*=\s*\'.*\''
        ]

        for pattern in injection_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                return True

        return False

    def validate_and_fix(self, sql: str) -> Dict:
        """
        Validate and attempt to auto-fix common issues

        Args:
            sql: SQL query to validate and fix

        Returns:
            Validation result with potentially fixed SQL
        """
        result = self.validate(sql)

        # If invalid, try to fix common issues
        if not result["is_valid"]:
            # Try removing markdown
            if "```" in sql:
                sql = self._remove_markdown(sql)
                result = self.validate(sql)

        return result
