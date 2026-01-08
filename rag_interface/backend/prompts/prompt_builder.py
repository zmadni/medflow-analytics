"""
Prompt Builder for RAG System
Constructs optimized prompts for LLM SQL generation

Author: Zeeshan Madni
Created: 2025-12-31
Updated: 2026-01-08 - Added ChromaDB vector store integration
"""

import json
import logging
from typing import Dict, List, Optional

# Configure logging
logger = logging.getLogger(__name__)

class PromptBuilder:
    """Builds prompts with table schemas and business rules for LLM"""

    def __init__(self, schema_loader, vector_store=None):
        """
        Initialize prompt builder

        Args:
            schema_loader: SchemaLoader instance with loaded schemas
            vector_store: Optional VectorStore instance for semantic retrieval
        """
        self.schema_loader = schema_loader
        self.vector_store = vector_store
        self.use_vector_store = vector_store is not None
        self.system_prompt = self._load_system_prompt()

        if self.use_vector_store:
            logger.info("✅ PromptBuilder initialized with ChromaDB vector store")
        else:
            logger.warning("⚠️ PromptBuilder initialized WITHOUT vector store (loading all schemas)")

    def _load_system_prompt(self) -> str:
        """Load base system prompt template"""
        return """You are an expert SQL assistant for a healthcare claims analytics system using Apache Iceberg tables.

Your job is to convert natural language questions into accurate SQL queries.

IMPORTANT RULES:
1. ALWAYS use fully-qualified table names (catalog.schema.table)
2. ALWAYS add LIMIT clause (default 100) unless user specifies otherwise or using aggregates
3. Use appropriate date formatting for year_month columns (YYYY-MM format)
4. Return ONLY the SQL query, no explanation or markdown formatting
5. Use standard SQL syntax compatible with Apache Spark SQL

TABLE SELECTION STRATEGY (CRITICAL):
🥇 PREFER GOLD LAYER for:
   - Monthly/time-based trends and summaries
   - Aggregated metrics (totals, averages, rates)
   - Pre-computed KPIs (approval rates, reimbursement rates)
   - Provider performance analysis
   - Approval funnel metrics
   ✅ Gold tables are PRE-AGGREGATED and MUCH FASTER

🥈 USE SILVER LAYER for:
   - Detailed claim-level queries with WHERE filters
   - Patient-specific or claim-specific lookups
   - Custom aggregations not available in Gold
   - Diagnosis/procedure code analysis at claim level

AVAILABLE TABLES:
{table_schemas}

BUSINESS RULES:
{business_rules}

EXAMPLE QUERIES:
{examples}

SQL GENERATION GUIDELINES:
- For monthly trends → USE iceberg.gold.claims_monthly_summary
- For approval funnel → USE iceberg.gold.claims_approval_funnel
- For provider metrics → USE iceberg.gold.provider_performance_metrics
- For claim details → USE iceberg.silver.claims
- Apply partition filters (payer_name, year_month) when possible for performance
- Use LIKE 'E11%' for diabetes, 'I10%' for hypertension (ICD-10 patterns)
- Round decimal results to 2 places for currency
- For TOP N queries, use ORDER BY + LIMIT
"""

    def build_prompt(self, question: str, include_examples: bool = True) -> str:
        """
        Build complete prompt with context for LLM

        Args:
            question: User's natural language question
            include_examples: Whether to include example queries

        Returns:
            Complete prompt string ready for LLM
        """
        # Get relevant table schemas (semantic search if vector store available)
        table_schemas = self._format_table_schemas(question)

        # Get business rules
        business_rules = self._format_business_rules()

        # Get example queries (optional, using similar queries from history)
        examples = ""
        if include_examples:
            examples = self._format_example_queries(question)

        # Build complete prompt
        system_section = self.system_prompt.format(
            table_schemas=table_schemas,
            business_rules=business_rules,
            examples=examples
        )

        user_section = f"\nUSER QUESTION: {question}\n\nSQL QUERY:"

        return system_section + user_section

    def _format_table_schemas(self, question: str = "") -> str:
        """
        Format table schemas for prompt.
        Uses vector store for semantic retrieval if available,
        otherwise loads all schemas.

        Args:
            question: User's question for semantic matching

        Returns:
            Formatted string describing available tables
        """
        # Decide which schemas to include
        if self.use_vector_store and question:
            # Use vector store to get top-k relevant schemas
            relevant_schemas = self.vector_store.get_relevant_schemas(
                query=question,
                n_results=3,  # Top 3 most relevant tables
                min_similarity=0.3  # Minimum similarity threshold
            )

            logger.info(f"📊 Vector store found {len(relevant_schemas)} relevant schemas")

            # Get full schema details for relevant tables
            schemas_to_format = {}
            for schema_info in relevant_schemas:
                table_name = schema_info["table_name"]
                table_data = self.schema_loader.get_schema(table_name)
                if table_data:
                    schemas_to_format[table_name] = table_data
                    logger.info(f"   - {table_name} (similarity: {schema_info['similarity']})")

        else:
            # Fallback: Load all schemas
            schemas_to_format = self.schema_loader.get_all_schemas()
            logger.info(f"📚 Loading all {len(schemas_to_format)} schemas (no vector store)")

        # Format the selected schemas
        formatted = []
        for table_name, table_info in schemas_to_format.items():
            # Table header
            formatted.append(f"\n--- {table_name} ({table_info['layer'].upper()} layer) ---")
            formatted.append(f"Description: {table_info['description']}")
            formatted.append(f"Estimated rows: {table_info.get('row_count_estimate', 'Unknown')}")

            # Columns
            formatted.append("\nColumns:")
            for col_name, col_info in table_info['columns'].items():
                col_type = col_info['type']
                col_desc = col_info['description']
                nullable = "NULL" if col_info.get('nullable', True) else "NOT NULL"
                formatted.append(f"  - {col_name} ({col_type}, {nullable}): {col_desc}")

                # Add examples if available
                if 'example' in col_info:
                    formatted.append(f"    Example: {col_info['example']}")

            # Partition info
            if table_info.get('partitions'):
                formatted.append(f"\nPartitions: {', '.join(table_info['partitions'])}")

        return "\n".join(formatted)

    def _format_business_rules(self) -> str:
        """
        Format business rules for prompt

        Returns:
            Formatted string describing business rules
        """
        schemas = self.schema_loader.get_all_schemas()

        formatted = []
        for table_name, table_info in schemas.items():
            if 'business_rules' in table_info:
                formatted.append(f"\n{table_name}:")
                for rule_name, rule_sql in table_info['business_rules'].items():
                    formatted.append(f"  - {rule_name}: {rule_sql}")

        return "\n".join(formatted) if formatted else "No specific business rules defined."

    def _format_example_queries(self, question: str, max_examples: int = 3) -> str:
        """
        Format relevant example queries based on question similarity.
        Uses vector store query history if available, otherwise uses static examples.

        Args:
            question: User's question for relevance matching
            max_examples: Maximum number of examples to include

        Returns:
            Formatted example queries
        """
        examples = []

        # Try to get similar queries from vector store history
        if self.use_vector_store:
            similar_queries = self.vector_store.get_similar_queries(
                query=question,
                n_results=max_examples,
                success_only=True  # Only include successful queries
            )

            if similar_queries:
                logger.info(f"🔍 Found {len(similar_queries)} similar queries from history")
                examples = [
                    {
                        "question": q["question"],
                        "sql": q["sql"]
                    }
                    for q in similar_queries
                ]

        # Fallback to static examples if no similar queries found
        if not examples:
            logger.info("📝 Using static example queries (no history available)")
            examples = [
                {
                    "question": "How many claims does BlueCross have?",
                    "sql": "SELECT COUNT(*) as total_claims FROM iceberg.silver.claims WHERE payer_name = 'BlueCross' LIMIT 1"
                },
                {
                    "question": "What are the monthly claim trends for 2025?",
                    "sql": "SELECT year_month, payer_name, total_claims, total_billed_amount, approval_rate FROM iceberg.gold.claims_monthly_summary WHERE year_month >= '2025-01' ORDER BY year_month, payer_name"
                },
                {
                    "question": "Show me top 10 providers by approval rate",
                    "sql": "SELECT provider_id, total_claims, approval_rate, avg_claim_amount FROM iceberg.gold.provider_performance_metrics WHERE total_claims >= 100 ORDER BY approval_rate DESC LIMIT 10"
                },
                {
                    "question": "What's the approval rate by payer and claim type?",
                    "sql": "SELECT payer_name, claim_type, approval_rate, total_submitted, approved_count FROM iceberg.gold.claims_approval_funnel ORDER BY payer_name, claim_type LIMIT 100"
                },
                {
                    "question": "Show me denied claims for Aetna",
                    "sql": "SELECT claim_id, patient_id, billed_amount, claim_date, diagnosis_code FROM iceberg.silver.claims WHERE payer_name = 'Aetna' AND claim_status = 'Denied' ORDER BY billed_amount DESC LIMIT 100"
                }
            ]

        # Format the examples
        formatted = ["\n--- Example Queries ---"]
        for i, example in enumerate(examples[:max_examples], 1):
            formatted.append(f"\nExample {i}:")
            formatted.append(f"Question: {example['question']}")
            formatted.append(f"SQL: {example['sql']}")

        return "\n".join(formatted)

    def build_simple_prompt(self, question: str) -> str:
        """
        Build simplified prompt for quick queries (no examples)

        Args:
            question: User's question

        Returns:
            Simplified prompt
        """
        return self.build_prompt(question, include_examples=False)

    def get_table_context(self, table_name: str) -> Optional[Dict]:
        """
        Get context for a specific table

        Args:
            table_name: Name of the table

        Returns:
            Table schema and metadata
        """
        return self.schema_loader.get_schema(table_name)
