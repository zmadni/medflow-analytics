"""
Schema Loader Utility
Loads and manages table schemas and metadata for RAG system

Author: Zeeshan Madni
Created: 2025-12-31
"""

import json
import os
from typing import Dict, Optional, List

class SchemaLoader:
    """Loads and caches table schemas from JSON files"""

    def __init__(self, schemas_path: str = "schemas/iceberg_schemas.json"):
        """
        Initialize schema loader

        Args:
            schemas_path: Path to schemas JSON file
        """
        self.schemas_path = schemas_path
        self.schemas: Dict = {}
        self.loaded = False

    def load_schemas(self) -> Dict:
        """
        Load table schemas from JSON file

        Returns:
            Dictionary of all table schemas

        Raises:
            FileNotFoundError: If schemas file doesn't exist
            json.JSONDecodeError: If schemas file is invalid JSON
        """
        # Get absolute path relative to project root
        base_path = os.getenv("RAG_BASE_PATH", "/app")
        full_path = os.path.join(base_path, self.schemas_path)

        if not os.path.exists(full_path):
            raise FileNotFoundError(f"Schemas file not found: {full_path}")

        try:
            with open(full_path, 'r') as f:
                data = json.load(f)

            self.schemas = data.get('tables', {})
            self.loaded = True

            print(f"✅ Loaded {len(self.schemas)} table schemas from {full_path}")

            return self.schemas

        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Invalid JSON in schemas file: {str(e)}",
                e.doc, e.pos
            )

    def get_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get schema for a specific table

        Args:
            table_name: Fully qualified table name (e.g., healthcare.claims_clean)

        Returns:
            Table schema dictionary or None if not found
        """
        if not self.loaded:
            self.load_schemas()

        return self.schemas.get(table_name)

    def get_all_schemas(self) -> Dict:
        """
        Get all loaded table schemas

        Returns:
            Dictionary of all schemas
        """
        if not self.loaded:
            self.load_schemas()

        return self.schemas

    def get_table_names(self) -> List[str]:
        """
        Get list of all available table names

        Returns:
            List of table names
        """
        if not self.loaded:
            self.load_schemas()

        return list(self.schemas.keys())

    def get_tables_by_layer(self, layer: str) -> Dict:
        """
        Get tables for a specific medallion layer

        Args:
            layer: Layer name (bronze, silver, gold)

        Returns:
            Dictionary of tables in that layer
        """
        if not self.loaded:
            self.load_schemas()

        return {
            name: schema
            for name, schema in self.schemas.items()
            if schema.get('layer', '').lower() == layer.lower()
        }

    def get_column_info(self, table_name: str, column_name: str) -> Optional[Dict]:
        """
        Get information about a specific column

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            Column info dictionary or None
        """
        schema = self.get_schema(table_name)
        if not schema:
            return None

        columns = schema.get('columns', {})
        return columns.get(column_name)

    def get_business_rules(self, table_name: str) -> Optional[Dict]:
        """
        Get business rules for a specific table

        Args:
            table_name: Table name

        Returns:
            Dictionary of business rules or None
        """
        schema = self.get_schema(table_name)
        if not schema:
            return None

        return schema.get('business_rules')

    def is_loaded(self) -> bool:
        """
        Check if schemas are loaded

        Returns:
            True if schemas loaded, False otherwise
        """
        return self.loaded

    def reload(self):
        """Force reload of schemas from file"""
        self.loaded = False
        return self.load_schemas()

    def get_schema_summary(self) -> Dict:
        """
        Get summary statistics about loaded schemas

        Returns:
            Dictionary with summary info
        """
        if not self.loaded:
            self.load_schemas()

        layers = {}
        total_columns = 0

        for table_name, schema in self.schemas.items():
            layer = schema.get('layer', 'unknown')
            layers[layer] = layers.get(layer, 0) + 1
            total_columns += len(schema.get('columns', {}))

        return {
            "total_tables": len(self.schemas),
            "tables_by_layer": layers,
            "total_columns": total_columns,
            "table_names": list(self.schemas.keys())
        }
