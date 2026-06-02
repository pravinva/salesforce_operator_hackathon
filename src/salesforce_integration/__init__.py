"""
Salesforce Integration for Databricks Lakeflow Jobs External Orchestration.

This package provides Functions and Operators for integrating
Salesforce with Databricks Workflows using the new Python operator framework.

Supports:
- Salesforce Bulk API 2.0 (batch operations)
- Salesforce Collections API (real-time sync)
- Unity Catalog Connections for authentication
"""

from salesforce_integration.functions import (
    salesforce_upsert,
    salesforce_insert,
    salesforce_update,
    salesforce_delete,
)

__version__ = "1.0.1"

__all__ = [
    "salesforce_upsert",
    "salesforce_insert",
    "salesforce_update",
    "salesforce_delete",
    "SalesforceBulkWriteOperator",
    "SalesforceUpsertOperator",
]


def __getattr__(name: str):
    # Lazy-load operator classes so function tasks do not require
    # python_operator_task unless those symbols are actually used.
    if name == "SalesforceBulkWriteOperator":
        from salesforce_integration.operators import SalesforceBulkWriteOperator

        return SalesforceBulkWriteOperator
    if name == "SalesforceUpsertOperator":
        from salesforce_integration.operators import SalesforceUpsertOperator

        return SalesforceUpsertOperator
    raise AttributeError(f"module 'salesforce_integration' has no attribute '{name}'")
