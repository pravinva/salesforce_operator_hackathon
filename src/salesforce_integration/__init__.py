"""
Salesforce Integration for Databricks Lakeflow Jobs External Orchestration.

This package provides Functions, Sensors, and Operators for integrating
Salesforce with Databricks Workflows using the new Python operator framework.

Supports:
- Salesforce Bulk API 2.0 (batch operations)
- Salesforce Collections API (real-time sync)
- Unity Catalog Connections for authentication
- Deferrable sensor execution for cost efficiency
"""

from salesforce_integration.functions import (
    salesforce_upsert,
    salesforce_insert,
    salesforce_update,
    salesforce_delete,
)

from salesforce_integration.sensors import (
    SalesforceBulkJobSensor,
)

from salesforce_integration.operators import (
    SalesforceBulkWriteOperator,
    SalesforceUpsertOperator,
)

__version__ = "1.0.0"

__all__ = [
    "salesforce_upsert",
    "salesforce_insert",
    "salesforce_update",
    "salesforce_delete",
    "SalesforceBulkJobSensor",
    "SalesforceBulkWriteOperator",
    "SalesforceUpsertOperator",
]
