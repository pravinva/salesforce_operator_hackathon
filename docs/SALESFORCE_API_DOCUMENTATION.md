# Salesforce Orchestrator API Documentation

## Overview

This document describes the public API surface for the Salesforce orchestrator used in the hackathon implementation.

Package: `salesforce_integration`  
Version: `1.0.1`

Exports:
- `salesforce_upsert`
- `salesforce_insert`
- `salesforce_update`
- `salesforce_delete`
- `SalesforceBulkWriteOperator`
- `SalesforceUpsertOperator`

---

## API Coverage Matrix

| Capability | API/Primitive | Coverage Status | Notes |
|---|---|---|---|
| Bulk upsert | `salesforce_upsert` | Covered | Supports `external_id_field`, optional wait |
| Bulk insert | `salesforce_insert` | Covered | Batch insert through Bulk API 2.0 |
| Bulk update | `salesforce_update` | Covered | Expects Salesforce `Id` in records |
| Bulk delete | `salesforce_delete` | Covered | Expects Salesforce `Id` in records |
| Generic bulk orchestration | `SalesforceBulkWriteOperator` | Covered | `insert/update/upsert/delete/hardDelete` |
| Upsert operator wrapper | `SalesforceUpsertOperator` | Covered | Convenience wrapper over generic operator |
| UC connection auth via proxy | internal request path | Covered | Uses `conn_id` + UC proxy |
| Connection base-path fallback | internal request path | Covered | Retries alternate path style on 404 |
| CSV LF line-ending compliance | internal serialization | Covered | Uses LF for Salesforce ingest compatibility |
| Task value propagation | functions/operators | Covered | Writes metadata best-effort to task values |
| Deferrable sensor for Salesforce | N/A in this repo | Not in scope | Intentionally removed for this implementation |

---

## Authentication Model

All API calls run through Databricks Unity Catalog Connection proxy endpoints.

Required runtime input:
- `conn_id` (for example: `salesforce_m2m_conn`)

No Salesforce credentials are embedded in source code.

---

## Functions API

All functions are designed for Databricks Python Operator task parameter binding.  
`records` is accepted as JSON string input and converted internally.

### `salesforce_upsert(object_name: str, external_id_field: str, records: str, conn_id: str, api_version: str = "v60.0", wait_for_completion: bool = False) -> dict`

Creates a Bulk API 2.0 upsert job, uploads records, and closes the job.

Returns:
- `job_id`
- `object`
- `operation`
- `state`
- `records_total`
- `api_version`
- `conn_id`
- plus `records_processed`/`records_failed` when `wait_for_completion=True`

---

### `salesforce_insert(object_name: str, records: str, conn_id: str, api_version: str = "v60.0", wait_for_completion: bool = False) -> dict`

Bulk insert flow for new records.

---

### `salesforce_update(object_name: str, records: str, conn_id: str, api_version: str = "v60.0", wait_for_completion: bool = False) -> dict`

Bulk update flow for existing records (records should include Salesforce `Id`).

---

### `salesforce_delete(object_name: str, records: str, conn_id: str, api_version: str = "v60.0", wait_for_completion: bool = False) -> dict`

Bulk delete flow for existing records (records should include Salesforce `Id`).

---

## Function Runtime Behavior

Common internal flow for all write functions:
1. Coerce and validate `records` payload
2. Convert records to CSV with Salesforce-compatible LF line endings
3. Create Bulk API ingest job
4. Upload batch CSV
5. Set job state to `UploadComplete`
6. Optionally poll to terminal state when `wait_for_completion=True`

Robustness features:
- UC proxy path fallback for host-only vs base_path-style connections
- best-effort task value writes (`salesforce_job_id`, `salesforce_object`, `salesforce_operation`)
- explicit error raising on create/upload/close failures

---

## Operators API

### `SalesforceBulkWriteOperator`

Full lifecycle operator for external orchestration:
- `open()` -> creates and submits Salesforce Bulk job
- `poll()` -> checks job state and defers between checks
- `close()` -> attempts cleanup/abort for non-terminal jobs

Supported operations:
- `insert`
- `update`
- `upsert`
- `delete`
- `hardDelete`

Constructor parameters:
- `object_name: str`
- `operation: str`
- `records: str`
- `conn_id: str`
- `task_key: str`
- `external_id_field: str = ""`
- `api_version: str = "v60.0"`
- `poll_interval_minutes: int = 1`

Poll outcomes:
- `JobComplete` -> `SensorResult.completed()`
- `Failed` / `Aborted` -> raises exception
- transient states -> `SensorResult.deferred(...)`

---

### `SalesforceUpsertOperator`

Convenience wrapper over `SalesforceBulkWriteOperator` with fixed `operation="upsert"`.

Constructor:
- `object_name`
- `external_id_field`
- `records`
- `conn_id`
- `task_key`
- `api_version`
- `poll_interval_minutes`

---

## Databricks Task Usage Examples

### Function task example
```yaml
python_operator_task:
  main: salesforce_integration.salesforce_upsert
  parameters:
    - name: object_name
      value: "Account"
    - name: external_id_field
      value: "Account_Number_External__c"
    - name: records
      value: '[{"Account_Number_External__c":"A001","Name":"Acme"}]'
    - name: conn_id
      value: "salesforce_m2m_conn"
    - name: wait_for_completion
      value: "true"
```

### Operator task example
```yaml
python_operator_task:
  main: salesforce_integration.SalesforceBulkWriteOperator
  parameters:
    - name: object_name
      value: "Account"
    - name: operation
      value: "upsert"
    - name: external_id_field
      value: "Account_Number_External__c"
    - name: records
      value: '[{"Account_Number_External__c":"A001","Name":"Acme"}]'
    - name: conn_id
      value: "salesforce_m2m_conn"
    - name: task_key
      value: "upsert_with_operator"
```

---

## Error Model

Failures surface as Python exceptions with contextual messages, including:
- UC proxy request failures (status + response text)
- validation failures for payload/schema
- terminal Salesforce job failures (`Failed`, `Aborted`)

---

## Deployment and Invocation

```bash
python3 setup.py bdist_wheel
databricks bundle validate --profile dogfood
databricks bundle deploy --profile dogfood

# Function workflow
databricks bundle run salesforce_full_workflow --profile dogfood

# Operator workflow
databricks bundle run salesforce_upsert_operator --profile dogfood
```

---

## Notes

- This implementation is function/operator only (no Salesforce sensor in repo scope).
- Designed for hackathon demo plus production-oriented extension.
