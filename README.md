# Salesforce Reverse ETL Task Orchestrator

## Why this orchestrator is needed

Enterprise Salesforce reverse ETL programs often begin as custom scripts running in scheduled jobs. That approach is functional, but it creates long-term operational overhead:

- repeated OAuth, polling, and retry logic across workflows
- inconsistent error behavior and observability between teams
- tight coupling between integration code and scheduler-specific constructs
- expensive migration effort from legacy Airflow operator patterns

This repository provides a Databricks-native orchestration model for Salesforce Bulk API write workflows using the Python Operator task framework.

## Solution overview

The implementation is packaged as a Databricks Asset Bundle with reusable integration primitives:

- **Functions** for one-shot write operations
- **Sensors** for deferred completion checks
- **Operators** for lifecycle orchestration (`open`, `poll`, `close`)
- **Unity Catalog HTTP connections** for authentication without raw secrets in task code

## Repository structure

- `src/salesforce_integration/`: Core integration package (functions, sensors, operators)
- `databricks.yml`: Primary Databricks Asset Bundle definition
- `docs/`: Runbooks, setup guides, and archived context
- `scripts/`: Operational helper scripts grouped by integration domain
- `examples/`: Migration and sample workflow artifacts
- `tests/manual/`: Manual validation scripts for direct API checks

## Functions

Independent function entry points are available under `salesforce_integration`:

- `salesforce_upsert`
- `salesforce_insert`
- `salesforce_update`
- `salesforce_delete`

These are callable independently via `python_operator_task.main` in any Databricks job task definition.

Example:

```yaml
python_operator_task:
  main: salesforce_integration.salesforce_insert
  parameters:
    - name: object_name
      value: "Account"
    - name: records
      value: '[{"Name":"Account A"}]'
    - name: conn_id
      value: "salesforce_m2m_conn"
```

## Operators

Operator primitives are designed for explicit lifecycle control and external job orchestration:

- `SalesforceBulkWriteOperator`
- `SalesforceUpsertOperator`

Lifecycle responsibilities:

- `open()`: create and submit Bulk API jobs
- `poll()`: check terminal status and defer between checks
- `close()`: cleanup hook for cancellation/termination scenarios

## Sensors

`SalesforceBulkJobSensor` supports deferred execution and state polling for submitted Bulk API jobs. This allows long-running waits without holding compute continuously.

Use this pattern when you want to split write submission and completion monitoring into distinct tasks.

## Airflow migration model

This solution supports straightforward migration from Airflow task patterns:

- Airflow `PythonOperator` write call -> Databricks function task
- Airflow polling sensor -> Databricks deferred sensor task
- Airflow custom operator -> Databricks Python operator lifecycle class

Migration objective:

- reduce orchestration glue code
- standardize authentication through UC connections
- keep runtime semantics in Databricks Workflows

## Deployment and execution

Deploy bundle:

```bash
databricks bundle deploy --profile dogfood
```

Run representative jobs:

```bash
databricks bundle run salesforce_upsert_with_sensor --profile dogfood
databricks bundle run salesforce_upsert_operator --profile dogfood
databricks bundle run salesforce_insert_operator --profile dogfood
```

## Runtime customization

`databricks.yml` exposes runtime configuration via variables, including:

- `salesforce_conn_id`
- `salesforce_object`
- `salesforce_external_id`

These can be overridden per target or per run for environment-specific execution without code changes.

## Security and governance

Authentication is routed through Unity Catalog HTTP connections. Integration code does not require embedded Salesforce usernames, passwords, or tokens.

This supports centralized credential governance and clearer operational separation of concerns.

## License

Apache 2.0
