# Deployment Checklist

## Scope

Use this checklist to deploy and validate the Salesforce reverse ETL orchestrator in a Databricks workspace.

## Prerequisites

- Databricks workspace access with bundle deployment permissions.
- Salesforce Connected App with OAuth credentials.
- Unity Catalog metastore and connection privileges.

## 1) Configure Unity Catalog connection

1. Open Databricks workspace.
2. Navigate to **Data** -> **Connections**.
3. Create or verify an HTTP connection for Salesforce.
4. Confirm OAuth settings and test connectivity.

Reference: `docs/UC_CONNECTION_SETUP.md`

## 2) Deploy bundle

```bash
cd ~/Documents/Demo/salesforce_operator_hackathon
databricks bundle deploy --profile dogfood
```

## 3) Execute validation jobs

```bash
databricks bundle run salesforce_upsert_operator --profile dogfood
databricks bundle run salesforce_insert_operator --profile dogfood
databricks bundle run salesforce_full_workflow --profile dogfood
```

## 4) Validate runtime behavior

- Confirm task execution succeeded in Databricks Workflows UI.
- Confirm expected Salesforce records were written.
- Confirm function/operator behavior is observable in logs and task state.

## 5) Post-deployment checks

- Verify no credentials are hardcoded in repository files.
- Confirm only UC connection IDs are referenced in runtime config.
- Record run links and operational notes in deployment ticket.
