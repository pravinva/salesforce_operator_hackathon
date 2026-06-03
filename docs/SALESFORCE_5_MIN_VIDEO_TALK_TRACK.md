# Salesforce Functions + Operators Hackathon Demo  
## 5-Minute Word-for-Word Talk Track

Use this script while recording.  
Each section includes what to **say** and what to **show**.

---

## 0:00 - 0:30 | Opening: Problem and Promise

### Say
"Hi, this is a quick demo of our Salesforce orchestrator for the hackathon.  
The problem we are solving is that Salesforce reverse ETL workflows often require repeated custom code for auth, batching, polling, retries, and cleanup.  
Our solution is a Databricks-native implementation that gives reusable **functions** and **operators** so teams can ship faster with consistent runtime behavior."

### Show
- Open `README.md`
- Keep project tree visible (`src/salesforce_integration`, `databricks.yml`, `docs`)

---

## 0:30 - 1:15 | Scope of What We Built

### Say
"The scope is intentionally focused and practical.  
On the function side we provide `salesforce_insert`, `salesforce_upsert`, `salesforce_update`, and `salesforce_delete`.  
On the operator side we provide `SalesforceBulkWriteOperator` and `SalesforceUpsertOperator`.  
Authentication is done through Unity Catalog connections, so there are no Salesforce credentials hardcoded in task code."

### Show
- Open `src/salesforce_integration/functions.py` and point at function definitions
- Open `src/salesforce_integration/operators.py` and point at class definitions
- Briefly highlight `conn_id` parameter usage

---

## 1:15 - 2:00 | Functions vs Operators

### Say
"Here’s the practical difference.  
Functions are ideal when you want straightforward task execution from input to completion.  
Operators are ideal when you want explicit orchestration lifecycle semantics: `open` to create external work, `poll` to monitor progress, and `close` for cleanup behavior.  
This gives us both simplicity and advanced control in the same plugin."

### Show
- Open `databricks.yml`
- Highlight `salesforce_full_workflow` job (function path)
- Highlight `salesforce_upsert_operator` and `salesforce_insert_operator` jobs (operator path)

---

## 2:00 - 3:10 | Function Path Demo

### Say
"Now I’ll show the function workflow run.  
This demonstrates that function-based orchestration works end to end for Salesforce writes.  
What matters here is that teams consume a standard primitive instead of rebuilding API lifecycle logic in each pipeline."

### Show
- Open function job link:  
  `https://e2-dogfood.staging.cloud.databricks.com/jobs/223154473806435?o=6051921418418893`
- Open latest successful run
- Click task output/logs and show:
  - records submitted
  - job progress/state
  - terminal success

### Optional line while logs are on screen
"You can see this task is calling a reusable function contract, not custom per-workflow script logic."

---

## 3:10 - 4:15 | Operator Path Demo

### Say
"Next is the operator workflow.  
This is the same business outcome, but with explicit lifecycle orchestration semantics.  
This is especially useful when migrating from custom scheduler operators and when teams need standardized external job control behavior."

### Show
- Open upsert operator job link:  
  `https://e2-dogfood.staging.cloud.databricks.com/jobs/13271629834262?o=6051921418418893`
- Open latest successful run
- Show task output/logs and terminal success

### Optional line while logs are on screen
"This gives us a clean operator abstraction instead of hand-rolled orchestration code in every DAG."

---

## 4:15 - 4:45 | Migration Story

### Say
"We also documented a concrete migration path from Airflow-style orchestration.  
In simple terms: Airflow Python task maps to Databricks function task, and Airflow custom operator pattern maps to Databricks operator class.  
So migration is incremental and practical, not a full replatform rewrite."

### Show
- Open `docs/SALESFORCE_MIGRATION_GUIDE.md` (or PDF)
- Briefly scroll concept mapping and deployment section

---

## 4:45 - 5:00 | Close

### Say
"To summarize, this hackathon deliverable provides a production-oriented Salesforce orchestration foundation on Databricks with reusable functions, reusable operators, and a clear migration path from legacy orchestration patterns.  
Next steps are expanding object-specific templates and adding more packaging automation while keeping the same API contract."

### Show
- Open `docs/SALESFORCE_API_DOCUMENTATION.md` (or PDF)
- End on API coverage matrix and successful run status tabs

---

## Backup One-Liners (If You Need to Fill Time)

- "The key value is not just API calls; it is standardizing orchestration behavior."
- "We intentionally removed Salesforce sensor scope here and focused on function plus operator coverage."
- "All authentication is handled through the UC connection boundary, which keeps secrets out of pipeline code."
- "Both function and operator workflows were validated successfully in the target workspace."
