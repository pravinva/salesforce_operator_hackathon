# 5-Minute Notebook Demo Talk Track

## 0:00-0:30 - Problem + Goal

- **Say:** "I'm showing how we run Salesforce writes in Databricks using Python functions and operators, with M2M auth through Unity Catalog connections - no credentials in code."
- **Show:** Notebook title (`Build, Deploy and Run using SDK`) and quick glance at the job list it triggers.

## 0:30-1:15 - Architecture in 20 seconds

- **Say:** "This notebook triggers two jobs: a full workflow (function-based upserts) and a bulk write operator upsert. Both call Salesforce Bulk API 2.0 via UC HTTP proxy."
- **Show:** The cell where job suffixes are set:
  - `[hackathon] Salesforce Full Workflow`
  - `[hackathon] Salesforce Bulk Write Operator (Upsert)`
- **Say:** "External IDs make reruns idempotent."

## 1:15-2:15 - Live run from notebook

- **Say:** "Now I run the notebook end-to-end."
- **Show:** Execute the run cell and keep output visible:
  - job lookup
  - run IDs and URLs
  - terminal states (`RUNNING` -> `SUCCESS`)
- **Narrate while it runs:** "This is real execution, not screenshots. The notebook waits for completion and fails fast on errors."

## 2:15-3:20 - Show Salesforce updates (Developer Console)

- **Switch screen to Salesforce Developer Console -> Query Editor** (`Use Tooling API` unchecked).

- **Run this query (Accounts):**

```sql
SELECT Id, Name, Account_Number_External__c, AnnualRevenue, LastModifiedDate, FORMAT(LastModifiedDate)
FROM Account
WHERE Account_Number_External__c IN ('WF001','WF002','WF003','OPS001','OPS002')
ORDER BY LastModifiedDate DESC
```

- **Say:** "`FORMAT(LastModifiedDate)` shows local time. You can see records touched by this run."

- **Run this query (Contacts):**

```sql
SELECT Id, FirstName, LastName, Email, LastModifiedDate, FORMAT(LastModifiedDate)
FROM Contact
WHERE Email IN ('john@wf001.com','jane@wf002.com')
ORDER BY LastModifiedDate DESC
```

- **Say:** "Contacts were upserted by the workflow task using Email as external key."

## 3:20-4:15 - Prove rerun behavior (idempotency)

- **Say:** "Now I rerun the notebook to show no duplicate creation."
- **Show:** Re-run notebook trigger cell quickly.
- **Return to Developer Console**, re-run the same queries.
- **Say:** "Same external IDs, newer `LastModifiedDate` - this is idempotent upsert behavior."

## 4:15-5:00 - Close with value

- **Say:**
  - "We replaced scheduler-specific logic with Databricks-native functions/operators."
  - "Authentication is centralized in UC M2M connection."
  - "The workflow is production-aligned: observable runs, retry-ready, and safe reruns through external IDs."
- **Show:** Notebook final success output and one Databricks run URL briefly.

---

## One-line backup narration (if asked "what happened?")

"The notebook invoked two Salesforce jobs, both completed successfully, and Salesforce records for `WF*` and `OPS*` keys were inserted/updated with local-time `LastModifiedDate` proof in Developer Console."
