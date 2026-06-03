"""
Salesforce Operators for Databricks Python Operator Framework.

Operators provide full lifecycle management: open (start work), poll (check status),
and close (cleanup on cancellation).

Note: OperatorV0 is currently available. close() is NOT yet called on cancellation
in the current version, so external jobs may leak.
"""

import datetime
import json
from typing import Dict, Any, List, Optional

import requests
from databricks.sdk import WorkspaceClient
from python_operator_task import OperatorV0, SensorResult
try:
    from databricks.sdk.runtime import dbutils  # type: ignore
except Exception:  # pragma: no cover - runtime availability differs by execution context
    dbutils = None

from salesforce_integration.functions import _records_to_csv


class SalesforceBulkWriteOperator(OperatorV0):
    """
    Full-lifecycle operator for Salesforce Bulk API 2.0 write operations.

    Supports: insert, update, upsert, delete, hardDelete.
    For upsert, external_id_field is required.
    """

    SUPPORTED_OPERATIONS = {"insert", "update", "upsert", "delete", "hardDelete"}
    TERMINAL_STATES = {"JobComplete", "Failed", "Aborted"}
    FAILURE_STATES = {"Failed", "Aborted"}

    def __init__(
        self,
        object_name: str,
        operation: str,
        records: str,
        conn_id: str,
        task_key: str,
        external_id_field: str = "",
        api_version: str = "v60.0",
        poll_interval_minutes: int = 1,
    ):
        self.object_name = object_name
        self.operation = operation
        self.records = self._coerce_records(records)
        self.conn_id = conn_id
        self.task_key = task_key
        self.external_id_field = external_id_field
        self.api_version = api_version
        self.poll_interval_minutes = int(poll_interval_minutes)

        self._validate_inputs()

        self.w = WorkspaceClient()
        self.proxy_base_url = (
            f"{self.w.config.host}/api/2.0/unity-catalog/connections/"
            f"{self.conn_id}/proxy"
        )
        self.job_id = ""

    def _set_task_value(self, key: str, value: str) -> None:
        if dbutils is None:
            return
        try:
            dbutils.jobs.taskValues.set(key, value)
        except Exception:
            pass

    def _get_task_value(self, key: str, default: str = "") -> str:
        if dbutils is None:
            return default
        try:
            return dbutils.jobs.taskValues.get(self.task_key, key, default=default)
        except Exception:
            return default

    @staticmethod
    def _coerce_records(records: Any) -> List[Dict[str, Any]]:
        """Parse records passed as list[dict] or JSON string."""
        if isinstance(records, str):
            parsed = json.loads(records)
            if not isinstance(parsed, list):
                raise ValueError("records JSON must decode to a list of objects")
            records = parsed
        if not isinstance(records, list):
            raise ValueError("records must be a list of objects")
        for idx, item in enumerate(records):
            if not isinstance(item, dict):
                raise ValueError(
                    f"records[{idx}] must be an object, got {type(item).__name__}"
                )
        return records

    def _request_salesforce(
        self,
        method: str,
        resource_path: str,
        headers: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Send a request to Salesforce via UC proxy.

        Supports both connection styles:
        1) Connection host only (no base_path): use /services/data/{api_version}/...
        2) Connection with base_path already set: use /...
        """
        candidate_paths = [
            f"/services/data/{self.api_version}{resource_path}",
            resource_path,
        ]
        auth_headers = self.w.config.authenticate()
        merged_headers = {
            **auth_headers,
            "Accept": "application/json",
            "Accept-Encoding": "identity",
        }
        if headers:
            merged_headers.update(headers)

        last_response: Optional[requests.Response] = None
        for idx, candidate_path in enumerate(candidate_paths):
            for attempt in range(2):
                auth_headers = self.w.config.authenticate()
                merged_headers = {
                    **auth_headers,
                    "Accept": "application/json",
                    "Accept-Encoding": "identity",
                }
                if headers:
                    merged_headers.update(headers)
                response = requests.request(
                    method,
                    f"{self.proxy_base_url}{candidate_path}",
                    headers=merged_headers,
                    **kwargs,
                )
                if (
                    attempt == 0
                    and response.status_code == 401
                    and "INVALID_SESSION_ID" in response.text
                ):
                    print(
                        "[SalesforceBulkWriteOperator] received INVALID_SESSION_ID; "
                        "retrying request once..."
                    )
                    continue
                break
            last_response = response
            should_fallback = (
                idx == 0
                and response.status_code == 404
                and "NOT_FOUND" in response.text
            )
            if not should_fallback:
                return response

            print(
                "[SalesforceBulkWriteOperator] First path style returned 404; "
                "retrying with connection base_path..."
            )

        return last_response  # type: ignore[return-value]

    def _validate_inputs(self) -> None:
        if self.operation not in self.SUPPORTED_OPERATIONS:
            supported = ", ".join(sorted(self.SUPPORTED_OPERATIONS))
            raise ValueError(
                f"Unsupported operation '{self.operation}'. Supported: {supported}"
            )
        if self.operation == "upsert" and not self.external_id_field:
            raise ValueError("external_id_field is required for upsert operation")
        if not self.records:
            raise ValueError("records cannot be empty")

    def open(self):
        """Create a Salesforce ingest job, upload data, and submit the job."""
        print(
            f"[SalesforceBulkWriteOperator] Starting {self.operation} job "
            f"for {self.object_name}"
        )

        csv_data = _records_to_csv(self.records)

        payload = {
            "object": self.object_name,
            "operation": self.operation,
        }
        if self.external_id_field:
            payload["externalIdFieldName"] = self.external_id_field

        create_job_response = self._request_salesforce(
            "POST",
            "/jobs/ingest",
            headers={"Content-Type": "application/json"},
            json=payload,
        )
        if create_job_response.status_code != 200:
            raise Exception(
                f"Failed to create job: {create_job_response.status_code} "
                f"{create_job_response.text}"
            )

        job_data = create_job_response.json()
        job_id = job_data["id"]
        self.job_id = job_id
        print(f"[SalesforceBulkWriteOperator] Job created: {job_id}")

        self._set_task_value("salesforce_job_id", job_id)
        self._set_task_value("salesforce_operation", self.operation)
        self._set_task_value("salesforce_object", self.object_name)

        upload_response = self._request_salesforce(
            "PUT",
            f"/jobs/ingest/{job_id}/batches",
            headers={"Content-Type": "text/csv"},
            data=csv_data,
        )
        if upload_response.status_code not in [200, 201]:
            raise Exception(
                f"Failed to upload data: {upload_response.status_code} "
                f"{upload_response.text}"
            )

        close_job_response = self._request_salesforce(
            "PATCH",
            f"/jobs/ingest/{job_id}",
            headers={"Content-Type": "application/json"},
            json={"state": "UploadComplete"},
        )
        if close_job_response.status_code != 200:
            raise Exception(
                f"Failed to close job: {close_job_response.status_code} "
                f"{close_job_response.text}"
            )

        final_job_data = close_job_response.json()
        print(
            f"[SalesforceBulkWriteOperator] Job submitted. "
            f"State: {final_job_data['state']}"
        )

    def poll(self) -> SensorResult:
        """Poll job status and defer compute while the job is running."""
        job_id = self._get_task_value("salesforce_job_id", default=self.job_id)
        if not job_id:
            raise ValueError(
                "Could not load salesforce_job_id from task values; "
                "ensure open() completed and task_key is correct."
            )

        response = self._request_salesforce("GET", f"/jobs/ingest/{job_id}")
        if response.status_code != 200:
            raise Exception(
                f"Failed to get job status: {response.status_code} {response.text}"
            )

        job_data = response.json()
        job_state = job_data.get("state", "Unknown")
        records_processed = job_data.get("numberRecordsProcessed", 0)
        records_failed = job_data.get("numberRecordsFailed", 0)

        print(
            f"[SalesforceBulkWriteOperator] State: {job_state}, "
            f"Processed: {records_processed}, Failed: {records_failed}"
        )

        if job_state == "JobComplete":
            self._set_task_value("records_processed", str(records_processed))
            self._set_task_value("records_failed", str(records_failed))
            print("[SalesforceBulkWriteOperator] Job completed successfully")
            return SensorResult.completed()

        if job_state in self.FAILURE_STATES:
            error_message = job_data.get("errorMessage", "Unknown error")
            raise Exception(f"Salesforce job {job_state.lower()}: {error_message}")

        print(
            f"[SalesforceBulkWriteOperator] Job still running. Deferring for "
            f"{self.poll_interval_minutes} minute(s)..."
        )
        return SensorResult.deferred(
            duration=datetime.timedelta(minutes=self.poll_interval_minutes)
        )

    def close(self):
        """
        Cleanup when task is cancelled.

        NOTE: In OperatorV0, close() is NOT yet called on cancellation.
        """
        print("[SalesforceBulkWriteOperator] close() called")

        job_id = self._get_task_value("salesforce_job_id", default=self.job_id)
        if not job_id:
            print("[SalesforceBulkWriteOperator] No job_id found, nothing to clean up")
            return

        try:
            response = self._request_salesforce("GET", f"/jobs/ingest/{job_id}")
            if response.status_code != 200:
                print(
                    "[SalesforceBulkWriteOperator] Could not fetch job during cleanup: "
                    f"{response.status_code} {response.text}"
                )
                return

            job_state = response.json().get("state", "Unknown")
            if job_state in self.TERMINAL_STATES:
                print(
                    f"[SalesforceBulkWriteOperator] Job already terminal: {job_state}"
                )
                return

            abort_response = self._request_salesforce(
                "PATCH",
                f"/jobs/ingest/{job_id}",
                headers={"Content-Type": "application/json"},
                json={"state": "Aborted"},
            )
            if abort_response.status_code == 200:
                print("[SalesforceBulkWriteOperator] Job aborted successfully")
            else:
                print(
                    "[SalesforceBulkWriteOperator] Failed to abort job: "
                    f"{abort_response.status_code} {abort_response.text}"
                )
        except Exception as e:
            print(f"[SalesforceBulkWriteOperator] Error during cleanup: {e}")


class SalesforceUpsertOperator(SalesforceBulkWriteOperator):
    """Backward-compatible wrapper specialized for upsert operations."""

    def __init__(
        self,
        object_name: str,
        external_id_field: str,
        records: str,
        conn_id: str,
        task_key: str,
        api_version: str = "v60.0",
        poll_interval_minutes: int = 1,
    ):
        super().__init__(
            object_name=object_name,
            operation="upsert",
            records=records,
            conn_id=conn_id,
            task_key=task_key,
            external_id_field=external_id_field,
            api_version=api_version,
            poll_interval_minutes=poll_interval_minutes,
        )
