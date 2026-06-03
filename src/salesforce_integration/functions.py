"""
Salesforce Functions for Databricks Python Operator Framework.

These functions execute Salesforce API operations using Unity Catalog Connections
for authentication. No raw credentials are exposed in code.

Each function:
- Uses type annotations for automatic parameter binding
- Authenticates via UC Connection proxy (no secrets in code)
- Returns structured results as task values for downstream tasks
- Can be registered as Genie Code skills for AI-assisted execution
"""

import json
import csv
import io
from typing import Dict, Any, List, Optional
import requests
from databricks.sdk import WorkspaceClient
try:
    from databricks.sdk.runtime import dbutils  # type: ignore
except Exception:  # pragma: no cover - runtime availability differs by execution context
    dbutils = None


def _coerce_records(records: Any) -> List[Dict[str, Any]]:
    """Accept list[dict] or JSON string for records."""
    if isinstance(records, str):
        records = json.loads(records)
    if not isinstance(records, list):
        raise ValueError("records must be a list of objects")
    for idx, item in enumerate(records):
        if not isinstance(item, dict):
            raise ValueError(
                f"records[{idx}] must be an object, got {type(item).__name__}"
            )
    return records


def _coerce_bool(value: Any) -> bool:
    """Parse bool values passed as strings from task parameters."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y"}:
            return True
        if normalized in {"false", "0", "no", "n"}:
            return False
    return bool(value)


def _request_salesforce(
    w: WorkspaceClient,
    conn_id: str,
    api_version: str,
    method: str,
    resource_path: str,
    **kwargs: Any,
) -> requests.Response:
    """
    Send request through UC proxy and support both connection host styles:
    1) host-only connection: prefix /services/data/{api_version}
    2) connection already configured with base_path: use resource path directly
    """
    proxy_base_url = f"{w.config.host}/api/2.0/unity-catalog/connections/{conn_id}/proxy"
    candidate_paths = [f"/services/data/{api_version}{resource_path}", resource_path]

    last_response = None
    for idx, path in enumerate(candidate_paths):
        for attempt in range(2):
            merged_headers = {
                **w.config.authenticate(),
                "Accept": "application/json",
                "Accept-Encoding": "identity",
            }
            if kwargs.get("headers"):
                merged_headers.update(kwargs["headers"])

            request_kwargs = dict(kwargs)
            request_kwargs["headers"] = merged_headers
            response = requests.request(
                method,
                f"{proxy_base_url}{path}",
                **request_kwargs,
            )
            last_response = response
            if (
                attempt == 0
                and response.status_code == 401
                and "INVALID_SESSION_ID" in response.text
            ):
                print(
                    "[salesforce_integration] received INVALID_SESSION_ID; "
                    "retrying request once..."
                )
                continue
            break
        should_fallback = idx == 0 and response.status_code == 404
        if not should_fallback:
            return response
        print("[salesforce_integration] path fallback: retrying with connection base_path")
    return last_response  # type: ignore[return-value]


def _set_task_value(key: str, value: str) -> None:
    if dbutils is None:
        return
    try:
        dbutils.jobs.taskValues.set(key, value)
    except Exception:
        # Task values are best-effort metadata, not required for core write flow.
        pass


def salesforce_upsert(
    object_name: str,
    external_id_field: str,
    records: str,
    conn_id: str,
    api_version: str = "v60.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    """
    Upsert records to Salesforce using Bulk API 2.0.

    Inserts new records or updates existing records based on external ID match.
    Uses Unity Catalog Connection for secure authentication.

    Args:
        object_name: Salesforce object name (e.g., "Account", "Contact")
        external_id_field: Field to match on for upsert (e.g., "AccountNumber")
        records: List of dictionaries containing record data
        conn_id: Unity Catalog Connection ID for Salesforce
        api_version: Salesforce API version (default: "v60.0")
        wait_for_completion: If True, polls until job completes.

    Returns:
        Dictionary containing:
            - job_id: Salesforce Bulk API job ID
            - object: Salesforce object name
            - operation: "upsert"
            - state: Current job state
            - records_total: Number of records submitted

    Example:
        >>> result = salesforce_upsert(
        ...     object_name="Account",
        ...     external_id_field="AccountNumber",
        ...     records=[
        ...         {"AccountNumber": "A001", "Name": "Acme Corp"},
        ...         {"AccountNumber": "A002", "Name": "Globex Inc"}
        ...     ],
        ...     conn_id="salesforce_prod"
        ... )
        >>> print(result["job_id"])
        750xx00000000001AAA
    """
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="upsert",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        external_id_field=external_id_field,
        wait_for_completion=wait_for_completion,
    )


def salesforce_insert(
    object_name: str,
    records: str,
    conn_id: str,
    api_version: str = "v60.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    """
    Insert new records to Salesforce using Bulk API 2.0.

    Only inserts new records. Fails if record already exists.

    Args:
        object_name: Salesforce object name
        records: List of dictionaries containing record data
        conn_id: Unity Catalog Connection ID for Salesforce
        api_version: Salesforce API version
        wait_for_completion: If True, polls until job completes

    Returns:
        Dictionary with job_id and metadata
    """
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="insert",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        wait_for_completion=wait_for_completion,
    )


def salesforce_update(
    object_name: str,
    records: str,
    conn_id: str,
    api_version: str = "v60.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    """
    Update existing records in Salesforce using Bulk API 2.0.

    Only updates existing records. Requires Salesforce ID in records.

    Args:
        object_name: Salesforce object name
        records: List of dictionaries with "Id" field and data to update
        conn_id: Unity Catalog Connection ID for Salesforce
        api_version: Salesforce API version
        wait_for_completion: If True, polls until job completes

    Returns:
        Dictionary with job_id and metadata
    """
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="update",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        wait_for_completion=wait_for_completion,
    )


def salesforce_delete(
    object_name: str,
    records: str,
    conn_id: str,
    api_version: str = "v60.0",
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    """
    Delete records from Salesforce using Bulk API 2.0.

    Hard deletes records (not recoverable from Recycle Bin via API).

    Args:
        object_name: Salesforce object name
        records: List of dictionaries with "Id" field
        conn_id: Unity Catalog Connection ID for Salesforce
        api_version: Salesforce API version
        wait_for_completion: If True, polls until job completes

    Returns:
        Dictionary with job_id and metadata
    """
    return _salesforce_bulk_operation(
        object_name=object_name,
        operation="delete",
        records=records,
        conn_id=conn_id,
        api_version=api_version,
        wait_for_completion=wait_for_completion,
    )


def _salesforce_bulk_operation(
    object_name: str,
    operation: str,
    records: Any,
    conn_id: str,
    api_version: str = "v60.0",
    external_id_field: Optional[str] = None,
    wait_for_completion: bool = False,
) -> Dict[str, Any]:
    """
    Internal function to execute Salesforce Bulk API 2.0 operations.

    Uses Unity Catalog Connection proxy for authentication.
    No raw credentials are exposed in code.
    """
    w = WorkspaceClient()

    records = _coerce_records(records)
    wait_for_completion = _coerce_bool(wait_for_completion)

    # Convert records to CSV format (Bulk API 2.0 requirement)
    csv_data = _records_to_csv(records)

    # Step 1: Create Bulk API job
    create_job_payload = {
        "object": object_name,
        "operation": operation,
        "lineEnding": "LF",
    }

    if external_id_field:
        create_job_payload["externalIdFieldName"] = external_id_field

    print(f"[Salesforce] Creating {operation} job for {object_name}...")

    create_job_response = _request_salesforce(
        w,
        conn_id,
        api_version,
        "POST",
        "/jobs/ingest",
        headers={"Content-Type": "application/json"},
        json=create_job_payload,
    )

    if create_job_response.status_code != 200:
        raise Exception(
            f"Failed to create Salesforce job: {create_job_response.status_code} "
            f"{create_job_response.text}"
        )

    job_data = create_job_response.json()
    job_id = job_data["id"]

    print(f"[Salesforce] Job created: {job_id}")

    # Step 2: Upload CSV data
    print(f"[Salesforce] Uploading {len(records)} records...")

    upload_response = _request_salesforce(
        w,
        conn_id,
        api_version,
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

    print(f"[Salesforce] Data uploaded successfully")

    # Step 3: Mark job as UploadComplete to start processing
    close_job_response = _request_salesforce(
        w,
        conn_id,
        api_version,
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

    print(f"[Salesforce] Job submitted. State: {final_job_data['state']}")

    result = {
        "job_id": job_id,
        "object": object_name,
        "operation": operation,
        "state": final_job_data["state"],
        "records_total": len(records),
        "api_version": api_version,
        "conn_id": conn_id,
    }

    # Store metadata in task values for downstream tasks (best effort).
    _set_task_value("salesforce_job_id", job_id)
    _set_task_value("salesforce_object", object_name)
    _set_task_value("salesforce_operation", operation)

    if wait_for_completion:
        # Poll job status until terminal state
        import time
        while True:
            status_response = _request_salesforce(
                w,
                conn_id,
                api_version,
                "GET",
                f"/jobs/ingest/{job_id}",
            )
            status_data = status_response.json()
            state = status_data["state"]

            print(f"[Salesforce] Job state: {state}")

            if state == "JobComplete":
                result.update({
                    "state": state,
                    "records_processed": status_data.get("numberRecordsProcessed", 0),
                    "records_failed": status_data.get("numberRecordsFailed", 0),
                })
                break
            elif state in ["Failed", "Aborted"]:
                raise Exception(f"Salesforce job {state}: {status_data.get('errorMessage', 'Unknown error')}")

            time.sleep(5)

    return result


def _records_to_csv(records: List[Dict[str, Any]]) -> str:
    """
    Convert list of dictionaries to CSV string for Salesforce Bulk API.

    Args:
        records: List of dictionaries

    Returns:
        CSV string with proper escaping
    """
    if not records:
        raise ValueError("Cannot create CSV from empty records list")

    # Get all unique field names from all records
    fieldnames = set()
    for record in records:
        fieldnames.update(record.keys())

    fieldnames = sorted(list(fieldnames))

    # Create CSV
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=fieldnames,
        extrasaction='ignore',
        lineterminator="\n",
    )

    writer.writeheader()
    writer.writerows(records)

    return output.getvalue()
