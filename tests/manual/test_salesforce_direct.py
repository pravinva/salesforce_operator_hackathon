"""
Direct test of Salesforce API without UC Connection.
This validates the core integration logic works before deploying to Databricks.
"""

import requests
import json
import csv
import io
import time

# Salesforce credentials
SF_INSTANCE_URL = "https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com"
SF_CLIENT_ID = "<SALESFORCE_CONSUMER_KEY>"
SF_CLIENT_SECRET = "<SALESFORCE_CONSUMER_SECRET>"
SF_USERNAME = "pravin.varmadbrx.00eac66fc88f@agentforce.com"

print("=" * 80)
print("SALESFORCE API TEST - Direct Authentication")
print("=" * 80)

# Step 1: Get OAuth access token
print("\n[1/5] Getting OAuth access token...")
token_url = f"{SF_INSTANCE_URL}/services/oauth2/token"

# For username/password flow, we need the user's password + security token
# Since we only have client credentials, let's try client credentials flow
token_data = {
    "grant_type": "client_credentials",
    "client_id": SF_CLIENT_ID,
    "client_secret": SF_CLIENT_SECRET,
}

try:
    token_response = requests.post(token_url, data=token_data)
    print(f"Token response status: {token_response.status_code}")

    if token_response.status_code != 200:
        print(f"Token error: {token_response.text}")
        print("\n⚠️  Client credentials flow may not be enabled.")
        print("For testing, please provide your Salesforce password and security token.")
        print("\nAlternatively, we can test via the UI by manually creating UC Connection.")
        exit(1)

    token_data = token_response.json()
    access_token = token_data["access_token"]
    print(f"✓ Access token obtained: {access_token[:20]}...")

except Exception as e:
    print(f"Error getting token: {e}")
    exit(1)

# Step 2: Create test records
print("\n[2/5] Creating test records...")
test_records = [
    {
        "AccountNumber": f"HACK{int(time.time())}001",
        "Name": "Hackathon Test Company 1",
        "Industry": "Technology",
        "AnnualRevenue": 1000000,
    },
    {
        "AccountNumber": f"HACK{int(time.time())}002",
        "Name": "Hackathon Test Company 2",
        "Industry": "Finance",
        "AnnualRevenue": 2000000,
    },
]

print(f"Test records: {json.dumps(test_records, indent=2)}")

# Step 3: Convert to CSV
print("\n[3/5] Converting to CSV...")
fieldnames = ["AccountNumber", "Name", "Industry", "AnnualRevenue"]
output = io.StringIO()
writer = csv.DictWriter(output, fieldnames=fieldnames)
writer.writeheader()
writer.writerows(test_records)
csv_data = output.getvalue()
print(f"CSV data ({len(csv_data)} bytes):")
print(csv_data)

# Step 4: Create Bulk API job
print("\n[4/5] Creating Salesforce Bulk API job...")
create_job_url = f"{SF_INSTANCE_URL}/services/data/v60.0/jobs/ingest"
create_job_payload = {
    "object": "Account",
    "operation": "upsert",
    "externalIdFieldName": "AccountNumber",
}

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

create_job_response = requests.post(
    create_job_url,
    headers=headers,
    json=create_job_payload,
)

if create_job_response.status_code != 200:
    print(f"Error creating job: {create_job_response.status_code}")
    print(create_job_response.text)
    exit(1)

job_data = create_job_response.json()
job_id = job_data["id"]
print(f"✓ Job created: {job_id}")
print(f"Job state: {job_data['state']}")

# Step 5: Upload CSV data
print("\n[5/5] Uploading data...")
upload_url = f"{SF_INSTANCE_URL}/services/data/v60.0/jobs/ingest/{job_id}/batches"
upload_headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "text/csv",
}

upload_response = requests.put(upload_url, headers=upload_headers, data=csv_data)

if upload_response.status_code not in [200, 201]:
    print(f"Error uploading data: {upload_response.status_code}")
    print(upload_response.text)
    exit(1)

print("✓ Data uploaded")

# Step 6: Mark job as complete
print("\n[6/6] Marking job as UploadComplete...")
close_url = f"{SF_INSTANCE_URL}/services/data/v60.0/jobs/ingest/{job_id}"
close_payload = {"state": "UploadComplete"}

close_response = requests.patch(close_url, headers=headers, json=close_payload)

if close_response.status_code != 200:
    print(f"Error closing job: {close_response.status_code}")
    print(close_response.text)
    exit(1)

final_job_data = close_response.json()
print(f"✓ Job marked as {final_job_data['state']}")

# Step 7: Monitor job completion
print("\n[7/7] Monitoring job completion...")
print("(This may take 30-60 seconds...)\n")

poll_count = 0
while poll_count < 60:  # Max 5 minutes
    poll_count += 1

    status_response = requests.get(close_url, headers=headers)
    status_data = status_response.json()
    state = status_data["state"]

    processed = status_data.get("numberRecordsProcessed", 0)
    failed = status_data.get("numberRecordsFailed", 0)

    print(f"  Poll #{poll_count}: State={state}, Processed={processed}, Failed={failed}")

    if state == "JobComplete":
        print("\n" + "=" * 80)
        print("✓ SUCCESS! Job completed")
        print("=" * 80)
        print(f"Records processed: {processed}")
        print(f"Records failed: {failed}")
        print(f"\nCheck Salesforce UI for new accounts:")
        print(f"  {SF_INSTANCE_URL}/lightning/o/Account/list")
        print(f"\nLook for accounts with AccountNumber starting with 'HACK'")
        break

    elif state in ["Failed", "Aborted"]:
        print(f"\n✗ Job {state}: {status_data.get('errorMessage', 'Unknown error')}")
        exit(1)

    time.sleep(5)

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
