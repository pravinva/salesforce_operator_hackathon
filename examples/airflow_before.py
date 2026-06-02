"""
BEFORE: Traditional Airflow DAG for Salesforce Sync

This is a typical Airflow implementation for syncing data from Databricks to Salesforce.
Problems:
- 2,500+ lines of custom code across multiple files
- $600/month for Airflow infrastructure (HA setup)
- $800/month for Databricks SQL warehouse (JDBC extracts)
- Polling loops block compute while waiting ($12/day for 24-hour wait loop)
- Manual chunking, retry logic, error handling
- Raw credentials in Airflow Variables
- Complex setup and maintenance

Total Cost: $1,400/month ($16,800/year)
Code Complexity: 2,500+ lines
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from simple_salesforce import Salesforce
import pandas as pd
import pyodbc
import csv
import io
import time


def extract_from_databricks(**context):
    """
    Extract data from Databricks using JDBC/ODBC.

    Problems:
    - Requires separate SQL warehouse ($800/month)
    - Complex connection setup
    - 20-30 lines of boilerplate code
    - Error-prone credential management
    """
    # JDBC connection setup (verbose and error-prone)
    conn = pyodbc.connect(
        "Driver={Simba Spark ODBC Driver};"
        f"Host={Variable.get('databricks_host')};"
        "Port=443;"
        f"HTTPPath={Variable.get('databricks_http_path')};"
        "AuthMech=3;"
        "UID=token;"
        f"PWD={Variable.get('databricks_token')}"  # Raw credential in Variable
    )

    cursor = conn.cursor()

    # Execute query
    query = """
        SELECT
            account_number,
            name,
            industry,
            annual_revenue,
            employee_count
        FROM main.crm.accounts
        WHERE updated_date >= CURRENT_DATE - 1
    """

    cursor.execute(query)

    # Fetch results
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    # Convert to DataFrame
    df = pd.DataFrame.from_records(rows, columns=columns)

    print(f"Extracted {len(df)} records from Databricks")

    # Store in XCom (limited size, serialization issues)
    context['ti'].xcom_push(key='accounts_data', value=df.to_dict('records'))

    cursor.close()
    conn.close()


def load_to_salesforce(**context):
    """
    Load data to Salesforce using simple-salesforce library.

    Problems:
    - 40-50 lines of manual chunking and error handling
    - Manual polling loop blocks compute ($12/day!)
    - No automatic retries
    - Complex error handling
    - Raw credentials in Variables
    """
    # Get data from upstream task
    records = context['ti'].xcom_pull(key='accounts_data', task_ids='extract')

    if not records:
        print("No records to sync")
        return

    # Authenticate to Salesforce (raw credentials!)
    sf = Salesforce(
        username=Variable.get('sf_username'),
        password=Variable.get('sf_password'),
        security_token=Variable.get('sf_security_token')  # Security risk!
    )

    # Manual chunking (Salesforce Bulk API limit: ~90MB per job)
    chunk_size = 10000
    total_chunks = (len(records) + chunk_size - 1) // chunk_size

    print(f"Processing {len(records)} records in {total_chunks} chunks")

    total_processed = 0
    total_failed = 0

    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]

        print(f"Processing chunk {i // chunk_size + 1}/{total_chunks}")

        # Convert to CSV (manual implementation)
        csv_data = _records_to_csv(chunk)

        # Create bulk job
        try:
            job = sf.bulk.Account.upsert(
                csv_data,
                external_id_field='AccountNumber'
            )

            job_id = job['id']

            print(f"Created Salesforce job: {job_id}")

            # MANUAL POLLING LOOP - BLOCKS COMPUTE!
            # This is the main cost driver: $12/day for 24-hour wait loop
            max_wait = 3600  # 1 hour timeout
            start_time = time.time()

            while True:
                if time.time() - start_time > max_wait:
                    raise Exception(f"Salesforce job {job_id} timeout after {max_wait} seconds")

                # Get job status
                status = sf.bulk.get_job(job_id)
                job_state = status['state']

                print(f"Job {job_id} state: {job_state}")

                if job_state == 'JobComplete':
                    # SUCCESS
                    processed = int(status.get('numberRecordsProcessed', 0))
                    failed = int(status.get('numberRecordsFailed', 0))

                    total_processed += processed
                    total_failed += failed

                    print(f"Chunk complete: Processed {processed}, Failed {failed}")
                    break

                elif job_state in ['Failed', 'Aborted']:
                    # FAILURE
                    error_msg = status.get('errorMessage', 'Unknown error')
                    raise Exception(f"Salesforce job {job_id} {job_state}: {error_msg}")

                # BLOCKS COMPUTE WHILE WAITING!
                # This 10-second sleep happens on a running cluster
                # For a 24-hour wait loop, this costs $12/day!
                time.sleep(10)

        except Exception as e:
            print(f"Error processing chunk {i // chunk_size + 1}: {e}")
            # Manual retry logic would go here (another 20-30 lines)
            raise

    print(f"Load complete: Processed {total_processed}, Failed {total_failed}")


def _records_to_csv(records):
    """
    Convert records to CSV for Bulk API.

    Manual implementation (10-15 lines).
    """
    if not records:
        return ""

    # Get all field names
    fieldnames = set()
    for record in records:
        fieldnames.update(record.keys())

    fieldnames = sorted(list(fieldnames))

    # Create CSV
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')

    writer.writeheader()
    writer.writerows(records)

    return output.getvalue()


# DAG definition
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['data-engineering@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'salesforce_account_sync',
    default_args=default_args,
    description='Daily sync of accounts from Databricks to Salesforce',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['salesforce', 'crm', 'reverse-etl'],
) as dag:

    # Extract data from Databricks
    extract_task = PythonOperator(
        task_id='extract_databricks',
        python_callable=extract_from_databricks,
        provide_context=True,
    )

    # Load to Salesforce
    load_task = PythonOperator(
        task_id='load_salesforce',
        python_callable=load_to_salesforce,
        provide_context=True,
    )

    # Define dependencies
    extract_task >> load_task


"""
INFRASTRUCTURE COSTS (Monthly):

1. Airflow Cluster (HA setup):
   - 3 workers (t3.large): $200/month
   - 1 scheduler (t3.large): $67/month
   - 1 webserver (t3.medium): $33/month
   - RDS PostgreSQL (db.t3.medium): $100/month
   - ELB: $20/month
   - EBS volumes: $180/month
   Total: $600/month

2. Databricks SQL Warehouse (for JDBC):
   - Classic SQL warehouse (Medium): $800/month
   Total: $800/month

3. Compute for polling wait loops:
   - 24-hour wait loop blocks cluster: $12/day = $360/month
   Total: $360/month

TOTAL MONTHLY COST: $1,760/month ($21,120/year)

CODE MAINTENANCE:
- airflow_dags/salesforce_sync.py: 200 lines
- utils/databricks_connector.py: 150 lines
- utils/salesforce_loader.py: 300 lines
- utils/csv_converter.py: 100 lines
- config/airflow_variables.json: 50 lines
- docker/airflow_Dockerfile: 100 lines
- terraform/airflow_infrastructure.tf: 500 lines
- monitoring/datadog_monitors.yaml: 200 lines
- ... (more support files)
Total: 2,500+ lines across 10+ files

OPERATIONAL OVERHEAD:
- 3 separate systems to manage (Airflow, Databricks, Salesforce)
- Multiple credential stores (Airflow Variables, K8s Secrets, etc.)
- Complex networking and VPC peering
- Airflow version upgrades and security patches
- Custom monitoring and alerting setup
- On-call rotation for Airflow failures
"""
