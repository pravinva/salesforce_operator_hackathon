from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType
import json

# Connect to dogfood
w = WorkspaceClient(profile="dogfood")

print("Creating Salesforce UC Connection...")
print(f"Workspace: {w.config.host}")

# Salesforce configuration
connection_config = {
    "name": "salesforce_dev",
    "connection_type": "SALESFORCE",  # Use native SALESFORCE type
    "options": {
        "instance_url": "https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com",
        "client_id": "<SALESFORCE_CONSUMER_KEY>",
        "client_secret": "<SALESFORCE_CONSUMER_SECRET>",
        "is_sandbox": "false",
    },
    "comment": "Salesforce Developer Edition for hackathon"
}

try:
    # Create connection
    connection = w.connections.create(**connection_config)

    print(f"\n✓ Connection created successfully!")
    print(f"  Name: {connection.name}")
    print(f"  Type: {connection.connection_type}")
    print(f"  Metastore: {connection.metastore_id}")
    print(f"  Connection ID: {connection.connection_id}")

    print("\n✓ Connection is ready to use!")
    print(f"  View in UI: {w.config.host}/explore/data/connections/{connection.name}")

except Exception as e:
    print(f"\n✗ Error creating connection: {e}")

    # Try to list existing connections
    print("\nExisting connections:")
    try:
        connections = list(w.connections.list())
        for conn in connections:
            print(f"  - {conn.name} ({conn.connection_type})")

        # Check if salesforce_dev already exists
        existing = [c for c in connections if c.name == "salesforce_dev"]
        if existing:
            print(f"\n✓ Connection 'salesforce_dev' already exists!")
            print(f"  Connection ID: {existing[0].connection_id}")
            print(f"  Type: {existing[0].connection_type}")
    except Exception as list_err:
        print(f"  Could not list connections: {list_err}")
