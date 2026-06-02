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
    "connection_type": ConnectionType.HTTP,
    "options": {
        "host": "https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com",
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

    print("\n✓ Next step: Add OAuth credentials via UI")
    print(f"  1. Go to: {w.config.host}/explore/data/connections/{connection.name}")
    print(f"  2. Click 'Edit'")
    print(f"  3. Add OAuth 2.0 authentication:")
    print(f"     - Token URL: https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com/services/oauth2/token")
    print(f"     - Grant Type: Client Credentials or Password")
    print(f"     - Client ID: <SALESFORCE_CONSUMER_KEY>")
    print(f"     - Client Secret: <SALESFORCE_CONSUMER_SECRET>")

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
            print(f"  You can use it or delete and recreate:")
            print(f"  databricks connections delete salesforce_dev --profile dogfood")
    except Exception as list_err:
        print(f"  Could not list connections: {list_err}")
