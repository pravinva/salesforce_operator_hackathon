"""
Create Unity Catalog Connection for Salesforce on e2-dogfood workspace.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ConnectionType,
    CreateConnection,
)

# Connect to dogfood workspace
w = WorkspaceClient(profile="dogfood")

# Salesforce credentials
SF_INSTANCE_URL = "https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com"
SF_CLIENT_ID = "<SALESFORCE_CONSUMER_KEY>"
SF_CLIENT_SECRET = "<SALESFORCE_CONSUMER_SECRET>"

print("Creating Unity Catalog Connection for Salesforce...")
print(f"Instance URL: {SF_INSTANCE_URL}")
print(f"Client ID: {SF_CLIENT_ID[:20]}...")

try:
    # Create HTTP connection
    connection = w.connections.create(
        name="salesforce_dev",
        connection_type=ConnectionType.HTTP,
        options={
            "host": SF_INSTANCE_URL,
            "http_path": "/services/data/v60.0",
        },
        # Note: SDK may not support setting credentials programmatically yet
        # You may need to add credentials via UI after creation
    )

    print(f"✓ Connection created: {connection.name}")
    print(f"  Connection ID: {connection.connection_id}")
    print(f"  Type: {connection.connection_type}")

    print("\n⚠️  IMPORTANT: You may need to add OAuth credentials via the UI:")
    print("  1. Go to Databricks UI → Data → Connections")
    print(f"  2. Click on '{connection.name}'")
    print("  3. Add OAuth 2.0 credentials:")
    print(f"     - Token URL: {SF_INSTANCE_URL}/services/oauth2/token")
    print(f"     - Client ID: {SF_CLIENT_ID}")
    print(f"     - Client Secret: {SF_CLIENT_SECRET}")
    print("  4. Test the connection")

except Exception as e:
    print(f"Error creating connection: {e}")
    print("\nYou can create the connection manually via the UI:")
    print("  1. Go to Databricks UI → Data → Connections")
    print("  2. Click 'Create Connection'")
    print("  3. Select 'HTTP' as connection type")
    print(f"  4. Name: salesforce_dev")
    print(f"  5. URL: {SF_INSTANCE_URL}")
    print("  6. Add OAuth 2.0 authentication:")
    print(f"     - Token URL: {SF_INSTANCE_URL}/services/oauth2/token")
    print(f"     - Client ID: {SF_CLIENT_ID}")
    print(f"     - Client Secret: {SF_CLIENT_SECRET}")
