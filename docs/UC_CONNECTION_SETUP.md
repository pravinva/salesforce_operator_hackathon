# Unity Catalog Connection Setup for Salesforce

This guide shows how to create a Unity Catalog Connection for authenticating to Salesforce.

## Prerequisites

- Salesforce Developer Edition account
- Salesforce Connected App with OAuth enabled
- Databricks workspace with Unity Catalog enabled (e2-dogfood for hackathon)

## Step 1: Get Salesforce Credentials

You should already have:
- **Instance URL**: `https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com`
- **Consumer Key (Client ID)**: `<SALESFORCE_CONSUMER_KEY>`
- **Consumer Secret (Client Secret)**: `<SALESFORCE_CONSUMER_SECRET>`

## Step 2: Create Unity Catalog Connection

### Via Databricks UI (Recommended for Hackathon)

1. **Navigate to Connections**:
   - Log into e2-dogfood: https://e2-dogfood.staging.cloud.databricks.com
   - Go to **Data** → **Connections**

2. **Create New Connection**:
   - Click **Create Connection**
   - **Connection Name**: `salesforce_dev`
   - **Connection Type**: **HTTP**

3. **Configure HTTP Connection**:
   - **URL**: `https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com`
   - **HTTP Path** (optional): `/services/data/v60.0`

4. **Add OAuth 2.0 Authentication**:
   - Click **Add Authentication**
   - **Auth Type**: OAuth 2.0
   - **Token URL**: `https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com/services/oauth2/token`
   - **Client ID**: `<SALESFORCE_CONSUMER_KEY>`
   - **Client Secret**: `<SALESFORCE_CONSUMER_SECRET>`

5. **Test Connection**:
   - Click **Test Connection**
   - Should see: "Connection successful"

6. **Save**:
   - Click **Create** to save the connection

## Step 3: Grant Access

If using in a notebook or job, ensure the service principal or user has access:

```sql
-- Grant access to connection
GRANT USE SCHEMA ON CONNECTION salesforce_dev TO `your-service-principal`;
```

## Step 4: Test the Connection

Create a test notebook:

```python
from databricks.sdk import WorkspaceClient
import requests

w = WorkspaceClient()

# Test connection via proxy
response = requests.get(
    f"{w.config.host}/api/2.0/unity-catalog/connections/salesforce_dev/proxy/services/data/v60.0/",
    headers={
        **w.config.authenticate(),
        "Accept": "application/json",
    }
)

print(response.json())
# Should see Salesforce API version info
```

## Alternative: Via Databricks CLI (Advanced)

```bash
databricks connections create \
  --name salesforce_dev \
  --connection-type HTTP \
  --options '{"host": "https://orgfarm-85729c3c78-dev-ed.develop.my.salesforce.com"}' \
  --profile dogfood
```

Note: Adding OAuth credentials via CLI may not be supported yet. Use UI for full setup.

## Troubleshooting

### Connection Test Fails

1. **Check Salesforce Connected App**:
   - Ensure OAuth is enabled
   - Callback URL should be set
   - Scopes should include `api`, `refresh_token`, `offline_access`

2. **Check Salesforce Org**:
   - Is the org active?
   - Are API calls enabled?

3. **Check Credentials**:
   - Client ID matches Consumer Key from Connected App
   - Client Secret matches Consumer Secret from Connected App

### "Unauthorized" Error

- Token may have expired
- Refresh the UC Connection or re-authenticate

### "Connection Not Found" Error

- Ensure connection name is `salesforce_dev` (case-sensitive)
- Check you're using the right workspace (dogfood)

## Next Steps

Once the connection is created and tested:

1. **Deploy the DAB**:
   ```bash
   databricks bundle deploy --profile dogfood
   ```

2. **Run a test job**:
   ```bash
   databricks bundle run salesforce_upsert_operator --profile dogfood
   ```

3. **Monitor in UI**:
   - Go to Workflows → salesforce_upsert_operator
   - Validate function/operator task lifecycle and results.

## Security Best Practices

✅ **Use UC Connections** - Credentials never touch your code
✅ **Grant least privilege** - Only grant access to users/principals that need it
✅ **Rotate credentials** - Update Connected App secrets regularly
✅ **Monitor access** - Use Unity Catalog audit logs to track connection usage
✅ **Use production orgs carefully** - Test on sandbox/developer editions first
