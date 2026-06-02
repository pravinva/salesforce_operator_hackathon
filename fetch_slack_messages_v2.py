#!/usr/bin/env python3
"""
Script to fetch Slack messages using Databricks SDK directly.
"""
from databricks.sdk import WorkspaceClient
import json
import sys

def get_slack_messages(channel_name):
    """Get messages from a Slack channel using Databricks Workspace Client."""

    # Initialize workspace client with ai-devtools profile
    w = WorkspaceClient(profile='ai-devtools')

    print(f"Connected to workspace: {w.config.host}")

    try:
        # Try to call the Slack API through Databricks connections
        # First, let's list channels
        print(f"Searching for channel: {channel_name}...")

        response = w.api_client.do(
            'POST',
            '/api/2.0/serving-endpoints/query',
            data={
                'name': 'slack',
                'query': {
                    'endpoint': 'conversations.list',
                    'params': {
                        'types': 'public_channel,private_channel',
                        'exclude_archived': True,
                        'limit': 500
                    }
                }
            }
        )

        print("Response:", json.dumps(response, indent=2))

    except Exception as e:
        print(f"Error with serving endpoint approach: {e}")

        # Try alternative: Direct API call
        try:
            print("\nTrying direct connection API call...")
            response = w.api_client.do(
                'GET',
                f'/api/2.0/connections/slack/channels',
                query={
                    'channel_name': channel_name
                }
            )
            print("Response:", json.dumps(response, indent=2))

        except Exception as e2:
            print(f"Error with direct API: {e2}")

            # Try listing all connections
            try:
                print("\nListing available connections...")
                response = w.api_client.do('GET', '/api/2.0/connections')
                print("Connections:", json.dumps(response, indent=2))

            except Exception as e3:
                print(f"Error listing connections: {e3}")

if __name__ == "__main__":
    channel_name = "2026-jobs-external-orchestration-hackathon"
    get_slack_messages(channel_name)
