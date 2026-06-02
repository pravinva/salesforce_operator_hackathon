#!/usr/bin/env python3
"""
Fetch Slack messages using Databricks connection.
"""
from databricks.sdk import WorkspaceClient
import json

def get_slack_channel_messages(channel_name):
    """Get messages from a Slack channel using Databricks connection."""

    w = WorkspaceClient(profile='ai-devtools')

    print(f"Connected to workspace: {w.config.host}")
    print(f"Searching for channel: {channel_name}...")

    # Get the Slack connection
    connection = w.connections.get("slack")
    print(f"Slack connection: {connection.name} ({connection.connection_type})")

    # Try to query Slack using the connection
    # The Databricks SDK may not have direct methods for this, so we'll use the REST API

    try:
        # Try to list channels using the Slack API through the connection
        response = w.api_client.do(
            'POST',
            '/api/2.0/http/send',
            data={
                'connection_name': 'slack',
                'method': 'GET',
                'path': '/conversations.list',
                'query_params': {
                    'types': 'public_channel,private_channel',
                    'exclude_archived': 'true',
                    'limit': '500'
                }
            }
        )

        print("Channels list response:")
        print(json.dumps(response, indent=2))

        # Find the channel by name
        channels = response.get('channels', [])
        target_channel = None
        for channel in channels:
            if channel.get('name') == channel_name:
                target_channel = channel
                break

        if not target_channel:
            print(f"Channel '{channel_name}' not found")
            return None

        channel_id = target_channel['id']
        print(f"\nFound channel ID: {channel_id}")

        # Get messages from the channel
        print("\nFetching messages...")
        messages_response = w.api_client.do(
            'POST',
            '/api/2.0/http/send',
            data={
                'connection_name': 'slack',
                'method': 'GET',
                'path': '/conversations.history',
                'query_params': {
                    'channel': channel_id,
                    'limit': '100'
                }
            }
        )

        print("\nMessages response:")
        print(json.dumps(messages_response, indent=2))

        return messages_response

    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    channel_name = "2026-jobs-external-orchestration-hackathon"
    get_slack_channel_messages(channel_name)
