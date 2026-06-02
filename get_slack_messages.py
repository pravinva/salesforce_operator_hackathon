#!/usr/bin/env python3
"""
Script to fetch Slack messages from a channel using Databricks workspace client.
"""
import os
from databricks.sdk import WorkspaceClient
import json

def get_slack_messages(channel_name):
    """Fetch messages from a Slack channel."""

    # Initialize workspace client with dogfood profile
    w = WorkspaceClient(profile='dogfood')

    # The Slack MCP uses internal Databricks APIs to access Slack
    # We'll try to use the workspace client's API client directly

    try:
        # Try to call the internal Slack API endpoint
        response = w.api_client.do(
            'GET',
            '/api/2.0/slack/conversations.history',
            data={
                'channel': channel_name,
                'limit': 100
            }
        )

        return response

    except Exception as e:
        print(f"Error fetching Slack messages: {e}")

        # Try alternative endpoint
        try:
            response = w.api_client.do(
                'GET',
                '/api/2.0/preview/slack/messages',
                query={
                    'channel_name': channel_name,
                    'limit': 100
                }
            )
            return response
        except Exception as e2:
            print(f"Alternative endpoint also failed: {e2}")
            return None

if __name__ == "__main__":
    channel_name = "2026-jobs-external-orchestration-hackathon"

    print(f"Fetching messages from #{channel_name}...")
    messages = get_slack_messages(channel_name)

    if messages:
        print(json.dumps(messages, indent=2))
    else:
        print("Failed to fetch messages")
