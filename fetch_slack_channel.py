#!/usr/bin/env python3
"""
Script to fetch Slack messages from a channel using MCP protocol.
"""
import subprocess
import json
import sys

def call_mcp_tool(tool_name, arguments):
    """Call an MCP tool using JSON-RPC protocol."""

    # JSON-RPC request format
    request = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    }

    # Start the MCP server process
    process = subprocess.Popen(
        ["python3.10", "/Users/pravin.varma/mcp/servers/slack_mcp/slack_mcp_deploy.pex"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**subprocess.os.environ, "DATABRICKS_CONFIG_PROFILE": "ai-devtools"}
    )

    try:
        # Send the request
        request_json = json.dumps(request) + "\n"
        stdout, stderr = process.communicate(input=request_json, timeout=30)

        if stderr:
            print(f"Error: {stderr}", file=sys.stderr)

        # Parse response
        if stdout:
            for line in stdout.split("\n"):
                if line.strip():
                    try:
                        response = json.loads(line)
                        return response
                    except json.JSONDecodeError:
                        continue

        return None

    except subprocess.TimeoutExpired:
        process.kill()
        print("Request timed out", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error calling MCP tool: {e}", file=sys.stderr)
        return None

def get_channel_messages(channel_name):
    """Get messages from a Slack channel."""

    print(f"Finding channel: {channel_name}...")

    # First, list channels to find the ID
    channels_result = call_mcp_tool(
        "slack_read_api_call",
        {
            "endpoint": "conversations.list",
            "params": {
                "types": "public_channel,private_channel",
                "exclude_archived": True,
                "limit": 500
            },
            "raw": True
        }
    )

    if not channels_result:
        print("Failed to get channel list")
        return

    # Extract channel data
    if "result" in channels_result and channels_result["result"]:
        channels_data = channels_result["result"][0].get("text", "")
        try:
            channels_json = json.loads(channels_data)
            channels = channels_json.get("channels", [])

            # Find our channel
            target_channel = None
            for channel in channels:
                if channel.get("name") == channel_name:
                    target_channel = channel
                    break

            if not target_channel:
                print(f"Channel '{channel_name}' not found")
                return

            channel_id = target_channel["id"]
            print(f"Found channel ID: {channel_id}")

            # Get messages from the channel
            print("Fetching messages...")
            messages_result = call_mcp_tool(
                "slack_read_api_call",
                {
                    "endpoint": "conversations.history",
                    "params": {
                        "channel": channel_id,
                        "limit": 100
                    },
                    "raw": True
                }
            )

            if messages_result and "result" in messages_result:
                messages_data = messages_result["result"][0].get("text", "")
                messages_json = json.loads(messages_data)

                print(json.dumps(messages_json, indent=2))
                return messages_json
            else:
                print("Failed to get messages")

        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")
            return
    else:
        print("No result data found")

if __name__ == "__main__":
    channel_name = "2026-jobs-external-orchestration-hackathon"
    get_channel_messages(channel_name)
