import logging

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os

log = logging.getLogger(__name__)

def test_notification():
    # Initialize Slack client with token
    client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

    try:
        response = client.chat_postMessage(
            channel="#test-channel",
            text="This is a test notification!"
        )
        return response["ok"]
    except SlackApiError as e:
        log.error(f"Error sending message: {e.response['error']}")
        return False
