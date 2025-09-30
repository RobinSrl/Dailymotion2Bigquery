import logging

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os

log = logging.getLogger(__name__)

def prepare_message(text:str, **kwargs):
    text = text.strip(kwargs.get("strip", None))
    text = text[0].upper() + text[1:]

    if kwargs.get("replace") and isinstance(kwargs.get("replace"),tuple) and len(kwargs.get("replace")) == 2:
        old, new = kwargs.get("replace")
        text = text.replace(old, new)

    if kwargs.get("_func") and callable(kwargs.get("_func")):
        text = kwargs.get("_func")(text)
        if text is None:
            raise TypeError(f"Function {kwargs.get('_func')} returned None instead of str")
    return text

def send(message:str, **kwargs):
    # Initialize Slack client with token
    client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

    message = prepare_message(message, _func=kwargs.pop('_func', None), **kwargs)

    try:
        response = client.chat_postMessage(
            channel=os.getenv("SLACK_CHANNEL"),
            text=message,
            **kwargs
        )
        log.debug(response)
        if response.get("ok"):
            log.info(f"Message sent to channelID {response.get('channel')}")
        return True
    except SlackApiError as e:
        log.exception(f"Error sending message:\n{e.response['error']}")
        return False