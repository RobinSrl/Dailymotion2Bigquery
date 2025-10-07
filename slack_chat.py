"""
Slack Chat Module - Notification and Error Handling Utilities
This module provides utilities for sending messages to Slack channels and handling exceptions:

Main functions:
- send(message, **kwargs): Sends a message to a Slack channel
- notify(): Decorator for exception handling and Slack notifications or alias of send()
- notify_on_exception(): Decorator for detailed exception handling
- notify_on_logging(): Decorator for logging notifications (TODO)

The module requires SLACK_BOT_TOKEN and SLACK_CHANNEL environment variables to be set.

"""
import enum, os
from typing import Callable, Any, Optional, Literal
import functools, os, logging, traceback
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

_log = logging.getLogger(__name__)
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL', 'C09JYN7M73J')


class TextLevel(enum.Enum):
    DEBUG = "🔍"
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "⛔"
    CRITICAL = "🔥"
    EXCEPTION = "❌"

    @staticmethod
    def get(level_name: str):
        enum = getattr(__class__, level_name.upper())
        return enum.name, enum.value


def _prepare_message(text: str, *,
                     text_level: Optional[TextLevel] = 'info',
                     **kwargs):
    text = text.strip(kwargs.get("strip", None))
    text = text[0].upper() + text[1:] if len(text) > 0 else text

    if kwargs.get("replace") and isinstance(kwargs.get("replace"), tuple) and len(kwargs.get("replace")) == 2:
        old, new = kwargs.get("replace")
        text = text.replace(old, new)

    if kwargs.get("_func") and callable(kwargs.get("_func")):
        text = kwargs.get("_func")(text)
        if text is None:
            raise TypeError(f"Function {kwargs.get('_func')} returned None instead of str")

    if text_level:
        name, emoji = TextLevel.get(str(text_level))
        text = f"{emoji} *{name}* {text}"

    return text


def send(message: str, *,
         channel,
         **kwargs):
    # Initialize Slack client with token
    client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

    message = _prepare_message(message, _func=kwargs.pop('_func', None), **kwargs)

    try:
        response = client.chat_postMessage(
            channel=channel,
            text=message,
            **kwargs
        )
        _log.debug(response)
        if response.get("ok"):
            _log.info(f"Message sent to channelID {response.get('channel')}")
        return True
    except SlackApiError as e:
        _log.exception(f"Error sending message:\n{e.response['error']}")
        return False


## DECORATOR
def notify_on_exception(func: Callable,
                        *,
                        silent: Optional[bool] = False,
                        message: Optional[str] = "",
                        **message_kwargs) -> Callable:
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except BaseException as e:

            tb = traceback.extract_tb(e.__traceback__)
            if tb:
                last_frame = tb[-1]
                filename = last_frame.filename.split('/')[-1]  # Solo il nome del file
                lineno = last_frame.lineno
            else:
                filename = "sconosciuto"
                lineno = "-1"

            error_msg = f"`{type(e).__name__}` [ _{filename}_ : {lineno} ] \n\n"
            error_msg += f"La funzione `{func.__name__}` ha generato un `{type(e).__name__}`:"
            error_msg += f"\t```{str(e)}\n{message}```\n"

            try:
                send(error_msg, channel=SLACK_CHANNEL, text_level='exception', **message_kwargs)
            except Exception as e:
                _log.exception(f"Error sending error message:\n{e}")
            if not silent:
                raise

    return wrapper


def notify_on_logging(func: Callable,
                      *,
                      level: int = logging.ERROR,
                      **message_kwargs) -> Callable:
    #TODO: Crea la funzione per intercettare e inviare il log
    raise NotImplementedError("The notify_on_logging function is not yet implemented")


def _notify_decorator(func: Callable, **kwargs) -> Callable:
    def decorator(function: Callable) -> Callable:
        original_function = function
        # if kwargs.pop("on_log", True):
        #     original_function = notify_on_logging(function, **kwargs)

        original_function = notify_on_exception(original_function, **kwargs)
        return original_function

    if func is not None:
        return decorator(func)
    return decorator


def notify(_: Optional[Callable | str] = None, **kwargs):
    """Send messages to Slack or decorate functions for exception handling.

    This function can be used in two ways:
    1. As a decorator: Wraps functions to catch exceptions and send notifications to Slack
    2. As a direct sender: Sends a message directly to the configured Slack channel

    Args:
        _: Either a callable (when used as decorator) or a string message (when used directly)
        **kwargs: Additional arguments passed to the Slack message
            - silent (bool): If True, suppresses re-raising of caught exceptions
            - message (str): Additional message to append to exception notifications
            - Any other kwargs are passed directly to the Slack API

    Returns:
        - When used as decorator: Returns decorated function
        - When used as sender: Returns True if message sent successfully, None if empty message

    Raises:
        TypeError: If the input is neither a Callable nor a string
    """

    if _ is None or (isinstance(_, Callable) and callable(_)):
        return _notify_decorator(func=_, **kwargs)

    elif isinstance(_, str):
        if _ and _.strip() != "" and not _.isspace():
            return send(_, channel=SLACK_CHANNEL, **kwargs)
        _log.warning('The sting must not be empty or only whitespaces')
        return None
    else:
        raise TypeError(f"This decorator accepts only Callable or str as argument not {type(_)} ")
