from typing import Callable, Any, Optional
import functools, os, logging, traceback
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

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


## DECORATOR
def notify_on_exception(func: Callable,
                        *,
                        silent:Optional[bool]=False,
                        suppress:Optional[bool]=False,
                        message:Optional[str]="") -> Callable:

    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:

            tb = traceback.extract_tb(e.__traceback__)
            if tb:
                last_frame = tb[-1]
                filename = last_frame.filename.split('/')[-1]  # Solo il nome del file
                lineno = last_frame.lineno
            else:
                filename = "sconosciuto"
                lineno = "-1"

            error_msg = f"❌ *Errore* `{type(e).__name__}` [ _app/{filename}_ : {lineno} ] \n\n"
            error_msg += f"La funzione `{func.__name__}` ha generato un `{type(e).__name__}`:"
            error_msg += f"\t```{str(e)}\n{message}```\n"

            try:
                send(error_msg)
            except Exception as e:
                log.exception(f"Error sending error message:\n{e}")
            if not silent and not suppress:
                raise

    return wrapper

def notify_on_logging(func: Callable,
                      *,
                      level: int = logging.ERROR
                      ) -> Callable:
    #TODO: Crea la funzione per intercettare e inviare il logg
    pass


def notify(_function:Callable, **kwargs):

    def decorator(function:Callable) -> Callable:
        original_function = function
        # if kwargs.pop("on_log", True):
        #     original_function = notify_on_logging(function, **kwargs)

        original_function = notify_on_exception(original_function, **kwargs)
        return original_function

    if _function is not None:
        return decorator(_function)

    return decorator