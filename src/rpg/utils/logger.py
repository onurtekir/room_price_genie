import traceback
from typing import Optional
from .datetime_util import format_now


class Logger:

    @classmethod
    def _log(cls, log_type: str, message: str):
        type_string = f"[{log_type}]".ljust(9)
        print(f"{format_now().ljust(20)} {type_string} : {message}")

    @classmethod
    def info(cls, message: str):
        cls._log(log_type="INFO", message=message)

    @classmethod
    def warning(cls, message: str):
        cls._log(log_type="WARNING", message=message)

    @classmethod
    def success(cls, message: str):
        cls._log(log_type="SUCCESS", message=message)

    @classmethod
    def error(cls, message: str, err: Optional[Exception] = None, include_stack_trace: Optional[bool] = False):
        cls._log(log_type="ERROR", message=message)
        if include_stack_trace:
            print(traceback.print_exception(type(err), err, err.__traceback__))