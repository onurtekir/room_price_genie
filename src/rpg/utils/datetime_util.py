from typing import Optional, Union
from datetime import datetime, date


def cast_datetime(value: Union[str, datetime, date, None],
                  pattern: Optional[str] = "%d.%m.%Y %H:%M:%S",
                  is_safe: Optional[bool] = False) -> Optional[datetime]:

    if value is None:
        return None

    try:

        if isinstance(value, str):
            return datetime.strptime(value, pattern)

        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())

        raise TypeError(f"Unsupported type: {type(value)}")

    except Exception as e:

        if is_safe:
            return None
        else:
            raise e

def cast_date(value: Union[str, datetime, date, None],
              pattern: Optional[str] = "%d.%m.%Y",
              is_safe: Optional[bool] = False) -> Optional[date]:

    if value is None:
        return None

    try:

        if isinstance(value, str):
            return datetime.strptime(value, pattern).date()

        if isinstance(value, datetime):
            return value.date()

        if isinstance(value, date):
            return value

        raise TypeError(f"Unsupported type: {type(value)}")

    except Exception as e:

        if is_safe:
            return None
        else:
            raise e

def format_datetime(value: Union[datetime, date, None],
                    pattern: Optional[str] = "%d.%m.%Y %H:%M:%S",
                    is_safe: Optional[bool] = False) -> Optional[str]:

    if value is None:
        return None

    try:

        if isinstance(value, (datetime, date)):
            return value.strftime(pattern)

        raise TypeError(f"Unsupported type: {type(value)}")

    except Exception as e:
        if is_safe:
            return None
        else:
            raise e

def format_now() -> str:
    return format_datetime(value=datetime.now(),pattern="%d.%m.%Y %H:%M:%S")

def valid_date(value: Union[str, date],
               pattern: Optional[str] = "%d.%m.%Y") -> bool:

    if isinstance(value, date) and not isinstance(value, datetime):
        return True

    try:
        cast_date(value=value, pattern=pattern)
        return  True
    except Exception:
        return False

def valid_datetime(value: Union[str, datetime],
                   pattern: Optional[str] = "%d.%m.%Y %H:%M:%S") -> bool:

    if isinstance(value, datetime):
        return True

    try:
        cast_datetime(value=value, pattern=pattern)
        return  True
    except Exception:
        return False