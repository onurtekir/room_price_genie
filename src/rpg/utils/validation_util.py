import math
from datetime import datetime, date
from typing import Optional, Any, Dict, Tuple, List
from rpg.utils.datetime_util import valid_date, cast_date, valid_datetime, cast_datetime



class ValidationError(ValueError):

    def __init__(self,
                 message: str,
                 field_name: Optional[str] = None,
                 value: Optional[Any] = None,
                 metadata: Optional[Dict[Any, Any]] = None):
        super().__init__(f"{field_name} : {message}")
        self._message = message
        self._field_name = field_name
        self._value = value
        self._metadata = metadata or {}

    @property
    def field_name(self) -> Optional[str]:
        return self._field_name

    @property
    def message(self) -> str:
        return self._message

    @property
    def value(self) -> Optional[Any]:
        return self._value

    @property
    def metadata(self) -> Dict[Any, Any]:
        return self._metadata

    def to_dict(self) -> Dict[str, Any]:
        return dict(
            message=self.message,
            field_name=self.field_name,
            value=self.value,
            metadata=self.metadata
        )


def validate_int(json_value: Dict[Any, Any],
                 field_name: str,
                 min_value: Optional[int] = None,
                 max_value: Optional[int] = None) -> Tuple[bool, Optional[ValidationError]]:

    metadata = dict(min_value=min_value, max_value=max_value)

    if json_value is None:
        return False, ValidationError(field_name=field_name,
                                      message="Value is NULL!",
                                      metadata=metadata)

    if field_name not in json_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is missing",
                                      metadata=metadata)

    field_value = json_value[field_name]

    if isinstance(field_value, bool):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be an integer",
                                      value=field_value,
                                      metadata=metadata)

    try:
        if isinstance(field_value, str):
            field_value = field_value.strip()
            if field_value == "":
                raise ValueError("Empty string")

        value = int(field_value)

    except (ValueError, TypeError):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be an integer or integer-like string",
                                      value=field_value,
                                      metadata=metadata)

    if min_value is not None and value < min_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} field value {value} must be >= {min_value}",
                                      value=value,
                                      metadata=metadata)

    if max_value is not None and value > max_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} field value {value} must be <= {max_value}",
                                      value=value,
                                      metadata=metadata)

    return True, None

def validate_number(json_value: Dict[Any, Any],
                    field_name: str,
                    min_value: Optional[float] = None,
                    max_value: Optional[float] = None,
                    allow_int: Optional[bool] = True) -> Tuple[bool, Optional[ValidationError]]:

    metadata = dict(min_value=min_value, max_value=max_value, allow_int=allow_int)

    if json_value is None:
        return False, ValidationError(field_name=field_name,
                                      message="Value is NULL!",
                                      metadata=metadata)

    if field_name not in json_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is missing",
                                      metadata=metadata)

    field_value = json_value[field_name]

    if field_value is None:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is NULL",
                                      value=field_value,
                                      metadata=metadata)

    if isinstance(field_value, bool):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be a number",
                                      value=field_value,
                                      metadata=metadata)

    if isinstance(field_value, (float, int)):
        value = float(field_value)
    else:
        if isinstance(field_value, str):
            field_value = field_value.strip()
            if field_value == "":
                return False, ValidationError(field_name=field_name,
                                              message=f"{field_name} must be a number",
                                              value=field_value,
                                              metadata=metadata)
        try:
            value = float(field_value)
        except (TypeError, ValueError):
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} must be a number",
                                          value=field_value,
                                          metadata=metadata)

    if not math.isfinite(value):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be finite number",
                                      value=field_value,
                                      metadata=metadata)

    if not allow_int and value.is_integer():
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be non-integer number",
                                      value=field_value,
                                      metadata=metadata)

    if min_value is not None and value < min_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} field value {value} must be >= {min_value}",
                                      value=value,
                                      metadata=metadata)

    if max_value is not None and value > max_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} field value {value} must be <= {max_value}",
                                      value=value,
                                      metadata=metadata)

    return True, None

def validate_string(json_value: Dict[Any, Any],
                    field_name: str,
                    allow_empty_string: Optional[bool] = True,
                    allowed_values: Optional[List[str]] = None) -> Tuple[bool, Optional[ValidationError]]:

    metadata = dict(allow_empty_string=allow_empty_string, allowed_values=allowed_values)

    if json_value is None:
        return False, ValidationError(field_name=field_name,
                                      message="Value is NULL!",
                                      metadata=metadata)

    if field_name not in json_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is missing",
                                      metadata=metadata)

    field_value = json_value[field_name]

    if not isinstance(field_value, str):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be a string",
                                      value=field_value,
                                      metadata=metadata)

    if not allow_empty_string and field_value.strip() == "":
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must NOT be empty string",
                                      value=field_value,
                                      metadata=metadata)


    if allowed_values is not None and field_value not in allowed_values:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be one of {', '.join(allowed_values)}",
                                      value=field_value,
                                      metadata=metadata)

    return True, None

def validate_boolean(json_value: Dict[Any, Any],
                     field_name: str) -> Tuple[bool, Optional[ValidationError]]:

    if json_value is None:
        return False, ValidationError(field_name=field_name,
                                      message="Value is NULL!")

    if field_name not in json_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is missing")

    field_value = json_value[field_name]

    if isinstance(field_value, str):
        field_value = field_value.strip()
        if field_value == "":
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} must be a boolean or boolean-like")
        elif field_value.lower() not in ["true", "false"]:
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} must be a boolean or boolean-like")
    elif not isinstance(field_value, bool):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be a boolean")

    return True, None

def validate_date(json_value: Dict[Any, Any],
                  field_name: str,
                  pattern: Optional[str] = "%d.%m.%Y",
                  min_date: Optional[date] = None,
                  max_date: Optional[date] = None) -> Tuple[bool, Optional[ValidationError]]:

    metadata=dict(pattern=pattern,
                  min_date=min_date.isoformat() if min_date else None,
                  max_date=max_date.isoformat() if max_date else None)

    if json_value is None:
        return False, ValidationError(field_name=field_name,
                                      message="Value is NULL!",
                                      metadata=metadata)

    if field_name not in json_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is missing",
                                      metadata=metadata)

    field_value = json_value[field_name]

    if isinstance(field_value, datetime) or not isinstance(field_value, (str, date)):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be a date string or date (not datetime)",
                                      value=field_value,
                                      metadata=metadata)

    if isinstance(field_value, (str, date)) and not valid_date(value=field_value, pattern=pattern):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be valid date value",
                                      value=field_value,
                                      metadata=metadata)

    if min_date is not None or max_date is not None:
        date_value = cast_date(value=field_value, pattern=pattern)

        if min_date is not None and date_value < min_date:
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} field value {date_value} must be >= {min_date}",
                                          value=field_value,
                                          metadata=metadata)

        if max_date is not None and date_value > max_date:
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} field value {date_value} must be <= {max_date}",
                                          value=field_value,
                                          metadata=metadata)

    return True, None

def validate_datetime(json_value: Dict[Any, Any],
                      field_name: str,
                      pattern: Optional[str] = "%d.%m.%Y %H:%M:%S",
                      min_datetime: Optional[datetime] = None,
                      max_datetime: Optional[datetime] = None) -> Tuple[bool, Optional[ValidationError]]:

    metadata=dict(pattern=pattern,
                  min_date=min_datetime.isoformat() if min_datetime else None,
                  maxn_date=max_datetime.isoformat() if max_datetime else None)

    if json_value is None:
        return False, ValidationError(field_name=field_name,
                                      message="Value is NULL!",
                                      metadata=metadata)

    if field_name not in json_value:
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} is missing",
                                      metadata=metadata)

    field_value = json_value[field_name]

    if not isinstance(field_value, (str, datetime)):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be a datetime string or datetime",
                                      value=field_value,
                                      metadata=metadata)

    if not valid_datetime(value=field_value, pattern=pattern):
        return False, ValidationError(field_name=field_name,
                                      message=f"{field_name} must be valid datetime value",
                                      value=field_value,
                                      metadata=metadata)

    if min_datetime is not None or max_datetime is not None:
        datetime_value = cast_datetime(value=field_value, pattern=pattern)

        if min_datetime is not None and datetime_value < min_datetime:
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} field value {datetime_value} must be >= {min_datetime}",
                                          value=field_value,
                                          metadata=metadata)

        if max_datetime is not None and datetime_value > max_datetime:
            return False, ValidationError(field_name=field_name,
                                          message=f"{field_name} field value {datetime_value} must be <= {max_datetime}",
                                          value=field_value,
                                          metadata=metadata)

    return True, None