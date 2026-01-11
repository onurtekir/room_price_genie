import json
import hashlib
from typing import Any, Dict
from datetime import date, datetime


def normalize_value(value: Any) -> Any:

    if value is None:
        return None

    if isinstance(value, (bool, int, float)):
        return value

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, dict):
        return {k: normalize_value(v) for k, v in value.items()}

    if isinstance(value, list):
        return [normalize_value(i) for i in value]

    return str(value)

def calculate_row_hash(row: Dict[Any, Any]) -> str:
    normalized_row = normalize_value(row)
    str_row = json.dumps(
        normalized_row,
        sort_keys=True,
        ensure_ascii=False
    )
    hash_value = hashlib.sha256(str_row.encode("utf-8")).hexdigest()
    return hash_value
