import json
from jsonschema import validate, ValidationError
from typing import Dict, Any, Union, List, Optional, Tuple


def read_json(filepath: str) -> Optional[Union[Dict[Any, Any], List[Dict[Any, Any]]]]:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_json(json_value: Dict[Any, Any],
                  schema: Dict[Any, Any],
                  generate_validation_report: Optional[bool] = False) -> Union[bool, Tuple[bool, ValidationError]]:
    try:
        validate(instance=json_value, schema=schema)
        return True
    except ValidationError as ve:
        if generate_validation_report:
            return False, ve
        else:
            return False
    except Exception as e:
        if generate_validation_report:
            return False, ValidationError(message="Unknow validation error!")
        else:
            return False