import jinja2
from typing import Dict, Any, Optional


def apply_jinja_template(source_value: str, jinja_parameters: Dict[Any,Any]) -> str:
    template = jinja2.Template(source=source_value)
    return template.render(jinja_parameters)
