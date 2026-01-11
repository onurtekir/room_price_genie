import importlib
from typing import Type

from rpg.db_engine.db_engine_base import DBEngineBase


def load_db_engine(engine_module: str, engine_name: str) -> Type[DBEngineBase]:

    module_name = engine_module
    module = importlib.import_module(module_name)
    engine_class = getattr(module, engine_name)

    if not issubclass(engine_class, DBEngineBase):
        raise TypeError(f"{engine_module} is not a valid DBEngineBase")

    return engine_class