import json
from typing import Dict, Any, Optional

from rpg.db_engine.db_engine_base import DBEngineBase
from rpg.db_engine.db_engine_factory import load_db_engine
from rpg.utils.io_util import file_exists
from rpg.utils.logger import Logger
from rpg.utils.validation_util import validate_string


class PipelineContext:

    def __init__(self, config_filepath: str, read_only: Optional[bool] = False):
        self._config = None
        self._db_engine = None
        self._init_context(config_filepath=config_filepath, read_only=read_only)

    @property
    def config(self) -> Dict[Any, Any]:
        return self._config

    @property
    def db_engine(self) -> DBEngineBase:
        return self._db_engine

    def _init_context(self, config_filepath: str, read_only: Optional[bool] = False):
        self._config = self.load_config(config_path=config_filepath)
        self._db_engine = self._init_db(read_only=read_only)

    def load_config(self, config_path: str) -> Dict[Any, Any]:

        """
        Load and validate pipeline context configuration JSON file.
        """

        if not file_exists(filepath=config_path):
            raise FileNotFoundError(f"Configuration JSON file '{config_path}' not found!")

        # region Read configuration JSON file
        Logger.info("Loading pipeline configuration...")
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        # endregion

        Logger.info("Validating pipeline configuration...")

        # region Source Type

        if "source_type" not in config:
            raise KeyError("source_type not found in pipeline configuration file!")

        source_type = config["source_type"]
        allowed_source_types = ["local", "api"]
        valid, validation_error = validate_string(json_value={"source_type": source_type},
                                                  field_name="source_type",
                                                  allow_empty_string=False,
                                                  allowed_values=allowed_source_types)
        if not valid:
            raise ValueError(validation_error.message)

        # endregion

        # region Source Config
        if "source_config" not in config:
            raise KeyError("source_config not found in pipeline configuration file!")

        source_config = config["source_config"]

        if source_type == "local":
            # region Source Type = LOCAL

            # region inventory_path
            valid, validation_error = validate_string(json_value=source_config,
                                                      field_name="inventory_path",
                                                      allow_empty_string=False)
            if not valid:
                raise ValueError(validation_error.message)
            # endregion

            # region inventory_column_separator
            valid, validation_error = validate_string(json_value=source_config,
                                                      field_name="inventory_column_separator",
                                                      allow_empty_string=False)
            if not valid:
                raise ValueError(validation_error.message)
            # endregion

            # region reservations_path
            valid, validation_error = validate_string(json_value=source_config,
                                                      field_name="reservations_path",
                                                      allow_empty_string=False)
            if not valid:
                raise ValueError(validation_error.message)
            # endregion

            # endregion
        elif source_type == "api":
            # region Source Type = API

            # region base_url
            valid, validation_error = validate_string(json_value=source_config,
                                                      field_name="base_url",
                                                      allow_empty_string=False)
            if not valid:
                raise ValueError(validation_error.message)
            # endregion

            # region inventory_endpoint
            valid, validation_error = validate_string(json_value=source_config,
                                                      field_name="inventory_endpoint",
                                                      allow_empty_string=False)
            if not valid:
                raise ValueError(validation_error.message)
            # endregion

            # region reservations_endpoint
            valid, validation_error = validate_string(json_value=source_config,
                                                      field_name="reservations_endpoint",
                                                      allow_empty_string=False)
            if not valid:
                raise KeyError(validation_error.message)
            # endregion

            # endregion

        # endregion

        # region Database Config
        if "db_config" not in config:
            raise KeyError("db_config not found in pipeline configuration file!")

        db_config = config["db_config"]

        # region Engine Module
        valid, validation_error = validate_string(json_value=db_config,
                                                  field_name="engine_module",
                                                  allow_empty_string=False)
        if not valid:
            raise ValueError(validation_error.message)
        # endregion

        # region Engine Name
        valid, validation_error = validate_string(json_value=db_config,
                                                  field_name="engine_name",
                                                  allow_empty_string=False)
        if not valid:
            raise ValueError(validation_error.message)
        # endregion

        # endregion

        # region Archive Path
        if "archive_path" not in config:
            raise KeyError("archive_path not found in pipeline configuration file!")
        # endregion

        Logger.success("Done!")
        return config

    def _init_db(self, read_only: Optional[bool] = False) -> DBEngineBase:
        # region Load Database engine
        db_config = self._config["db_config"]
        engine_module = db_config.get("engine_module")
        engine_name = db_config.get("engine_name")
        Logger.info(f"Initializing '{engine_name}' database engine...")
        try:
            engine_class = load_db_engine(engine_module=engine_module,
                                          engine_name=engine_name)
            db_engine = engine_class(database_configuration=db_config)
            if not read_only:
                db_engine.initialize_database()
            return db_engine
        except Exception as e:
            Logger.error(message="Error initializing database engine!",
                         err=e,
                         include_stack_trace=True)
            raise
        # endregion