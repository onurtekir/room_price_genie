from abc import ABC, abstractmethod
from typing import Union, Optional, Any, Dict, List

import pandas as pd


class DBEngineBase(ABC):

    def __init__(self, database_configuration: Dict[Any, Any]):
        self._engine_name = "Not defined"
        self._database_configuration = database_configuration

    @property
    def engine_name(self) -> str:
        return self._engine_name

    @property
    def database_configuration(self) -> Dict[Any, Any]:
        return self._database_configuration

    @abstractmethod
    def validate_connection(self) -> bool:
        """
        Validate database connection
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, query: str, is_safe: Optional[bool] = True) -> Optional[Union[bool, int, pd.DataFrame]]:
        """
        Execute query (SELECT, DML, DDL) and return Pandas DataFrame for SELECT execution
        """
        raise NotImplementedError

    @abstractmethod
    def insert_rows(self,
                    table_name: str,
                    rows: List[Dict[Any, Any]],
                    pre_query: Optional[str] = None,
                    post_query: Optional[str] = None,
                    overwrite: Optional[bool] = False,
                    is_safe: Optional[bool] = True
                    ) -> int:
        """
        Insert List[Dict] rows into database table
        """
        raise NotImplementedError

    @abstractmethod
    def initialize_database(self):
        """
        Initialize database and create Table/View and Stored Procedures
        """
        raise NotImplementedError
