from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import duckdb
import pandas as pd
from rpg.db_engine.db_engine_base import DBEngineBase
from rpg.utils.io_util import read_text_file, list_files
from rpg.utils.logger import Logger


class DuckDBEngine(DBEngineBase):

    ENGINE_NAME = "DuckDBEngine"

    def __init__(self, database_configuration: Dict[Any, Any]):
        super().__init__(database_configuration)
        self._engine_name = self.ENGINE_NAME
        self._db_path = None
        self._init()

    @property
    def db_path(self) -> str:
        return self._db_path

    def _init(self):
        # region Check configuration and set DB path
        db_path = Path(self.database_configuration["db_path"])
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db_path = db_path
        # endregion

    def validate_connection(self) -> bool:
        try:
            Logger.info("Validating DuckDB connection...")
            with duckdb.connect(database=self.db_path) as conn:
                Logger.success("Success!")
                pass
            return True
        except Exception as e:
            Logger.error(message="Error DukDB connection",
                         err=e,
                         include_stack_trace=True)
            return False

    def initialize_database(self):
        Logger.info("Initializing tables...")
        sql_source_path = Path(__file__).parents[1] / "sql"
        sql_paths = [str(p) for p in list_files(filepath=str(sql_source_path))]

        with duckdb.connect(self.db_path) as conn:
            for sql_path in sql_paths:
                Logger.info(f"Running DDL query '{sql_path}'")
                query = read_text_file(filepath=sql_path)
                conn.execute(query)
                Logger.success("Done!")

        return True

    def execute(self,
                query: str,
                is_safe: Optional[bool] = True) -> Optional[Union[bool, int, pd.DataFrame]]:
        try:

            # region Execute query and process result (DML, DDL, SELECT)
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute(query=query)

                if result.description is not None:
                    # If the description is not None, then the query is SELECT query. Fetch all
                    return result.df()

                row_count = getattr(result, "rowcount", None) # Try to get row count for DMLs
                if row_count is not None:
                    return row_count

            # If DDL and has no errors, return True
            return True
            # endregion

        except Exception as e:
            Logger.error(message="Error executing query!",
                         err=e,
                         include_stack_trace=True)
            if is_safe:
                return False
            else:
                raise e

    def insert_rows(self,
                    table_name: str,
                    rows: List[Dict[Any, Any]],
                    pre_query: Optional[str] = None,
                    post_query: Optional[str] = None,
                    overwrite: Optional[bool] = False,
                    is_safe: Optional[bool] = True) -> int:

        try:

            # region If there is no rows, return 0
            if not rows or len(rows) == 0:
                return 0
            # endregion

            # region Prepare SQL query and parameters
            columns = rows[0].keys()
            values = [tuple(row[col] for col in columns) for row in rows]

            sql_value_params = ", ".join(["?"] * len(columns))
            sql_columns = ", ".join(columns)

            sql_statement = f"""
            INSERT INTO {table_name} ({sql_columns})
            VALUES ({sql_value_params})
            """
            # endregion

            # region Begin transaction and execute insert query to prevent missing inserts
            with duckdb.connect(self.db_path) as conn:
                conn.execute("BEGIN")

                try:

                    if overwrite:
                        conn.execute(f"TRUNCATE {table_name}")

                    if pre_query:
                        conn.execute(pre_query)

                    conn.executemany(query=sql_statement,
                                     parameters=values)

                    if post_query:
                        conn.execute(post_query)

                    conn.execute("COMMIT")

                except Exception as e:
                    conn.execute("ROLLBACK")
                    raise
            # endregion

            return len(rows)

        except Exception as e:

            Logger.error(message=f"Error inserting rows into '{table_name}'",
                         err=e,
                         include_stack_trace=True)
            if is_safe:
                return 0
            else:
                raise
