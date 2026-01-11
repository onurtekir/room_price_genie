from datetime import datetime
from pathlib import Path
from typing import Dict, Any

from rpg.db_engine.db_engine_base import DBEngineBase
from rpg.extract.api_extract_engine import ApiExtractEngine
from rpg.extract.extract_engine_base import ExtractEngineBase
from rpg.extract.local_extract_engine import LocalExtractEngine
from rpg.pipeline.scheduler import Scheduler
from rpg.utils.datetime_util import format_datetime
from rpg.utils.logger import Logger


class Runner:

    def __init__(self, config :Dict[Any, Any], db_engine: DBEngineBase):
        self._config = config
        self._db_engine = db_engine

    def run(self):
        self._run()

    def start(self, interval_minutes: int):
        scheduler = Scheduler(interval_minutes=interval_minutes,
                              runner_func=self.run)
        scheduler.start()

    def _init_extraction_engine(self) -> ExtractEngineBase:
        Logger.info("Initializing extraction engine...")
        source_type = self._config["source_type"]
        if source_type == "local":
            return LocalExtractEngine(configuration=self._config)
        elif source_type == "api":
            return ApiExtractEngine(configuration=self._config)
        else:
            raise ValueError(f"Source type '{source_type}' not supported!")

    def _run(self):
        """
        Start running ingestion
        """

        Logger.info("Ingestion started!")
        extraction_engine = self._init_extraction_engine()

        # region Inventory Ingestion
        inventory_extraction_result = extraction_engine.extract_inventory()
        if inventory_extraction_result:
            Logger.info("Processing inventory records...")
            inventory_file_info, df_inventory = inventory_extraction_result
            pre_query = "UPDATE inventory SET is_active=False"
            rows = df_inventory.to_dict(orient="records")
            rows_affected = self._db_engine.insert_rows(table_name="inventory",
                                                        rows=rows,
                                                        pre_query=pre_query)

            # region Move processed temporary file to success archive folder
            success_archive_path = Path(self._config["archive_path"]) / "success"
            success_archive_path.mkdir(parents=True, exist_ok=True)
            original_filename = Path(inventory_file_info['original_filename'])
            temporary_filepath = Path(inventory_file_info["temporary_filepath"])
            success_filename_suffix = format_datetime(value=datetime.now(),
                                                      pattern="%Y%m%d%H%S%M")
            success_filepath = success_archive_path / f"{original_filename.stem}__{success_filename_suffix}.{original_filename.suffix}"
            temporary_filepath.rename(success_filepath)
            # endregion

            Logger.success("Done!")

        # endregion

        # region Reservations Ingestion
        reservation_extraction_results = extraction_engine.extract_reservations()
        if reservation_extraction_results:
            Logger.info("Processing reservation records...")
            Logger.info(f"{len(reservation_extraction_results)} batch ingested!")
            for index,  extraction_result in enumerate(reservation_extraction_results):

                Logger.info(f"Processing Batch #{index + 1} of {len(reservation_extraction_results)}")

                reservations_file_info, df_imports, df_stay_dates, df_rejected_imports = extraction_result

                # region Rejected Rows
                Logger.info("Processing rejected reservations...")
                rejected_rows = df_rejected_imports.to_dict(orient="records")
                self._db_engine.insert_rows(table_name="rejected_imports",
                                            rows=rejected_rows)
                Logger.success("Done!")
                # endregion

                # region Reservations
                Logger.info("Processing reservations...")
                reservation_rows = df_imports.to_dict(orient="records")
                table_name = "reservation_imports"
                staging_table_name = "staging_reservation_imports"
                pre_query = f"""
                CREATE TEMP TABLE {staging_table_name} AS 
                SELECT * FROM {table_name} WHERE 1=0
                """

                post_query = f"""
                INSERT INTO {table_name}
                SELECT stg.*
                FROM {staging_table_name} AS stg
                LEFT JOIN {table_name} AS tbl
                ON tbl.reservation_hash = stg.reservation_hash
                WHERE tbl.reservation_hash IS NULL
                """

                self._db_engine.insert_rows(table_name=staging_table_name,
                                            pre_query=pre_query,
                                            post_query=post_query,
                                            rows=reservation_rows)
                Logger.success("Done!")
                # endregion

                # region Reservation Stay Dates
                Logger.info("Processing reservation stay dates...")
                stay_date_rows = df_stay_dates.to_dict(orient="records")
                table_name = "reservation_stay_dates"
                staging_table_name = "staging_reservation_stay_dates"

                pre_query = f"""
                CREATE TEMP TABLE {staging_table_name} AS 
                SELECT * FROM {table_name} WHERE 1=0
                """

                post_query = f"""
                INSERT INTO {table_name}
                SELECT stg.*
                FROM {staging_table_name} AS stg
                LEFT JOIN {table_name} AS tbl
                ON tbl.reservation_hash = stg.reservation_hash
                AND tbl.stay_date_hash = stg.stay_date_hash
                WHERE tbl.reservation_hash IS NULL
                """

                self._db_engine.insert_rows(table_name=staging_table_name,
                                            pre_query=pre_query,
                                            post_query=post_query,
                                            rows=stay_date_rows)
                Logger.success("Done!")
                # endregion

                # region Move processed temporary file to success archive folder
                success_archive_path = Path(self._config["archive_path"]) / "success"
                success_archive_path.mkdir(parents=True, exist_ok=True)
                original_filename = Path(reservations_file_info['original_filename'])
                temporary_filepath = Path(reservations_file_info["temporary_filepath"])
                success_filename_suffix = format_datetime(value=datetime.now(),
                                                          pattern="%Y%m%d%H%S%M")
                success_filepath = success_archive_path / f"{original_filename.stem}__{success_filename_suffix}.{original_filename.suffix}"
                temporary_filepath.rename(success_filepath)
                # endregion

                Logger.success("Done!")

        # endregion



