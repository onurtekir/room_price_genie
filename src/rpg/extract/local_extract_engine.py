import json
import os.path
from email.policy import default
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

from rpg.utils.datetime_util import cast_date, cast_datetime
from rpg.utils.hash_util import calculate_row_hash
from rpg.utils.io_util import file_exists
from rpg.utils.logger import Logger
from typing import List, Optional, Dict, Tuple, Any
from rpg.extract.extract_engine_base import ExtractEngineBase
from rpg.utils.validation_util import validate_int, validate_string, validate_date, validate_datetime, ValidationError, \
    validate_number


class LocalExtractEngine(ExtractEngineBase):

    # region INVENTORY EXTRACTION

    def extract_inventory(self) -> Optional[Tuple[Dict[str, Any], pd.DataFrame]]:
        try:

            # region Read configuration parameters
            inventory_path = Path(self.configuration["source_config"]["inventory_path"])
            archive_path = Path(self.configuration["archive_path"])
            temp_path = Path(os.path.join(archive_path, "tmp"))
            error_path = Path(os.path.join(archive_path, "error"))
            column_separator = self.configuration["source_config"]["inventory_column_separator"]
            row_separator = self.configuration["source_config"]["inventory_row_separator"]
            # endregion

            # region Create paths if not exists
            inventory_path.mkdir(parents=True, exist_ok=True)
            archive_path.mkdir(parents=True, exist_ok=True)
            temp_path.mkdir(parents=True, exist_ok=True)
            error_path.mkdir(parents=True, exist_ok=True)
            # endregion

            Logger.info("Loading inventory CSV file(s)...")
            csv_filenames = [p.name for p in inventory_path.glob("*.csv")]

            if len(csv_filenames) == 0:
                # region No CSV files found
                Logger.info("No inventory CSV files found!")
                return None
                # endregion
            elif len(csv_filenames) > 1:
                # region Multiple CSV files found
                # System should fail inventory loading process and move the files into error folder.
                # Because, if there are more than one inventory file, system cannot detect which one is the latest one
                # There is no identifier in the filename and/or CSV content
                # To protect the consistency of the system and prevent late-arrived CSV file, system should ignore them
                Logger.error(f"There are {len(csv_filenames)} in the inventory folder."
                             " There should be only ONE file in the folder for each process"
                             " Moving all CSV files into ERROR folder...")
                for csv_filename in csv_filenames:
                    Logger.info(f"Moving '{csv_filename}' into '{str(error_path)}'...")
                    csv_path = Path(os.path.join(inventory_path, csv_filename))
                    error_filepath = Path(
                        os.path.join(error_path,
                                     f"error_{csv_filename.split('.')[0]}_{str(datetime.now().timestamp()).replace('.', '_')}.csv")
                    )
                    csv_path.rename(error_filepath)
                    Logger.success("Done!")
                return None
                # endregion
            else:
                # region Validate and return inventory file
                csv_filename = csv_filenames[0]
                csv_path = Path(os.path.join(inventory_path, csv_filename))
                temp_filepath = Path(
                    os.path.join(temp_path,
                                 f"tmp_{csv_filename.split('.')[0]}_{str(datetime.now().timestamp()).replace('.', '_')}.csv")
                )
                csv_path.rename(temp_filepath)

                # region Validate CSV file
                is_valid = self.validate_inventory(filepath=temp_filepath,
                                                   column_separator=column_separator,
                                                   row_separator=row_separator)
                if not is_valid:
                    Logger.error(f"INVALID: Moving '{temp_filepath}' to error folder '{str(error_path)}'")
                    error_filepath = Path(str(temp_filepath).replace("tmp_", "error_", 1))
                    temp_filepath.rename(error_filepath)
                    Logger.success("Done!")
                    return None
                else:
                    Logger.success(f"VALID: Inventory file '{temp_filepath}' is valid.")
                    file_info = dict(original_filename=csv_filename,
                                     temporary_filepath=temp_filepath)
                    return file_info, self.inventory_to_dataframe(file_info=file_info,
                                                                  column_separator=column_separator,
                                                                  row_seperator=row_separator)
                # endregion

                # endregion

        except Exception as e:
            Logger.error(message="Error loading inventory CSV file",
                         err=e,
                         include_stack_trace=True)
            return None

    def inventory_to_dataframe(self, file_info: Dict[str, str], column_separator: str, row_seperator: str) -> pd.DataFrame:
        df = pd.read_csv(file_info["temporary_filepath"], sep=column_separator, lineterminator=row_seperator)
        df = df[["hotel_id", "room_type_id", "quantity"]]
        df["hotel_id"] = pd.to_numeric(df["hotel_id"]).astype(int)
        df["quantity"] = pd.to_numeric(df["quantity"]).astype(int)
        df["ingested_at"] = datetime.now()
        df["source_filename"] = file_info["original_filename"]
        df["is_active"] = True
        return df

    def validate_inventory(self,
                           filepath: Path,
                           column_separator: str,
                           row_separator: str,
                           ignore_empty_lines: Optional[bool] = True) -> bool:

        # TODO: Document it and describe, loading and validating row by row to allow huge files

        is_header = True
        column_names = []
        row_index = 0

        hotel_id_index = -1
        room_type_id_index = -1
        quantity_index = -1

        with open(str(filepath), "r") as f:

            for line in f:

                line = line.rstrip(row_separator).strip()

                if line == "" and ignore_empty_lines:
                    continue

                if is_header:

                    # region Validate header columns
                    expected_column_names = ["hotel_id", "room_type_id", "quantity"]
                    column_names = line.split(column_separator)
                    if not set(expected_column_names).issubset(column_names):
                        Logger.error(message=f"'{filepath}' should have the columns {', '.join(expected_column_names)}")
                        return False
                    # endregion

                    # region Find the column indexes
                    hotel_id_index = column_names.index("hotel_id")
                    room_type_id_index = column_names.index("room_type_id")
                    quantity_index = column_names.index("quantity")
                    # endregion

                    is_header = False
                else:
                    values = line.split(column_separator)

                    if len(values) < len(column_names):
                        Logger.error(
                            message=f"Invalid row. Row {row_index} column count should be {len(column_names)}, but have {len(values)} columns"
                        )
                        return False

                    # region Validate hotel_id
                    hotel_id = values[hotel_id_index]
                    valid, validation_error = validate_string(json_value={"hotel_id": hotel_id},
                                                              field_name="hotel_id",
                                                              allow_empty_string=False)
                    if not valid:
                        Logger.error(message=f"Invalid hotel_id. {validation_error.message}")
                        return False
                    # endregion

                    # region Validate room_type_id
                    room_type_id = values[room_type_id_index]
                    valid, validation_error = validate_string(json_value={"room_type_id": room_type_id},
                                                              field_name="room_type_id",
                                                              allow_empty_string=False)
                    if not valid:
                        Logger.error(message=f"Invalid room_type_id. {validation_error.message}")
                        return False
                    # endregion

                    # region Validate quantity
                    quantity = values[quantity_index]
                    valid, validation_error = validate_int(json_value={"quantity": quantity},
                                                           field_name="quantity",
                                                           min_value=0)
                    if not valid:
                        Logger.error(message=f"Invalid room_type_id. {validation_error.message}")
                        return False
                    # endregion

        return True

    # endregion

    # region RESERVATION EXTRACTION

    def extract_reservations(self) -> Optional[List[Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]]]:

        # region Read configuration parameters
        reservations_path = Path(self.configuration["source_config"]["reservations_path"])
        archive_path = Path(self.configuration["archive_path"])
        temp_path = Path(os.path.join(archive_path, "tmp"))
        error_path = Path(os.path.join(archive_path, "error"))
        # endregion

        # region Create paths if not exists
        reservations_path.mkdir(parents=True, exist_ok=True)
        archive_path.mkdir(parents=True, exist_ok=True)
        temp_path.mkdir(parents=True, exist_ok=True)
        error_path.mkdir(parents=True, exist_ok=True)
        # endregion

        Logger.info("Loading reservations JSON file(s)...")
        json_filenames = [p.name for p in reservations_path.glob("*.json")]

        if len(json_filenames) == 0:
            # region No CSV files found
            Logger.info("No reservations JSON files found!")
            return None
            # endregion
        else:

            validated_reservations: List[Dict[str, Any]] = []

            ingested_reservations: List[Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]] = []

            for json_filename in json_filenames:

                json_path = Path(os.path.join(reservations_path, json_filename))
                temp_filepath = Path(
                    os.path.join(temp_path,
                                 f"tmp_{json_filename.split('.')[0]}_{str(datetime.now().timestamp()).replace('.', '_')}.json")
                )
                json_path.rename(temp_filepath)

                validation_result = self.validate_reservation(filepath=temp_filepath)
                if validation_result:
                    valid_rows, invalid_rows = validation_result
                    validated_reservations.append(dict(
                        filename=json_filename,
                        valid_rows=valid_rows,
                        invalid_rows=invalid_rows,
                        error=None
                    ))
                else:
                    validated_reservations.append(dict(
                        filename=json_filename,
                        error=ValidationError(message="Error reading JSON file")
                    ))

                df_imports, df_stay_dates, df_rejected_imports = self.reservations_to_dataframe(validated_reservations)
                ingested_reservations.append(
                    (
                        dict(original_filename=json_filename,
                             temporary_filepath=temp_filepath),
                        df_imports,
                        df_stay_dates,
                        df_rejected_imports
                    )
                )

            return ingested_reservations

    def reservations_to_dataframe(self,
                                  reservation_imports: List[Dict[str, Any]]
                                  ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        import_rows = []
        stay_date_rows = []
        rejected_rows = []

        # region Create rows for reservation_imports and reservation_stay_dates
        Logger.info("Generating import rows...")
        for reservation_import in reservation_imports:

            pre_process_error = reservation_import.get("error", None)
            if  pre_process_error:
                Logger.warning(f"Reservation import error. {pre_process_error}. Skipping...")
                continue

            # region Generate VALID rows

            for reservation in reservation_import["valid_rows"]:

                reservation_hash = calculate_row_hash(row=reservation)

                # region Generate reservation_imports row
                import_rows.append(dict(
                    hotel_id=reservation["hotel_id"],
                    reservation_id=reservation["reservation_id"],
                    status=reservation["status"],
                    arrival_date=cast_date(value=reservation["arrival_date"],
                                           pattern="%Y-%m-%d"),
                    departure_date=cast_date(value=reservation["departure_date"],
                                             pattern="%Y-%m-%d"),
                    source_name=reservation.get("source_name", None),
                    source_id=reservation.get("source_id", None),
                    created_at=cast_datetime(value=reservation["created_at"],
                                             pattern="%Y-%m-%d %H:%M:%S.%f"),
                    updated_at = cast_datetime(value=reservation["updated_at"],
                                               pattern="%Y-%m-%d %H:%M:%S.%f"),
                    source_filename=reservation_import["filename"],
                    ingested_at=datetime.now(),
                    reservation_hash=reservation_hash
                ))
                # endregion

                # region Generate reservation_stay_dates rows
                for stay_date in reservation["stay_dates"]:
                    stay_date_rows.append(dict(
                        hotel_id=reservation["hotel_id"],
                        reservation_id=reservation["reservation_id"],
                        start_date=cast_date(value=stay_date["start_date"],
                                            pattern="%Y-%m-%d"),
                        end_date=cast_date(value=stay_date["end_date"],
                                           pattern="%Y-%m-%d"),
                        room_type_id=stay_date["room_type_id"],
                        room_type_name=stay_date["room_type_name"],
                        number_of_adults=int(stay_date["number_of_adults"]),
                        number_of_children=int(stay_date["number_of_children"]),
                        revenue_gross_amount=float(stay_date["room_revenue_gross_amount"]),
                        revenue_net_amount=float(stay_date["room_revenue_net_amount"]),
                        fnb_gross_amount=float(
                            stay_date["fnb_gross_amount"]) if "fnb_gross_amount" in stay_date else None,
                        fnb_net_amount=float(
                            stay_date["fnb_net_amount"]) if "fnb_net_amount" in stay_date else None,
                        created_at=cast_datetime(value=reservation["created_at"],
                                                 pattern="%Y-%m-%d %H:%M:%S.%f"),
                        updated_at=cast_datetime(value=reservation["updated_at"],
                                                 pattern="%Y-%m-%d %H:%M:%S.%f"),
                        ingested_at=datetime.now(),
                        reservation_hash=reservation_hash,
                        stay_date_hash=calculate_row_hash(row=stay_date)
                    ))
                # endregion

            # endregion

            # region Generate INVALID rows
            for rejected_reservation in reservation_import["invalid_rows"]:
                rejected_rows.append(dict(
                    rejected_row=rejected_reservation["row"],
                    validation_errors=rejected_reservation["validation_errors"],
                    source_filename=reservation_import["filename"],
                    ingested_at=datetime.now()
                ))
            # endregion

        Logger.success("Done!")
        # endregion

        # region Generate Import DataFrames
        Logger.info("Generating imports DataFrames...")
        reservation_imports_columns = [
            "hotel_id",
            "reservation_id",
            "status",
            "arrival_date",
            "departure_date",
            "source_name",
            "source_id",
            "created_at",
            "updated_at",
            "source_filename",
            "ingested_at",
            "reservation_hash"
        ]
        reservation_stay_dates_columns = [
            "hotel_id",
            "reservation_id",
            "start_date",
            "end_date",
            "room_type_id",
            "room_type_name",
            "number_of_adults",
            "number_of_children",
            "revenue_gross_amount",
            "revenue_net_amount",
            "fnb_gross_amount",
            "fnb_net_amount",
            "created_at",
            "updated_at",
            "ingested_at",
            "reservation_hash",
            "stay_date_hash"
        ]
        rejected_imports_columns = [
            "rejected_row",
            "validation_errors",
            "source_filename",
            "ingested_at"
        ]

        df_reservation_imports = pd.DataFrame(columns=reservation_imports_columns, data=import_rows).replace({np.nan: None})
        df_reservation_stay_dates = pd.DataFrame(columns=reservation_stay_dates_columns, data=stay_date_rows).replace({np.nan: None})
        df_rejected_imports = pd.DataFrame(columns=rejected_imports_columns, data=rejected_rows).replace({np.nan: None})
        Logger.success("Done!")
        # endregion

        return df_reservation_imports, df_reservation_stay_dates, df_rejected_imports

    def validate_reservation(self, filepath: Path) -> Optional[Tuple[List[Dict[Any, Any]], List[Dict[Any, Any]]]]:

        if not file_exists(filepath=str(filepath)):
            Logger.error(message=f"Reservations JSON file '{str(filepath)}' not found")

        try:

            # region Read JSON file content
            Logger.info(f"Reading '{str(filepath)}' JSON file...")
            with open(str(filepath), "r", encoding="utf-8") as f:
                data = json.load(f)
                if "data" not in data:
                    Logger.error(f"Reservations list not found in JSON file")
                    return False
                else:
                    data = data["data"]
            Logger.success(f"Done! {len(data)} rows loaded!")
            # endregion

            valid_reservations = []
            invalid_reservations = []

            # region Ingestion and Business level validations

            for res in data:

                reservation_validation_errors: List[ValidationError] = []

                # region INGESTION LEVEL Validations

                # region hotel_id
                valid, validation_error = validate_string(json_value=res,
                                                          field_name="hotel_id",
                                                          allow_empty_string=False)
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # region reservation_id
                valid, validation_error = validate_string(json_value=res,
                                                          field_name="reservation_id",
                                                          allow_empty_string=False)
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # region status
                valid, validation_error = validate_string(json_value=res,
                                                          field_name="status",
                                                          allow_empty_string=False,
                                                          allowed_values=[
                                                              "provisional",
                                                              "waiting_list",
                                                              "confirmed",
                                                              "cancelled",
                                                              "no_show",
                                                              "checked_in",
                                                              "checked_out"
                                                          ])
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # region departure_date
                valid, validation_error = validate_date(json_value=res,
                                                        field_name="departure_date",
                                                        pattern="%Y-%m-%d")
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # region arrival_date
                valid, validation_error = validate_date(json_value=res,
                                                        field_name="arrival_date",
                                                        pattern="%Y-%m-%d")
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # region created_at
                valid, validation_error = validate_datetime(json_value=res,
                                                            field_name="created_at",
                                                            pattern="%Y-%m-%d %H:%M:%S.%f")
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # region updated_at
                valid, validation_error = validate_datetime(json_value=res,
                                                            field_name="updated_at",
                                                            pattern="%Y-%m-%d %H:%M:%S.%f")
                if not valid:
                    reservation_validation_errors.append(validation_error)
                # endregion

                # endregion

                # region If there is no ingestion level validation errors, run BUSINESS LEVEL validations
                if not reservation_validation_errors:

                    # region arrival_date should be less than departure_date
                    arrival_date = cast_date(value=res["arrival_date"], pattern="%Y-%m-%d")
                    departure_date = cast_date(value=res["departure_date"], pattern="%Y-%m-%d")
                    if not arrival_date < departure_date:
                        reservation_validation_errors.append(ValidationError(
                            message=f"arrival_date '{arrival_date}' should be less than departure_date '{departure_date}'",
                            field_name="arrival_date",
                            value=arrival_date
                        ))
                    # endregion

                    # region updated_at should be greater than or equal to created_at
                    created_at = cast_datetime(value=res["created_at"],
                                               pattern="%Y-%m-%d %H:%M:%S.%f")
                    updated_at = cast_datetime(value=res["updated_at"],
                                               pattern="%Y-%m-%d %H:%M:%S.%f")
                    if not updated_at >= created_at:
                        reservation_validation_errors.append(ValidationError(
                            message=f"updated_at '{updated_at}' should be greater than or equal to created_at '{created_at}'",
                            field_name=updated_at,
                            value=updated_at
                        ))
                    # endregion

                # endregion

                # region stay_dates
                valid_stay_dates = []
                invalid_stay_dates = []

                if "stay_dates" not in res or not isinstance(res["stay_dates"], list) or len(res["stay_dates"]) == 0:
                    reservation_validation_errors.append(ValidationError(message="stay_dates missing or invalid"))
                else:
                    stay_dates = res["stay_dates"]

                    for stay_date in stay_dates:

                        stay_dates_validation_errors: List[ValidationError] = []

                        # region INGESTION LEVEL Validations

                        # region start_date
                        valid, validation_error = validate_date(json_value=stay_date,
                                                                field_name="start_date",
                                                                pattern="%Y-%m-%d")
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region end_date
                        valid, validation_error = validate_date(json_value=stay_date,
                                                                field_name="end_date",
                                                                pattern="%Y-%m-%d")
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region room_type_id
                        valid, validation_error = validate_string(json_value=stay_date,
                                                                  field_name="room_type_id",
                                                                  allow_empty_string=False)
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region room_type_name
                        valid, validation_error = validate_string(json_value=stay_date,
                                                                  field_name="room_type_name",
                                                                  allow_empty_string=False)
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region number_of_adults
                        valid, validation_error = validate_int(json_value=stay_date,
                                                               field_name="number_of_adults",
                                                               min_value=1)
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region number_of_children
                        valid, validation_error = validate_int(json_value=stay_date,
                                                               field_name="number_of_children",
                                                               min_value=0)
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region room_revenue_gross_amount
                        valid, validation_error = validate_number(json_value=stay_date,
                                                                  field_name="room_revenue_gross_amount",
                                                                  allow_int=True)
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region room_revenue_net_amount
                        valid, validation_error = validate_number(json_value=stay_date,
                                                                  field_name="room_revenue_net_amount",
                                                                  allow_int=True)
                        if not valid:
                            stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region fnb_gross_amount
                        if "fnb_gross_amount" in stay_date:
                            valid, validation_error = validate_number(json_value=stay_date,
                                                                      field_name="fnb_gross_amount",
                                                                      allow_int=True)
                            if not valid:
                                stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # region fnb_net_amount
                        if "fnb_net_amount" in stay_date:
                            valid, validation_error = validate_number(json_value=stay_date,
                                                                      field_name="fnb_net_amount",
                                                                      allow_int=True)
                            if not valid:
                                stay_dates_validation_errors.append(validation_error)
                        # endregion

                        # endregion

                        # region If there is no ingestion level validation error, run BUSINESS LEVEL validations

                        if not stay_dates_validation_errors:

                            # region start_date should be less than or equal to end_date
                            start_date = cast_date(value=stay_date["start_date"], pattern="%Y-%m-%d")
                            end_date = cast_date(value=stay_date["end_date"], pattern="%Y-%m-%d")
                            if not start_date <= end_date:
                                stay_dates_validation_errors.append(ValidationError(
                                    message=f"start_date '{start_date}' should be less than or equal to end_date '{end_date}'",
                                    field_name="start_date",
                                    value=stay_date
                                ))
                            # endregion

                            # region All dates must be fall within the reservation_period
                            arrival_date = cast_date(value=res.get("arrival_date", None),
                                                     pattern="%Y-%m-%d",
                                                     is_safe=True)
                            departure_date = cast_date(value=res.get("departure_date", None),
                                                       pattern="%Y-%m-%d",
                                                       is_safe=True)

                            if arrival_date is None or departure_date is None:
                                stay_dates_validation_errors.append(ValidationError(
                                    message=f"All dates must be fall within reservation period."
                                    "Invalid arrival_date and/or departure_date"
                                ))
                            else:
                                if not(start_date >= arrival_date and end_date <= departure_date):
                                    stay_dates_validation_errors.append(ValidationError(
                                        message="All dates must be fall within reservation period."
                                        f"'{start_date}' and '{end_date}' not fall into '{arrival_date}' and '{departure_date}'"
                                    ))

                            # endregion

                        # endregion

                        if stay_dates_validation_errors:
                            invalid_stay_dates.append(dict(stay_date=stay_date,
                                                           validation_errors=[v.to_dict() for v in stay_dates_validation_errors]))
                        else:
                            valid_stay_dates.append(stay_date)

                # endregion

                if reservation_validation_errors:
                    # region Reservation is invalid, generate stay_dates VALID + INVALID stay_dates
                    res["stay_dates"] = invalid_stay_dates + [dict(row=r, validation_errors=None) for r in valid_stay_dates]
                    invalid_reservations.append(dict(row=res,
                                                     validation_errors=[v.to_dict() for v in reservation_validation_errors]))
                    # endregion
                else:
                    # region Reservation is valid, generate stay_dates just by using VALID stay_dates
                    # Generate new dictionary with valid stay_dates to prevent overwrite the object reference
                    res_valid = {**res, "stay_dates": valid_stay_dates}
                    valid_reservations.append(res_valid)
                    # endregion

                    # region If there is at least one invalid stay_date, add reservation to invalid reservations with invalid stay_date
                    if invalid_stay_dates:
                        # Generate new dictionary with invalid stay_dates to prevent overwrite the object reference
                        res_invalid = {**res, "stay_dates": invalid_stay_dates}
                        invalid_reservations.append(dict(
                            row=res_invalid,
                            validation_errors=[]
                        ))
                    # endregion

            # endregion

            return valid_reservations, invalid_reservations

        except Exception as e:
            Logger.error(message=f"Error reading '{str(filepath)}' JSON file",
                         err=e,
                         include_stack_trace=True)
            return None

    # endregion
