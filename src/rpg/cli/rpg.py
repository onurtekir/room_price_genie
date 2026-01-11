import argparse
from pathlib import Path
from datetime import date
from typing import Optional, List

from rpg.pipeline.kpi_calculator import KpiCalculator
from rpg.pipeline.pipeline import Pipeline
from rpg.utils.datetime_util import cast_date, format_datetime
from rpg.utils.io_util import read_text_file


def show_logo():
    try:
        logo_filepath = Path(__file__).parent / "art/logo.txt"
        logo = read_text_file(filepath=str(logo_filepath))
        print(logo)
    except:
        pass

def run_once(config_path: str):
    print("Run once and exit!")
    print(f"Config path: {config_path}")
    Pipeline(config_path=config_path,
             run_once=True)

def run_scheduler(config_path: str, interval_minutes: int):
    print(f"Running scheduler every {interval_minutes} minutes...")
    print(f"Config path: {config_path}")
    Pipeline(config_path=config_path,
             schedule_minutes=interval_minutes)

def calculate_kpi(config_filepath: str,
                  start_date: date,
                  end_date: date,
                  hotel_id: int,
                  export_path: Path,
                  export_type: Optional[str] = "CSV",
                  exclude_dates: Optional[List[date]] = None):
    calculator = KpiCalculator(config_filepath=config_filepath,
                               start_date=start_date,
                               end_date=end_date,
                               hotel_id=hotel_id,
                               export_path=export_path,
                               export_type=export_type,
                               exclude_dates=exclude_dates)
    calculator.run()

def validate_date_arg(arg_value: str):
    try:
        date_value = cast_date(value=arg_value,
                               pattern="%Y-%m-%d")
        return date_value
    except Exception as e:
        raise argparse.ArgumentTypeError(f"{arg_value} is not a valid date value!")

def validate_dates_arg(arg_value: str):
    try:
        values = arg_value.split(",")
        date_values: List[date] = []
        for value in values:
            date_values.append(cast_date(value=value,
                                         pattern="%Y-%m-%d"))
        return date_values
    except Exception as e:
        raise argparse.ArgumentTypeError(f"{arg_value} is not a valid date list!")

def validate_export_type(arg_value: str):
    try:
        if arg_value.upper() in ["CSV", "HTML"]:
            return arg_value.upper()
        else:
            raise argparse.ArgumentTypeError(f"{arg_value} is not a valid export type. Allowed values are HTML and CSV")
    except Exception as e:
        raise

def init_parser():

    # region Main parser
    parser = argparse.ArgumentParser(
        prog="RoomPriceGenie Pipeline",
        description="RoomPriceGenie : Reservations ingestion and KPI reporting pipeline"
    )

    parser.add_argument(
        "--config-path",
        help="Pipeline configuration JSON file",
        required=True
    )
    # endregion

    subparsers = parser.add_subparsers()

    # region run-once parser
    run_once_parser = subparsers.add_parser(
        name="run-once",
        help="Run pipeline once and exit"
    )
    run_once_parser.set_defaults(func=lambda args: run_once(config_path=args.config_path))
    # endregion

    # region run-scheduler parser
    run_scheduler_parser = subparsers.add_parser(
        name="schedule",
        help="Run pipeline in scheduled mode"
    )
    run_scheduler_parser.add_argument(
        "--interval-minutes",
        type=int,
        required=True,
        help="Schedule interval in minutes"
    )
    run_scheduler_parser.set_defaults(func=lambda args: run_scheduler(config_path=args.config_path,
                                                                      interval_minutes=args.interval_minutes))
    # endregion

    # region KPI parser
    kpi_parser = subparsers.add_parser(
        name="kpi",
        help="Calculate KPI based on hotel_id and date range"
    )
    kpi_parser.add_argument(
        "--start-date",
        type=validate_date_arg,
        required=True,
        help="Start date in YYYY-MM-DD format"
    )
    kpi_parser.add_argument(
        "--end-date",
        type=validate_date_arg,
        required=True,
        help="End date in YYYY-MM-DD format"
    )
    kpi_parser.add_argument(
        "--hotel-id",
        type=int,
        required=True,
        help="ID of the hotel"
    )
    kpi_parser.add_argument(
        "--exclude-dates",
        type=validate_dates_arg,
        required=False,
        help="Comma separated date(s) to exclude from KPI"
    )
    kpi_parser.add_argument(
        "--export-type",
        type=validate_export_type,
        required=False,
        default="CSV",
        help="Export type of KPI report. Allowed values HTML, CSV. Default: CSV"
    )
    kpi_parser.add_argument(
        "--export-path",
        type=Path,
        required=False,
        default=Path.cwd(),
        help="Export path of KPI report. Default path is working directory"
    )
    kpi_parser.set_defaults(func=lambda args: calculate_kpi(config_filepath=args.config_path,
                                                            start_date=args.start_date,
                                                            end_date=args.end_date,
                                                            hotel_id=args.hotel_id,
                                                            export_type=args.export_type,
                                                            export_path=args.export_path,
                                                            exclude_dates=args.exclude_dates))

    # endregion

    return parser

def main(args=None):

    show_logo()

    parser = init_parser()
    arguments = parser.parse_args(args)
    arguments.func(arguments)

if __name__ == "__main__":
    main()
