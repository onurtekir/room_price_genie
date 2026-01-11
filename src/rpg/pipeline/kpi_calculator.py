import pandas as pd
from pathlib import Path
from typing import Optional, List
from datetime import date, datetime

from rpg.utils.logger import Logger
from rpg.utils.io_util import read_text_file
from rpg.utils.datetime_util import format_datetime
from rpg.pipeline.pipeline_context import PipelineContext
from rpg.utils.string_util import apply_jinja_template


class KpiCalculator:

    def __init__(self,
                 config_filepath: str,
                 start_date: date,
                 end_date: date,
                 hotel_id: int,
                 export_path: Path,
                 export_type: str,
                 exclude_dates: Optional[List[date]] = None):
        self._context = PipelineContext(config_filepath=config_filepath, read_only=True)
        self._start_date = start_date
        self._end_date = end_date
        self._hotel_id = hotel_id
        self._export_path = export_path
        self._export_type = export_type
        self._exclude_dates = exclude_dates or []

    def run(self):
        """
        Run KPI calculation
        """

        # region Show information
        Logger.info(f"Generating KPI report")
        Logger.info(f"Hotel Id      : {self._hotel_id}")
        Logger.info(f"Start Date    : {self._start_date}")
        Logger.info(f"End Date      : {self._end_date}")
        if self._exclude_dates:
            Logger.info(f"Exclude Dates : {', '.join(
                [format_datetime(value=d, pattern='%Y-%m-%d') for d in self._exclude_dates]
            )}")
        Logger.info(f"Export Path  : {self._export_path}")
        Logger.info(f"Export Type  : {self._export_type}")
        # endregion

        # region Calculate KPI
        export_columns = ["NIGHT_OF_STAY", "OCCUPANCY_PERCENTAGE", "TOTAL_NET_REVENUE", "ADR"]
        df_kpi = self._load_kpi_data()
        if self._exclude_dates:
            df_kpi = df_kpi[~df_kpi["NIGHT_OF_STAY"].isin(self._exclude_dates)][export_columns]
        # endregion

        # region Export KPI report
        if self._export_type == "CSV":
            exported_filename = self._export_csv_file(df=df_kpi)
            if exported_filename:
                Logger.success(f"KPI report generated and exported as CSV to '{exported_filename}'")
        elif self._export_type == "HTML":
            exported_filename = self._export_html_file(df=df_kpi)
            if exported_filename:
                Logger.success(f"KPI report generated and exported as HTML to '{exported_filename}'")
        # endregion

    def _load_kpi_data(self) -> Optional[pd.DataFrame]:
        """
        Generate KPI data from database
        """
        try:

            start_date = format_datetime(value=self._start_date, pattern="%Y-%m-%d")
            end_date = format_datetime(value=self._end_date, pattern="%Y-%m-%d")
            query = f"""
            SELECT * 
            FROM view_kpi 
            WHERE NIGHT_OF_STAY BETWEEN '{start_date}' AND '{end_date}' 
            AND HOTEL_ID = {self._hotel_id}
            """
            df_kpi = self._context.db_engine.execute(query=query, is_safe=False)
            # Convert NIGHT_OF_STAY to date format
            df_kpi["NIGHT_OF_STAY"] = df_kpi["NIGHT_OF_STAY"].dt.date
            return df_kpi

        except Exception as e:
            Logger.error(message="Error calculating KPI report!",
                         err=e,
                         include_stack_trace=True)
            return None

    def _export_csv_file(self, df: pd.DataFrame) -> Optional[str]:
        """
        Export KPI report as CSV file
        """
        try:
            str_start_date = format_datetime(value=self._start_date, pattern="%Y_%m_%d")
            str_end_date = format_datetime(value=self._end_date, pattern="%Y_%m_%d")
            filename = f"kpi_{self._hotel_id}_{str_start_date}_to_{str_end_date}.csv"
            export_filepath = Path(self._export_path) / filename
            export_filepath.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(str(export_filepath), index=False)
            return str(export_filepath)
        except Exception as e:
            Logger.error(message="Error exporting KPI report!",
                         err=e,
                         include_stack_trace=True)
            return None

    def _export_html_file(self, df: pd.DataFrame) -> Optional[str]:
        """
        Export KPI as HTML file
        """
        try:
            str_start_date = format_datetime(value=self._start_date, pattern="%Y_%m_%d")
            str_end_date = format_datetime(value=self._end_date, pattern="%Y_%m_%d")
            filename = f"kpi_{self._hotel_id}_{str_start_date}_to_{str_end_date}.html"
            export_filepath = Path(self._export_path) / filename
            export_filepath.parent.mkdir(parents=True, exist_ok=True)

            # region Load HTML template and generate report HTML
            html_template_path = Path(__file__).parents[1] / "html_template/template.html"
            html_template = read_text_file(filepath=str(html_template_path))
            jinja_parameters = dict(
                report_date=format_datetime(value=datetime.now(), pattern="%Y-%m-%d %H:%M:%S"),
                hotel_id=self._hotel_id,
                start_date=format_datetime(value=self._start_date, pattern="%Y-%m-%d"),
                end_date=format_datetime(value=self._end_date, pattern="%Y-%m-%d"),
                exclude_dates=
                ", ".join([format_datetime(value=d, pattern="%Y-%m-%d") for d in self._exclude_dates])
                if self._exclude_dates else "No dates excluded!",
                report_lines=[
                    dict(
                        night_of_stay=r["NIGHT_OF_STAY"],
                        occupancy_percentage=f"{format(r["OCCUPANCY_PERCENTAGE"], '.2f')}%",
                        total_net_revenue=f"{format(r["TOTAL_NET_REVENUE"], '.2f')} €",
                        adr = f"{format(r["ADR"], '.2f')} €"
                    )
                    for _, r in df.iterrows()
                ]

            )
            html_content = apply_jinja_template(source_value=html_template, jinja_parameters=jinja_parameters)
            with open(export_filepath, "w", encoding="utf-8") as f:
                f.write(html_content)
            # endregion

            return str(export_filepath)
        except Exception as e:
            Logger.error(message="Error exporting KPI report!",
                         err=e,
                         include_stack_trace=True)
            return None
