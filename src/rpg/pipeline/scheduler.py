import time
import signal
from pathlib import Path
from typing import Callable, Any
from datetime import datetime, timedelta

from rpg.utils.logger import Logger
from rpg.utils.io_util import file_exists
from rpg.utils.datetime_util import format_datetime, format_now


class Scheduler:

    def __init__(self, interval_minutes: int, runner_func: Callable[..., Any]):
        self._internal_minutes = interval_minutes
        self._runner_func = runner_func
        self._lock_path = Path(__file__).parent / "rpg.lock"
        self._stopped = False
        self._init()

    def _init(self):

        Logger.info(f"Initializing pipeline scheduler to run every {self._internal_minutes} minutes")

        if self._is_locked():
            raise RuntimeError("Scheduler already running! Exiting...")
        self._lock()

        # region Register shutdown handlers
        signal.signal(signal.SIGINT, self._unlock)
        signal.signal(signal.SIGTERM, self._unlock)
        # endregion

        Logger.success("Done!")

    def _is_locked(self) -> bool:
        return file_exists(filepath=str(self._lock_path))

    def _unlock(self, *_):
        Logger.info("Scheduler is shutting down...")
        self._lock_path.unlink(missing_ok=True)
        self._stopped = True
        Logger.success("Done!")

    def _lock(self):
        self._lock_path.write_text(f"RunId: {datetime.now().isoformat()}")

    def start(self):

        try:

            while not self._stopped:
                self._run()

                # region Wait until next run
                next_run_datetime = datetime.now() + timedelta(minutes=self._internal_minutes)
                Logger.info(
                    f"Next run will be executed on '{format_datetime(value=next_run_datetime, 
                                                                     pattern="%d.%m.%Y %H:%M:%S")}'"
                )
                for _ in range(self._internal_minutes * 60):
                    if self._stopped:
                        break
                    time.sleep(1)
                # endregion
        finally:
            self._unlock()

    def _run(self):
        try:
            Logger.info(f"Schedule execution started: {format_now()}")
            self._runner_func()
            Logger.success(f"Schedule execution completed: {format_now()}")
        except Exception as e:
            Logger.error(message=f"Scheduled execution failed",
                         err=e,
                         include_stack_trace=True)

