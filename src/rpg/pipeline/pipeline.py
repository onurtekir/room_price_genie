from typing import Optional
from argparse import ArgumentError

from rpg.pipeline.runner import Runner
from rpg.pipeline.pipeline_context import PipelineContext

class Pipeline:

    def __init__(self,
                 config_path: str,
                 run_once: Optional[bool] = None,
                 schedule_minutes: Optional[int] = None):

        context = PipelineContext(config_filepath=config_path,
                                  read_only=False)

        if run_once is None and (schedule_minutes is None or schedule_minutes < 1):
            raise ArgumentError(message="Error initializing pipeline. Invalid/missing construction parameters!")

        self._runner = Runner(config=context.config,
                              db_engine=context.db_engine)

        if run_once:
            self._runner.run()
        else:
            self._runner.start(interval_minutes=schedule_minutes)


