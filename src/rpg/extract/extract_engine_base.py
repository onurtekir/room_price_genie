from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd


class ExtractEngineBase(ABC):

    def __init__(self, configuration: Dict[Any, Any]):
        self._configuration = configuration

    @property
    def configuration(self) -> Dict[Any, Any]:
        return self._configuration

    @abstractmethod
    def extract_inventory(self) -> Optional[Tuple[Dict[str, Any], pd.DataFrame]]:
        pass

    @abstractmethod
    def extract_reservations(self) -> Optional[List[Tuple[Dict[str, Any], pd.DataFrame, pd.DataFrame, pd.DataFrame]]]:
        pass