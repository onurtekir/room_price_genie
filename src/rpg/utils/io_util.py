import os
from pathlib import Path
from typing import List, Optional


def file_exists(filepath: str) -> bool:
    return os.path.exists(filepath)

def read_text_file(filepath: str) -> str:
    if not file_exists(filepath=filepath):
        raise FileNotFoundError(f"File '{filepath}' not found!")

    with open(filepath, "r") as f:
        return f.read()

def list_files(filepath: str, extension: Optional[str] = "*.*") -> List[Path]:
    return sorted(Path(filepath).glob(extension))