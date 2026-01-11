from __future__ import annotations

import sys
from pathlib import Path

def _root_from_pyproject() -> Path:
    current_path = Path(__file__).resolve()
    for p in [current_path] + list(current_path.parents):
        if (p / "pyproject.toml").exists():
            return p
    raise RuntimeError("Could not find pyproject.toml!")

PROJECT_ROOT = _root_from_pyproject()
SOURCE_DIRECTORY = PROJECT_ROOT / "src"

if str(SOURCE_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(SOURCE_DIRECTORY))

from rpg.cli.rpg import main

if __name__ == "__main__":
    main()