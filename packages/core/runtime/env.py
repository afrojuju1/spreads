from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[3]
DOTENV_PATH = ROOT / ".env"


@lru_cache(maxsize=1)
def load_environment() -> None:
    if DOTENV_PATH.exists():
        load_dotenv(DOTENV_PATH, override=False)
