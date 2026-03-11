from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from Ingestion.structured_data import StructuredData


_REPO_ROOT = Path(__file__).resolve().parents[1]
_OUTPUT_DIR = _REPO_ROOT / "Ingestion" / "test" / "output"


def dataset_label(path: Path) -> str:
    try:
        return str(path.relative_to(_OUTPUT_DIR))
    except ValueError:
        return path.name


def dataset_options() -> list[dict[str, str]]:
    return [
        {"label": dataset_label(candidate), "value": str(candidate)}
        for candidate in sorted(_OUTPUT_DIR.rglob("*_structured.json"))
    ]


@lru_cache(maxsize=32)
def load_dataset(dataset_value: str) -> StructuredData | None:
    path = Path(dataset_value)
    if not path.exists():
        return None
    try:
        return StructuredData.load(path)
    except Exception:
        return None
