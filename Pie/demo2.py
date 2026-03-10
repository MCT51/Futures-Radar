from __future__ import annotations

from pathlib import Path

try:
    from Ingestion.structured_data import StructuredData
    from Pie.pie import Pie
except ModuleNotFoundError:  # Supports `python3 Pie/demo2.py`
    import sys

    ROOT = Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from Ingestion.structured_data import StructuredData
    from Pie.pie import Pie


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_STRUCTURED_JSON = REPO_ROOT / "Ingestion" / "test" / "output" / "fsm_ethnicity_structured.json"


def load_demo2_structured_data(path: Path = DEFAULT_STRUCTURED_JSON) -> StructuredData:
    if not path.exists():
        raise FileNotFoundError(
            f"StructuredData JSON not found at {path}.\n"
            "Run: python3 Ingestion/test/make_fsm_ethnicity_structured.py"
        )
    return StructuredData.load(path)


def main() -> None:
    structured = load_demo2_structured_data()
    pie = Pie(structured_data=structured, title="FSM/Ethnicity Pie Demo", default_metric="percent")
    pie.run()


if __name__ == "__main__":
    main()
