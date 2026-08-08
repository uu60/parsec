"""Artifact adapters for the paper's parsec_charts plotting scripts."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any


SCRIPT_MODULES = {
    "figure2": "plot_micro_bool",
    "figure4": "plot_micro_bmt",
    "figure5": "plot_dyn",
    "figure7": "plot_endtoend",
    "figure8": "plot_sort",
    "table1": "plot_tables",
}


def render(
    experiment: str, rows: list[dict[str, Any]], output_dir: Path, profile: str | None,
) -> list[Path]:
    """Dispatch normalized artifact rows through the corresponding paper chart script."""
    script = SCRIPT_MODULES.get(experiment)
    if script is None:
        raise RuntimeError(f"no parsec_charts script is defined for experiment: {experiment}")
    module = import_module(f"{__name__}.{script}")
    return module.render(experiment, rows, output_dir, profile)


def script_path(experiment: str) -> Path:
    script = SCRIPT_MODULES[experiment]
    return Path(__file__).resolve().parent / f"{script}.py"
