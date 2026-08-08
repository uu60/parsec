#!/usr/bin/env python3
"""Paper-oriented plots and tables for ParsecDB artifact results.

The plotting layer consumes only aggregate CSV files.  This keeps figure
generation independent from expensive MPC execution and lets evaluators merge
results collected on different machines without editing a Python source file.
"""

from __future__ import annotations

import csv
import math
import os
from pathlib import Path
from typing import Any, Iterable


FIGURE_FORMATS = ("png", "svg", "pdf")
PAPER_WORKLOAD_ORDER = (
    "q6", "q4", "q13", "password_reuse", "credit_score",
    "comorbidity", "recurrent_c_diff", "aspirin_count",
)
WORKLOAD_LABELS = {
    "q6": "Q6", "q4": "Q4", "q13": "Q13", "password_reuse": "pwd",
    "credit_score": "credit", "comorbidity": "comorb.",
    "recurrent_c_diff": "rcdiff", "aspirin_count": "aspirin",
    "sort": "Sort",
}
SERIES_LABELS = {
    "secrecy": "Secrecy", "parsec": "Parsec", "parsec_base": "Parsec-base",
    "orq_2pc_real_bmt": "ORQ", "orq_bitonic": "ORQ-bitonic",
    "orq_quick": "ORQ-quick", "orq_radix": "ORQ-radix",
    "aws": "AWS", "hpc": "HPC", "hash": "HASH", "nested": "NESTED",
}
SERIES_COLORS = {
    "secrecy": "#777777", "parsec": "#2878B5", "parsec_base": "#B9B9B9",
    "orq_2pc_real_bmt": "#B52A2A", "orq_bitonic": "#A61C1C",
    "orq_quick": "#555555", "orq_radix": "#7B2CBF",
    "aws": "#2A788E", "hpc": "#B8B8B8", "hash": "#2878B5",
    "nested": "#B52A2A",
}
PRIMITIVE_LABELS = {
    "<": "gt", "<=": "ge", "==": "eq", "!=": "neq",
    "mux": "mux", "ar": "ar", "sort": "sort",
}
PRIMITIVE_ORDER = ("!=", "<", "==", "ar", "mux", "sort")
PRIMITIVE_COLORS = {
    "!=": "#4C78A8", "<": "#72A0C1", "==": "#9AA7B0",
    "ar": "#E3A018", "mux": "#B33A3A", "sort": "#6B5B95",
}


def configure_plot_cache() -> None:
    """Keep plotting functional in containers and read-only home directories."""
    cache_root = Path(
        os.environ.get(
            "PARSEC_ARTIFACT_PLOT_CACHE",
            Path(__file__).resolve().parent / "results" / ".plot-cache",
        )
    )
    (cache_root / "matplotlib").mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_root / "matplotlib"))
    os.environ.setdefault("XDG_CACHE_HOME", str(cache_root))


def _matplotlib() -> tuple[Any, Any, Any]:
    configure_plot_cache()
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError as exc:
        raise RuntimeError(
            "plot generation requires matplotlib and numpy; install artifact/requirements.txt"
        ) from exc
    return matplotlib, plt, np


def _number(value: str) -> Any:
    if value == "":
        return None
    try:
        integer = int(value)
        if str(integer) == value:
            return integer
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def read_aggregate_csvs(paths: Iterable[Path]) -> list[dict[str, Any]]:
    """Load and concatenate normalized aggregate CSV files."""
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.is_file():
            raise RuntimeError(f"aggregate CSV does not exist: {path}")
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames:
                raise RuntimeError(f"aggregate CSV has no header: {path}")
            for row in reader:
                parsed = {key: _number(value) for key, value in row.items()}
                parsed["_source_csv"] = str(path)
                rows.append(parsed)
    if not rows:
        raise RuntimeError("no aggregate rows were supplied to the plotter")
    return rows


def _configure_style(matplotlib: Any) -> None:
    matplotlib.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
        "font.size": 9,
        "axes.labelsize": 9,
        "axes.titlesize": 9,
        "legend.fontsize": 8,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "axes.linewidth": 0.8,
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
    })


def _save(fig: Any, output_dir: Path, stem: str, profile: str | None) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if profile == "quick":
        layout = fig.get_layout_engine()
        if layout is not None:
            layout.set(rect=(0, 0.07, 1, 1))
        fig.text(
            0.995, 0.012, "Quick profile - functional validation only",
            ha="right", va="bottom", fontsize=6, color="#666666",
        )
    paths = [output_dir / f"{stem}.{suffix}" for suffix in FIGURE_FORMATS]
    for path in paths:
        fig.savefig(path, bbox_inches="tight")
    return paths


def _positive(values: Iterable[Any]) -> list[float]:
    return [float(value) for value in values if value is not None and float(value) > 0]


def _log_limits(ax: Any, values: Iterable[Any]) -> None:
    positive = _positive(values)
    if not positive:
        return
    low = 10 ** math.floor(math.log10(min(positive)))
    high = 10 ** math.ceil(math.log10(max(positive)))
    if low == high:
        high *= 10
    ax.set_ylim(low / 1.15, high * 1.6)


def _micro_primitives(rows: list[dict[str, Any]], preferred: tuple[str, ...] = PRIMITIVE_ORDER) -> list[str]:
    available = {str(row.get("primitive")) for row in rows}
    ordered = [primitive for primitive in preferred if primitive in available]
    ordered.extend(sorted(available - set(ordered)))
    return ordered


def _micro_panel_rows(
    rows: list[dict[str, Any]], *, scale: float | None = None, width: int | None = None,
    primitive: str | None = None,
) -> list[dict[str, Any]]:
    selected = []
    for row in rows:
        if scale is not None and not math.isclose(float(row.get("data_scale", -1)), scale, rel_tol=1e-9):
            continue
        if width is not None and int(row.get("width", -1)) != width:
            continue
        if primitive is not None and str(row.get("primitive")) != primitive:
            continue
        selected.append(row)
    return selected


def _primitive_scales(rows: list[dict[str, Any]], primitive: str) -> list[float]:
    """Return the small/medium/large scales used by parsec_charts."""
    scales = sorted({float(row["data_scale"]) for row in rows if str(row.get("primitive")) == primitive})
    if len(scales) >= 3:
        return [scales[0], scales[len(scales) // 2], scales[-1]]
    return scales


def _style_micro_axis(ax: Any, *, xlabel: str | None = None, ylabel: str | None = None) -> None:
    if xlabel:
        ax.set_xlabel(xlabel)
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.grid(which="both", color="#D4D8DC", linewidth=0.5)
    ax.set_axisbelow(True)


def plot_figure2(rows: list[dict[str, Any]], output_dir: Path, profile: str | None) -> list[Path]:
    matplotlib, plt, np = _matplotlib()
    _configure_style(matplotlib)
    primitives = _micro_primitives(rows, ("<", "==", "ar", "mux", "sort"))
    widths = sorted({int(row["width"]) for row in rows})
    overview_width = max(widths)
    fig, axes = plt.subplots(1, 3, figsize=(7.05, 2.35), layout="constrained")

    overview = _micro_panel_rows(rows, scale=1.0, width=overview_width)
    lookup = {str(row["primitive"]): row for row in overview}
    x = np.arange(len(primitives))
    boolean = [float(lookup[p]["mean_boolean_ms"]) if p in lookup else np.nan for p in primitives]
    arithmetic = [float(lookup[p]["mean_arithmetic_ms"]) if p in lookup else np.nan for p in primitives]
    axes[0].bar(x - 0.18, boolean, 0.36, label="Boolean", color="#2878B5")
    bars = axes[0].bar(x + 0.18, arithmetic, 0.36, label="Arithmetic", color="#B52A2A", hatch="////")
    for primitive, bar in zip(primitives, bars):
        row = lookup.get(primitive)
        if row and row.get("arithmetic_over_boolean") is not None:
            axes[0].annotate(f"{float(row['arithmetic_over_boolean']):.1f}x",
                             (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                             xytext=(0, 2), textcoords="offset points", ha="center", fontsize=6.5,
                             rotation=90)
    axes[0].set_title("(a) Time (ms) comparison")
    axes[0].set_xticks(x, [PRIMITIVE_LABELS.get(p, p) for p in primitives])
    axes[0].set_yscale("log")
    _log_limits(axes[0], [*boolean, *arithmetic])
    axes[0].legend(loc="upper left", fontsize=6.5)
    _style_micro_axis(axes[0], xlabel="Primitive Operations")

    scale_labels = ("small", "medium", "large")
    shown = [p for p in ("<", "==", "mux", "sort") if p in primitives]
    scale_x = np.arange(len(scale_labels))
    scale_bar_width = 0.8 / max(1, len(shown))
    for index, primitive in enumerate(shown):
        primitive_scales = _primitive_scales(rows, primitive)
        by_scale = {
            float(row["data_scale"]): row
            for row in _micro_panel_rows(rows, width=overview_width, primitive=primitive)
        }
        values = [
            float(by_scale[scale]["arithmetic_over_boolean"])
            if scale in by_scale and by_scale[scale].get("arithmetic_over_boolean") is not None
            else np.nan
            for scale in primitive_scales
        ]
        values.extend([np.nan] * (len(scale_labels) - len(values)))
        offset = (index - (len(shown) - 1) / 2) * scale_bar_width
        axes[1].bar(
            scale_x + offset, values[:len(scale_labels)], scale_bar_width,
            label=PRIMITIVE_LABELS.get(primitive, primitive),
            color=PRIMITIVE_COLORS.get(primitive), edgecolor="white", linewidth=0.5,
            hatch="////" if index % 2 else None,
        )
    axes[1].axhline(1, color="#666666", linewidth=0.8, linestyle=":")
    axes[1].set_title("(b) Slowdown /w Data Scale")
    axes[1].set_xticks(scale_x, scale_labels)
    axes[1].set_yscale("log")
    axes[1].legend(loc="upper left", ncols=2, fontsize=6.5)
    _style_micro_axis(axes[1], xlabel="Data Scale")

    bit_x = np.arange(len(widths))
    bit_bar_width = 0.8 / max(1, len(shown))
    for index, primitive in enumerate(shown):
        selected = _micro_panel_rows(rows, scale=1.0, primitive=primitive)
        by_width = {int(row["width"]): row for row in selected}
        values = [float(by_width[width]["mean_boolean_ms"]) if width in by_width else np.nan for width in widths]
        offset = (index - (len(shown) - 1) / 2) * bit_bar_width
        axes[2].bar(
            bit_x + offset, values, bit_bar_width,
            label=PRIMITIVE_LABELS.get(primitive, primitive),
            color=PRIMITIVE_COLORS.get(primitive), edgecolor="white", linewidth=0.5,
            hatch="////" if index % 2 else None,
        )
    axes[2].set_title("(c) Time (ms) /w BSS Quant")
    axes[2].set_xticks(bit_x, widths)
    axes[2].set_yscale("log")
    axes[2].legend(loc="upper left", ncols=2, fontsize=6.5)
    _style_micro_axis(axes[2], xlabel="Bit Width")
    return _save(fig, output_dir, "figure2", profile)


def plot_figure4(rows: list[dict[str, Any]], output_dir: Path, profile: str | None) -> list[Path]:
    matplotlib, plt, np = _matplotlib()
    _configure_style(matplotlib)
    primitives = _micro_primitives(rows, ("!=", "<", "==", "mux", "sort"))
    widths = sorted({int(row["width"]) for row in rows})
    overview_width = max(widths)
    fig, axes = plt.subplots(1, 3, figsize=(7.05, 2.35), layout="constrained")

    overview = _micro_panel_rows(rows, scale=1.0, width=overview_width)
    lookup = {str(row["primitive"]): row for row in overview}
    x = np.arange(len(primitives))
    jit = [float(lookup[p]["mean_jit_ms"]) if p in lookup else np.nan for p in primitives]
    background = [float(lookup[p]["mean_background_ms"]) if p in lookup else np.nan for p in primitives]
    axes[0].bar(x - 0.18, jit, 0.36, label="JIT", color="#2878B5")
    bars = axes[0].bar(x + 0.18, background, 0.36, label="Background", color="#B52A2A", hatch="////")
    for primitive, bar in zip(primitives, bars):
        row = lookup.get(primitive)
        if row and row.get("background_over_jit") is not None:
            axes[0].annotate(f"{float(row['background_over_jit']):.1f}x",
                             (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                             xytext=(0, 2), textcoords="offset points", ha="center", fontsize=6.5,
                             rotation=90)
    axes[0].set_title("(a) Time (ms) comparison")
    axes[0].set_xticks(x, [PRIMITIVE_LABELS.get(p, p) for p in primitives])
    axes[0].set_yscale("log")
    _log_limits(axes[0], [*jit, *background])
    axes[0].legend(loc="upper left", fontsize=6.5)
    _style_micro_axis(axes[0], xlabel="Primitive Operations")

    scale_labels = ("small", "medium", "large")
    scale_x = np.arange(len(scale_labels))
    scale_bar_width = 0.8 / max(1, len(primitives))
    for index, primitive in enumerate(primitives):
        primitive_scales = _primitive_scales(rows, primitive)
        by_scale = {
            float(row["data_scale"]): row
            for row in _micro_panel_rows(rows, width=overview_width, primitive=primitive)
        }
        values = [
            float(by_scale[scale]["background_over_jit"])
            if scale in by_scale and by_scale[scale].get("background_over_jit") is not None
            else np.nan
            for scale in primitive_scales
        ]
        values.extend([np.nan] * (len(scale_labels) - len(values)))
        offset = (index - (len(primitives) - 1) / 2) * scale_bar_width
        axes[1].bar(
            scale_x + offset, values[:len(scale_labels)], scale_bar_width,
            label=PRIMITIVE_LABELS.get(primitive, primitive),
            color=PRIMITIVE_COLORS.get(primitive), edgecolor="white", linewidth=0.5,
            hatch="////" if index % 2 else None,
        )
    axes[1].axhline(1, color="#666666", linewidth=0.8, linestyle=":")
    axes[1].set_title("(b) Slowdown /w Data Scale")
    axes[1].set_xticks(scale_x, scale_labels)
    axes[1].legend(loc="upper left", ncols=2, fontsize=6.2)
    _style_micro_axis(axes[1], xlabel="Data Scale")

    bit_x = np.arange(len(widths))
    bit_bar_width = 0.8 / max(1, len(primitives))
    for index, primitive in enumerate(primitives):
        selected = _micro_panel_rows(rows, scale=1.0, primitive=primitive)
        by_width = {int(row["width"]): row for row in selected}
        values = [
            float(by_width[width]["background_over_jit"])
            if width in by_width and by_width[width].get("background_over_jit") is not None
            else np.nan for width in widths
        ]
        offset = (index - (len(primitives) - 1) / 2) * bit_bar_width
        axes[2].bar(
            bit_x + offset, values, bit_bar_width,
            label=PRIMITIVE_LABELS.get(primitive, primitive),
            color=PRIMITIVE_COLORS.get(primitive), edgecolor="white", linewidth=0.5,
            hatch="////" if index % 2 else None,
        )
    axes[2].axhline(1, color="#666666", linewidth=0.8, linestyle=":")
    axes[2].set_title("(c) Slowdown /w Bit Width")
    axes[2].set_xticks(bit_x, widths)
    axes[2].legend(loc="upper left", ncols=2, fontsize=6.2)
    _style_micro_axis(axes[2], xlabel="Bit Width")
    return _save(fig, output_dir, "figure4", profile)


def plot_figure5(rows: list[dict[str, Any]], output_dir: Path, profile: str | None) -> list[Path]:
    matplotlib, plt, _ = _matplotlib()
    _configure_style(matplotlib)
    primitives = _micro_primitives(rows, ("<", "==", "ar", "mux", "sort"))
    widths = sorted({int(row["width"]) for row in rows})
    overview_width = max(widths)
    fig, axes = plt.subplots(1, 3, figsize=(7.05, 2.35), layout="constrained")

    for primitive in primitives:
        selected = _micro_panel_rows(rows, scale=1.0, width=overview_width, primitive=primitive)
        selected.sort(key=lambda row: int(row["batch_size"]))
        axes[0].plot([int(row["batch_size"]) for row in selected],
                     [float(row["mean_execution_ms"]) for row in selected],
                     marker="o", linewidth=1.1, markersize=3,
                     label=PRIMITIVE_LABELS.get(primitive, primitive),
                     color=PRIMITIVE_COLORS.get(primitive))
    axes[0].set_title("(a) Primitives /w Batch Size")
    axes[0].legend(loc="upper left", ncols=2, fontsize=6.5)

    scales = sorted({float(row["data_scale"]) for row in rows if str(row.get("primitive")) == "sort"})
    for scale in scales:
        selected = _micro_panel_rows(rows, scale=scale, width=overview_width, primitive="sort")
        selected.sort(key=lambda row: int(row["batch_size"]))
        axes[1].plot([int(row["batch_size"]) for row in selected],
                     [float(row["mean_execution_ms"]) for row in selected],
                     marker="o", linewidth=1.1, markersize=3, label=f"{scale:g}x sort")
    axes[1].set_title("(b) Data Scale /w Batch Size")
    axes[1].legend(loc="upper left", fontsize=6.5)

    for width in widths:
        selected = _micro_panel_rows(rows, scale=1.0, width=width, primitive="sort")
        selected.sort(key=lambda row: int(row["batch_size"]))
        axes[2].plot([int(row["batch_size"]) for row in selected],
                     [float(row["mean_execution_ms"]) for row in selected],
                     marker="o", linewidth=1.1, markersize=3, label=f"{width}-bit p-sort")
    axes[2].set_title("(c) Bit Width /w Batch Size")
    axes[2].legend(loc="upper left", fontsize=6.5)

    for ax in axes:
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        _style_micro_axis(ax, xlabel="Batch Size", ylabel="Execution Time (ms)" if ax is axes[0] else None)
    return _save(fig, output_dir, "figure5", profile)


def plot_figure7(rows: list[dict[str, Any]], output_dir: Path, profile: str | None) -> list[Path]:
    matplotlib, plt, np = _matplotlib()
    _configure_style(matplotlib)
    workloads = [name for name in PAPER_WORKLOAD_ORDER if any(r.get("workload") == name for r in rows)]
    extra = [str(r["workload"]) for r in rows if str(r.get("workload")) not in workloads]
    workloads.extend(dict.fromkeys(extra))
    preferred = ("secrecy", "parsec", "parsec_base", "orq_2pc_real_bmt")
    series = [name for name in preferred if any(r.get("configuration") == name for r in rows)]
    series.extend(dict.fromkeys(
        str(r["configuration"]) for r in rows
        if str(r.get("configuration")) not in series
    ))
    lookup = {(str(r["workload"]), str(r["configuration"])): r for r in rows}
    x = np.arange(len(workloads))
    width = min(0.8 / max(1, len(series)), 0.22)
    fig, ax = plt.subplots(figsize=(7.05, 2.55), layout="constrained")
    all_values: list[float] = []
    bar_sets: dict[str, Any] = {}
    for index, name in enumerate(series):
        values = [
            float(lookup[(workload, name)]["mean_elapsed_seconds"])
            if (workload, name) in lookup else np.nan for workload in workloads
        ]
        all_values.extend(_positive(values))
        offset = (index - (len(series) - 1) / 2) * width
        bars = ax.bar(
            x + offset, values, width, label=SERIES_LABELS.get(name, name),
            color=SERIES_COLORS.get(name), edgecolor="#333333", linewidth=0.45,
            hatch="////" if name in {"secrecy", "orq_2pc_real_bmt"} else None,
        )
        bar_sets[name] = bars
    parsec = {workload: lookup[(workload, "parsec")]["mean_elapsed_seconds"] for workload in workloads if (workload, "parsec") in lookup}
    for name, bars in bar_sets.items():
        if name == "parsec":
            continue
        for workload, bar in zip(workloads, bars):
            baseline = parsec.get(workload)
            height = bar.get_height()
            if baseline and math.isfinite(height) and height > 0:
                ratio = height / float(baseline)
                label = f"{ratio:.2f}x" if ratio < 10 else f"{ratio:.0f}x"
                ax.annotate(label, (bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 2), textcoords="offset points", ha="center",
                            va="bottom", fontsize=6.5, rotation=90)
    ax.set_ylabel("Execution Time (s)")
    ax.set_yscale("log")
    _log_limits(ax, all_values)
    ax.set_xticks(x, [WORKLOAD_LABELS.get(name, name) for name in workloads])
    ax.grid(axis="y", which="both", color="#D4D8DC", linewidth=0.5)
    ax.set_axisbelow(True)
    ax.legend(ncols=min(4, len(series)), loc="upper left", frameon=True)
    return _save(fig, output_dir, "figure7", profile)


def plot_figure8(rows: list[dict[str, Any]], output_dir: Path, profile: str | None) -> list[Path]:
    matplotlib, plt, np = _matplotlib()
    _configure_style(matplotlib)
    rows = [r for r in rows if r.get("rows") is not None]
    sizes = sorted({int(r["rows"]) for r in rows})
    preferred = ("parsec", "orq_bitonic", "orq_quick", "orq_radix")
    series = [name for name in preferred if any(r.get("series") == name for r in rows)]
    series.extend(dict.fromkeys(str(r["series"]) for r in rows if str(r.get("series")) not in series))
    lookup = {(int(r["rows"]), str(r["series"])): float(r["mean_elapsed_seconds"]) for r in rows}
    fig, axes = plt.subplots(1, 2, figsize=(7.05, 2.55), layout="constrained")
    markers = ("o", "D", "^", "s", "x")
    styles = ("-", "-", "--", "-.", ":")
    for index, name in enumerate(series):
        xs = [size for size in sizes if (size, name) in lookup]
        ys = [lookup[(size, name)] for size in xs]
        axes[0].plot(xs, ys, marker=markers[index % len(markers)], linestyle=styles[index % len(styles)],
                     linewidth=1.2, markersize=3.5, label=SERIES_LABELS.get(name, name),
                     color=SERIES_COLORS.get(name))
    axes[0].set_title("(a) Sorting Runtime")
    axes[0].set_ylabel("Time (s)")
    axes[0].set_xscale("log", base=2)
    axes[0].set_yscale("log")
    axes[0].legend(ncols=2, loc="upper left")
    parsec = {size: lookup[(size, "parsec")] for size in sizes if (size, "parsec") in lookup}
    baseline_series = [name for name in series if name != "parsec"]
    for index, name in enumerate(baseline_series):
        xs = [size for size in sizes if size in parsec and (size, name) in lookup]
        ys = [lookup[(size, name)] / parsec[size] for size in xs]
        axes[1].plot(xs, ys, marker=markers[(index + 1) % len(markers)],
                     linestyle=styles[(index + 1) % len(styles)], linewidth=1.2,
                     markersize=3.5, label=SERIES_LABELS.get(name, name),
                     color=SERIES_COLORS.get(name))
    axes[1].axhline(1, color="#666666", linewidth=0.8, linestyle=":", label="1x baseline")
    axes[1].set_title("(b) Slowdown /w Number of Rows")
    axes[1].set_xscale("log", base=2)
    axes[1].set_yscale("log")
    if baseline_series:
        axes[1].legend(loc="upper left")
    else:
        axes[1].text(0.5, 0.5, "ORQ baseline data not supplied", transform=axes[1].transAxes,
                     ha="center", va="center", color="#666666")
    for ax in axes:
        ax.set_xlabel("Number of Rows")
        ax.grid(which="both", color="#D4D8DC", linewidth=0.5)
        ax.set_axisbelow(True)
    return _save(fig, output_dir, "figure8", profile)


def _format_value(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    return f"{float(value):.{digits}f}"


def _format_ratio(value: float | None) -> str:
    if value is None:
        return "-"
    if value < 0.1:
        return f"{value:.3f}x"
    return f"{value:.1f}x"


def plot_table1(rows: list[dict[str, Any]], output_dir: Path, profile: str | None) -> list[Path]:
    matplotlib, plt, _ = _matplotlib()
    _configure_style(matplotlib)
    cases = list(dict.fromkeys(str(r["case"]) for r in rows))
    lookup = {(str(r["case"]), str(r["join_mode"])): r for r in rows}
    cells = []
    for case in cases:
        h, n = lookup.get((case, "hash")), lookup.get((case, "nested"))
        ht, nt = (h or {}).get("mean_elapsed_seconds"), (n or {}).get("mean_elapsed_seconds")
        hr, nr = (h or {}).get("output_rows"), (n or {}).get("output_rows")
        speedup = float(nt) / float(ht) if ht and nt else None
        reduction = float(nr) / float(hr) if hr and nr else None
        cells.append([
            case.split("/", 1)[0], case.split("/", 1)[-1], _format_value(ht), _format_value(nt),
            _format_ratio(speedup), f"{int(hr):,}" if hr is not None else "-",
            f"{int(nr):,}" if nr is not None else "-", _format_ratio(reduction),
        ])
    columns = ["Join", "Rows", "HASH Time", "NESTED Time", "Speedup", "HASH Output", "NESTED Output", "Reduction"]
    fig, ax = plt.subplots(figsize=(7.05, 0.58 + 0.28 * max(1, len(cells))), layout="constrained")
    ax.axis("off")
    table = ax.table(cellText=cells, colLabels=columns, cellLoc="center", loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(7.5)
    table.scale(1, 1.25)
    for (row, _), cell in table.get_celld().items():
        cell.set_linewidth(0.35)
        if row == 0:
            cell.set_facecolor("#E8EEF3")
            cell.set_text_props(weight="bold")
    return _save(fig, output_dir, "table1", profile)


PLOTTERS = {
    "figure2": plot_figure2,
    "figure4": plot_figure4,
    "figure5": plot_figure5,
    "figure7": plot_figure7,
    "figure8": plot_figure8,
    "table1": plot_table1,
}


def generate_plots(
    experiment: str, csv_paths: Iterable[Path], output_dir: Path, profile: str | None = None,
) -> list[Path]:
    """Generate all publication and preview formats for one paper result."""
    rows = read_aggregate_csvs(csv_paths)
    try:
        from parsec_charts import render
    except ModuleNotFoundError:
        from artifact.parsec_charts import render
    return render(experiment, rows, output_dir, profile)
