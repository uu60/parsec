"""Figure 4 adapter for parsec_charts/plot_micro_bmt.py."""


def render(experiment, rows, output_dir, profile):
    try:
        from plotting import plot_figure4
    except ModuleNotFoundError:
        from artifact.plotting import plot_figure4

    return plot_figure4(rows, output_dir, profile)
