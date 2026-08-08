"""Figure 8 adapter for parsec_charts/plot_sort.py."""


def render(experiment, rows, output_dir, profile):
    try:
        from plotting import plot_figure8
    except ModuleNotFoundError:
        from artifact.plotting import plot_figure8

    return plot_figure8(rows, output_dir, profile)
