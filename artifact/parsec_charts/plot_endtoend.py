"""Figure 7 adapter for parsec_charts/plot_endtoend.py."""


def render(experiment, rows, output_dir, profile):
    try:
        from plotting import plot_figure7
    except ModuleNotFoundError:
        from artifact.plotting import plot_figure7

    return plot_figure7(rows, output_dir, profile)
