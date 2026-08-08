"""Figure 2 adapter for parsec_charts/plot_micro_bool.py."""


def render(experiment, rows, output_dir, profile):
    try:
        from plotting import plot_figure2
    except ModuleNotFoundError:
        from artifact.plotting import plot_figure2

    return plot_figure2(rows, output_dir, profile)
