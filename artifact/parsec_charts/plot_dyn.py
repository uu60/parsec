"""Figure 5 adapter for parsec_charts/plot_dyn.py."""


def render(experiment, rows, output_dir, profile):
    try:
        from plotting import plot_figure5
    except ModuleNotFoundError:
        from artifact.plotting import plot_figure5

    return plot_figure5(rows, output_dir, profile)
