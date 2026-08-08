"""Table adapters using the same publication output pipeline as parsec_charts."""


def render(experiment, rows, output_dir, profile):
    try:
        from plotting import plot_table1
    except ModuleNotFoundError:
        from artifact.plotting import plot_table1

    plotters = {"table1": plot_table1}
    return plotters[experiment](rows, output_dir, profile)
