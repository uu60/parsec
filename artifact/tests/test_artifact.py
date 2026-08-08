from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import yaml

from artifact.plotting import generate_plots
from artifact.run import (
    aggregate,
    build_parser,
    configuration_params,
    correctness_summary,
    finalize_performance,
    load_spec,
    parse_metrics,
    parse_micro_metrics,
    is_known_background_teardown_failure,
    micro_base_elements,
    paper_micro_points,
    DEFAULT_MPI_ARGS,
    validate_args,
)


class ArtifactTests(unittest.TestCase):
    def test_experiment_spec_has_all_workloads(self) -> None:
        spec = load_spec()
        self.assertEqual(spec["schema_version"], 1)
        self.assertEqual(len(spec["workloads"]), 8)
        self.assertEqual(spec["measurement"]["workload_seed"], 20270276)
        for figure in ("figure_2", "figure_4", "figure_5"):
            self.assertEqual(list(spec["experiments"][figure]["profiles"]), ["paper"])

    def test_release_and_external_scope_have_no_placeholders(self) -> None:
        spec = load_spec()
        self.assertEqual(spec["artifact"]["release_commit"], "$Format:%H$")
        self.assertEqual(
            spec["experiments"]["figure_7"]["repository_configurations"],
            ["parsec", "parsec_base"],
        )
        for name in ("orq_2pc_real_bmt", "secrecy"):
            external = spec["configurations"][name]["external_artifact"]
            self.assertEqual(external["status"], "not_included")
            self.assertFalse(external["supported_claims"])
        self.assertNotIn("REQUIRED_BEFORE_ARCHIVAL", str(spec))

    def test_aws_delivery_is_documented_without_credentials(self) -> None:
        aws = load_spec()["paper_environments"]["aws"]
        self.assertEqual(aws["delivery"], "author_provisioned_live_instances")
        self.assertEqual(aws["entry_node"], "parsec0")
        self.assertEqual(aws["peer_node"], "parsec1")
        self.assertEqual(aws["credentials"], "private_evaluation_channel")

        template = (
            Path(__file__).resolve().parents[1] / "REVIEWER_ACCESS.template.md"
        ).read_text(encoding="utf-8")
        self.assertIn("<PARSEC0_PUBLIC_IP_OR_DNS>", template)
        self.assertIn("<40_HEX_COMMIT>", template)
        self.assertNotIn("BEGIN OPENSSH PRIVATE KEY", template)

    def test_paper_batch_size_is_explicit(self) -> None:
        spec = load_spec()
        self.assertEqual(spec["configurations"]["parsec"]["params"]["batch_size"], 256)
        self.assertEqual(spec["configurations"]["parsec_noncompact"]["params"]["batch_size"], 256)
        self.assertEqual(spec["configurations"]["parsec_base"]["params"]["batch_size"], 0)

        for configuration in ("parsec", "parsec_noncompact"):
            self.assertIn("--batch_size=256", configuration_params(configuration, 1, False))
        self.assertIn("--batch_size=0", configuration_params("parsec_base", 1, False))

    def test_metric_parser_tolerates_log_prefix(self) -> None:
        text = 'host log ARTIFACT_METRIC {"rank":0,"elapsed_seconds":1.25}\n'
        self.assertEqual(parse_metrics(text)[0]["rank"], 0)

    def test_micro_metric_parser_tolerates_log_prefix(self) -> None:
        text = 'rank log ARTIFACT_MICRO_METRIC {"experiment":"figure5","batch_size":256}\n'
        self.assertEqual(parse_micro_metrics(text)[0]["batch_size"], 256)

    def test_only_known_post_metric_background_teardown_is_accepted(self) -> None:
        metric = (
            'ARTIFACT_MICRO_METRIC {"experiment":"figure4","variant":"background"}\n'
        )
        signature = (
            "malloc_consolidate(): unaligned fastbin chunk detected\n"
            "libmpi.so.40(ompi_mpi_finalize+0x934)\n"
            "rank 1 exited on signal 6 (Aborted)\n"
        )
        executable = Path("benchmark_background_single")
        self.assertTrue(
            is_known_background_teardown_failure(executable, 134, metric + signature, [{}])
        )
        self.assertFalse(
            is_known_background_teardown_failure(executable, 134, signature + metric, [{}])
        )
        self.assertFalse(
            is_known_background_teardown_failure(Path("benchmark_jit_single"), 134, metric + signature, [{}])
        )

    def test_correctness_summary_contains_no_performance_data(self) -> None:
        summary = correctness_summary(list(range(1, 9)), [], "tcp")
        self.assertEqual(summary["status"], "passed")
        self.assertEqual(summary["passed_count"], 8)
        self.assertEqual(len(summary["checks"]), 8)
        self.assertFalse(any("elapsed" in key or "time" in key for key in summary))

    def test_smoke_accepts_direct_host_sequence(self) -> None:
        args = build_parser().parse_args([
            "smoke", "--comm=mpi",
            "--mpi-arg=--bind-to", "--mpi-arg=none",
            "--mpi-arg=--map-by", "--mpi-arg=seq",
            "--mpi-arg=--host", "--mpi-arg=parsec0,parsec1,parsec0",
        ])
        self.assertEqual(args.mpi_arg, [
            "--bind-to", "none", "--map-by", "seq",
            "--host", "parsec0,parsec1,parsec0",
        ])

    def test_mpi_is_the_default_for_all_executable_workflows(self) -> None:
        parser = build_parser()
        commands = [
            ["smoke"],
            ["figure2"],
            ["figure4"],
            ["figure5"],
            ["figure7"],
            ["figure8"],
            ["table1"],
        ]
        for command in commands:
            with self.subTest(command=command[0]):
                args = parser.parse_args(command)
                validate_args(args)
                self.assertEqual(args.comm, "mpi")
                self.assertEqual(args.mpi_arg, DEFAULT_MPI_ARGS)
                if command[0] != "smoke":
                    self.assertEqual(args.profile, "paper")
                    self.assertFalse(hasattr(args, "data_scale"))
                    self.assertEqual(args.repetitions, 1)

    def test_explicit_mpi_arguments_replace_aws_defaults(self) -> None:
        args = build_parser().parse_args([
            "figure8",
            "--mpi-arg=--hostfile", "--mpi-arg=/cluster/hosts",
        ])
        validate_args(args)
        self.assertEqual(args.mpi_arg, ["--hostfile", "/cluster/hosts"])

    def test_paper_micro_cardinalities_are_fixed(self) -> None:
        profile = {
            "nums": [100000, 500000, 1000000],
            "sort_nums": [10000, 50000, 100000],
            "base_elements": 500000,
            "sort_base_elements": 50000,
            "widths": [16, 32, 64],
        }
        self.assertEqual(profile["nums"], [100000, 500000, 1000000])
        self.assertEqual(profile["sort_nums"], [10000, 50000, 100000])
        self.assertEqual(micro_base_elements(profile, "<"), 500000)
        self.assertEqual(micro_base_elements(profile, "sort"), 50000)

    def test_paper_micro_points_skip_redundant_widths(self) -> None:
        profile = {
            "primitives": ["<", "sort"],
            "nums": [50000, 250000, 500000],
            "sort_nums": [5000, 25000, 50000],
            "widths": [16, 32, 64],
        }
        points = paper_micro_points(profile)
        self.assertEqual(len(points), 10)
        self.assertEqual(
            [point for point in points if point[0] == "<"],
            [("<", 50000, 64), ("<", 250000, 16), ("<", 250000, 32),
             ("<", 250000, 64), ("<", 500000, 64)],
        )
        self.assertNotIn(("sort", 50000, 32), points)

    def test_input_scale_and_other_matrix_overrides_are_rejected(self) -> None:
        parser = build_parser()
        for override in ("--profile=quick", "--data-scale=0.5", "--data-scale=1", "--rows=16", "--repetitions=3"):
            with self.subTest(override=override), self.assertRaises(SystemExit):
                parser.parse_args(["figure8", override])

    def test_paper_timeout_is_the_only_performance_default(self) -> None:
        paper = build_parser().parse_args(["figure7"])
        validate_args(paper)
        self.assertEqual(paper.profile, "paper")
        self.assertFalse(hasattr(paper, "data_scale"))
        self.assertEqual(paper.repetitions, 1)
        self.assertEqual(paper.timeout, 86400)

        explicit = build_parser().parse_args([
            "figure7", "--timeout=123",
        ])
        validate_args(explicit)
        self.assertEqual(explicit.timeout, 123)

    def test_measurements_are_manifested_before_plotting(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "summary").mkdir()
            (root / "figures").mkdir()
            manifest = {"arguments": {"profile": "quick"}}
            records = [{
                "workload": "q6", "configuration": "parsec", "repetition": 1,
                "elapsed_seconds": 1.0, "output_rows": 1,
                "bmt_generator_accumulated_seconds": None,
                "online_without_bmt_seconds": None,
            }]
            with patch("artifact.plotting.generate_plots", side_effect=RuntimeError("plot failed")):
                with self.assertRaisesRegex(RuntimeError, "plot failed"):
                    finalize_performance(
                        experiment="figure7", records=records,
                        dimensions=["workload", "configuration"], output_dir=root,
                        manifest=manifest, x_field="workload", series_field="configuration",
                    )
            saved = yaml.safe_load((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(saved["status"], "finalizing")
            self.assertEqual(saved["data_status"], "passed")
            self.assertEqual(saved["aggregate_csv"], "summary/figure7.csv")
            self.assertTrue((root / saved["aggregate_csv"]).stat().st_size > 0)

    def test_aggregate_preserves_matrix_order(self) -> None:
        records = [
            {"rows": 2, "series": "parsec", "elapsed_seconds": 1.0, "output_rows": 2,
             "bmt_generator_accumulated_seconds": None, "online_without_bmt_seconds": None},
            {"rows": 16, "series": "parsec", "elapsed_seconds": 2.0, "output_rows": 16,
             "bmt_generator_accumulated_seconds": None, "online_without_bmt_seconds": None},
        ]
        result = aggregate(records, ["rows", "series"])
        self.assertEqual([row["rows"] for row in result], [2, 16])

    def test_plotter_generates_all_artifact_formats(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            fixtures = {
                "figure2": (
                    "primitive,elements,data_scale,width,mean_arithmetic_ms,mean_boolean_ms,arithmetic_over_boolean\n"
                    "<,100,1.0,64,30,10,3\n==,100,1.0,64,40,20,2\n"
                ),
                "figure4": (
                    "primitive,elements,data_scale,width,mean_background_ms,mean_jit_ms,background_over_jit\n"
                    "!=,100,1.0,64,30,10,3\n<,100,1.0,64,26,10,2.6\n"
                ),
                "figure5": (
                    "primitive,elements,data_scale,width,batch_size,mean_execution_ms\n"
                    "<,100,1.0,64,16,30\n<,100,1.0,64,64,20\n"
                    "sort,20,0.2,64,16,40\nsort,100,1.0,64,16,80\n"
                    "sort,100,1.0,16,16,45\n"
                ),
                "figure7": (
                    "workload,configuration,mean_elapsed_seconds\n"
                    "q6,parsec,1.0\nq6,parsec_base,2.0\n"
                ),
                "figure8": (
                    "rows,series,mean_elapsed_seconds\n"
                    "2,parsec,1.0\n2,orq_bitonic,2.0\n"
                ),
                "table1": (
                    "case,join_mode,mean_elapsed_seconds,output_rows\n"
                    "2-way/8,hash,1.0,16\n2-way/8,nested,2.0,32\n"
                ),
            }
            for experiment, content in fixtures.items():
                aggregate_csv = root / f"{experiment}.csv"
                aggregate_csv.write_text(content, encoding="utf-8")
                outputs = generate_plots(
                    experiment, [aggregate_csv], root / experiment, profile="quick"
                )
                self.assertEqual({path.suffix for path in outputs}, {".png", ".svg", ".pdf"})
                self.assertTrue(all(path.stat().st_size > 0 for path in outputs))


if __name__ == "__main__":
    unittest.main()
