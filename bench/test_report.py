"""Offline report tests, including the exact Markdown contract."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from bench.report import format_report

FIXTURES = Path(__file__).parent / "fixtures"


def trial(scenario="case_a", status="same", baseline=100, candidate=100, **extra):
    return {
        "scenario": scenario,
        "seed": 0,
        "status": status,
        "detail": "",
        "baseline": {"rows_scanned": baseline},
        "candidate": {"rows_scanned": candidate},
        **extra,
    }


def test_snapshot():
    data = json.loads((FIXTURES / "report_trials.json").read_text())
    assert format_report(data["trials"]) == (FIXTURES / "report_expected.md").read_text()


def test_incorrect_trials_never_receive_speed_credit():
    result = format_report(
        [
            trial(status="faster", candidate=50),
            trial(status="incorrect", candidate=1),
            trial(status="failed", candidate=2),
            trial(status="unsupported", candidate=3),
        ]
    )
    assert "0.500 [0.500, 0.500]; 2.000x; n=1" in result
    assert "faster=1, same=0, slower=0, incorrect=1, failed=1, unsupported=1" in result


@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        (0, 0, "1.000 [1.000, 1.000]"),
        (100, 0, "0.000 [0.000, 0.000]; 1000000.000x"),
        (0, 1, "1000000.000 [1000000.000, 1000000.000]"),
    ],
)
def test_zero_work(a, b, expected):
    assert expected in format_report([trial(baseline=a, candidate=b)])


def test_cloud_metrics_present_only():
    t = trial()
    t["baseline"].update(total_slot_ms=1000, bytes_processed=200, bytes_billed=300)
    t["candidate"].update(total_slot_ms=500, bytes_processed=100, bytes_billed=150)
    report = format_report([t])
    for metric in ("total_slot_ms", "bytes_processed", "bytes_billed"):
        assert f" {metric} |" in report
    assert " seconds |" not in report
    assert " statements |" not in report


def test_missing_metric_pairs_are_excluded_and_counted():
    t = trial()
    t["baseline"]["seconds"] = 10
    assert " seconds |" not in format_report([t])
    t["candidate"]["seconds"] = 5
    assert "0.500 [0.500, 0.500]; 2.000x; n=1" in format_report([t, trial()])


def test_geomean_not_arithmetic_mean():
    report = format_report([trial(candidate=25), trial(candidate=400)])
    assert "1.000 [0.250, 4.000]; 1.000x; n=2" in report


def test_threshold_boundaries_and_parameters():
    trials = [trial(candidate=n) for n in (94, 95, 105, 106)]
    assert "faster=1, same=2, slower=1" in format_report(trials)
    assert "faster=0, same=4, slower=0" in format_report(
        trials, faster_threshold=0.9, slower_threshold=1.1
    )


def test_previous_run_deltas_regressions_and_missing_cases():
    previous = [trial("better", candidate=100), trial("worse", candidate=50), trial("missing")]
    current = [
        trial("better", candidate=50),
        trial("worse", candidate=100),
        trial("broken", status="incorrect"),
        trial("new"),
    ]
    report = format_report(current, baseline_trials=previous)
    assert "| better | rows_scanned | 1.000 | 0.500 | -50.0% |" in report
    assert "| worse | rows_scanned | 0.500 | 1.000 | +100.0% |" in report
    assert "worse / rows_scanned: ratio worsened 100.0%" in report
    assert "broken: incorrect count increased 0 -> 1" in report
    assert "missing: missing current results." in report
    assert "| new | rows_scanned | - | 1.000 | - |" in report


def test_empty_and_all_failed_runs():
    assert "# Benchmark report: 0 trials across 0 scenarios" in format_report([])
    assert "None." in format_report([], baseline_trials=[])
    report = format_report([trial(status="failed")])
    assert " rows_scanned |" in report
    assert "| case_a | 1 | 0 | 0 | 0 | 0 | 1 | 0 | - |" in report
    assert "| case_a | 0 | failed |  |" in report


def test_escape_markdown_and_keep_all_noncorrect_trials():
    trials = [trial("case|a", status="incorrect", detail="bad\n<row> | mismatch")]
    trials.extend(trial(f"case_{n}", status="failed") for n in range(10))
    report = format_report(trials)
    assert "bad &lt;row&gt; &#124; mismatch" in report
    assert "| case_9 | 0 | failed |" in report


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), True, "1"])
def test_invalid_metrics_fail(value):
    with pytest.raises(ValueError, match="finite nonnegative"):
        format_report([trial(candidate=value)])


@pytest.mark.parametrize(("faster", "slower"), [(0, 1.05), (1, 1), (1.1, 1.2), (0.9, 0.99)])
def test_invalid_thresholds_fail(faster, slower):
    with pytest.raises(ValueError, match="thresholds"):
        format_report([], faster_threshold=faster, slower_threshold=slower)


def test_invalid_status_and_metrics_fail():
    with pytest.raises(ValueError, match="unknown trial status"):
        format_report([trial(status="mystery")])
    with pytest.raises(ValueError, match="nonempty and unique"):
        format_report([], metrics=())
    with pytest.raises(ValueError, match="nonempty and unique"):
        format_report([], metrics=("seconds", "seconds"))


def test_cli(tmp_path):
    previous = tmp_path / "previous.json"
    previous.write_text(json.dumps({"trials": [trial(candidate=100)]}))
    command = [
        sys.executable,
        "-m",
        "bench.report",
        str(FIXTURES / "report_trials.json"),
        "--baseline-json",
        str(previous),
        "--slower-threshold",
        "1.2",
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=True)
    assert "## Change against previous run" in completed.stdout
    assert "slower > 1.2" in completed.stdout
    assert not completed.stderr


def test_cli_rejects_bad_input(tmp_path):
    source = tmp_path / "invalid.json"
    source.write_text('{"trials": [{"scenario": "case_a", "status": "unknown"}]}')
    result = subprocess.run(
        [sys.executable, "-m", "bench.report", str(source)], capture_output=True, text=True
    )
    assert result.returncode == 2
    assert "unknown trial status" in result.stderr


def test_bad_trial_cloud_metrics_visible_without_credit():
    t = trial(status="incorrect")
    t["baseline"] = {"total_slot_ms": 100}
    t["candidate"] = {"total_slot_ms": 1}
    report = format_report([t])
    assert " total_slot_ms |" in report
    assert "n=1" not in report


def test_previous_bad_trials_do_not_contribute_deltas():
    report = format_report(
        [trial(candidate=50)], baseline_trials=[trial(status="incorrect", candidate=1)]
    )
    assert "| case_a | rows_scanned | - | 0.500 | - |" in report


def test_regression_threshold_parameter():
    current = [trial(candidate=110)]
    old = [trial()]
    assert "ratio worsened" in format_report(current, baseline_trials=old)
    assert "ratio worsened" not in format_report(current, baseline_trials=old, slower_threshold=1.2)


def test_tiny_nonzero_times_keep_their_true_ratio():
    t = trial()
    t["baseline"] = {"seconds": 1e-9}
    t["candidate"] = {"seconds": 1e-9}
    assert "1.000 [1.000, 1.000]" in format_report([t])


def test_positive_work_against_zero_never_receives_speed_credit():
    result = format_report([trial(baseline=0, candidate=1e-9)])
    assert "faster=0, same=0, slower=1" in result
