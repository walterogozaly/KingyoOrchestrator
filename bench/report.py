"""Format existing benchmark trial JSON as Markdown; no database or cloud access."""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

DEFAULT_METRICS = (
    "rows_scanned",
    "seconds",
    "statements",
    "total_slot_ms",
    "bytes_processed",
    "bytes_billed",
)
CORRECT = frozenset(("faster", "same", "slower"))
STATUSES = ("faster", "same", "slower", "incorrect", "failed", "unsupported")
RATIO_FLOOR = 1e-6


def _text(value):
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("|", "&#124;")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _ratio(trial, metric):
    a = trial.get("baseline", {}).get(metric)
    b = trial.get("candidate", {}).get(metric)
    if a is None or b is None:
        return None
    for value in (a, b):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{metric}: metrics must be finite nonnegative numbers")
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{metric}: metrics must be finite nonnegative numbers")
    if a == b == 0:
        return 1.0
    if a == 0:
        return 1 / RATIO_FLOOR
    ratio = b / a
    if not math.isfinite(ratio):
        raise ValueError(f"{metric}: ratio is not finite")
    return max(ratio, RATIO_FLOOR)


def _gmean(values):
    return math.exp(math.fsum(math.log(v) for v in values) / len(values))


def _summarize(trials, metrics, faster, slower):
    groups = {}
    for trial in trials:
        scenario = trial["scenario"]
        status = trial["status"]
        if status not in STATUSES:
            raise ValueError(f"unknown trial status: {status}")
        group = groups.setdefault(scenario, {"counts": Counter(), "ratios": {}, "trials": []})
        group["trials"].append(trial)
        if status in CORRECT:
            ratios = {metric: _ratio(trial, metric) for metric in metrics}
            # Thresholds classify work using the first requested metric available for this trial.
            primary = next((value for value in ratios.values() if value is not None), None)
            if primary is not None:
                status = (
                    "faster" if primary < faster else ("slower" if primary > slower else "same")
                )
            for metric, ratio in ratios.items():
                if ratio is not None:
                    group["ratios"].setdefault(metric, []).append(ratio)
        group["counts"][status] += 1
    return groups


def _metric_cell(values):
    if not values:
        return "-"
    mean = _gmean(values)
    return f"{mean:.3f} [{min(values):.3f}, {max(values):.3f}]; {1 / mean:.3f}x; n={len(values)}"


def format_report(
    trials,
    metrics=DEFAULT_METRICS,
    *,
    baseline_trials=None,
    faster_threshold=0.95,
    slower_threshold=1.05,
):
    """Return Markdown, counting only faster/same/slower trials as correct.

    Ratios are candidate / baseline; speedups are their reciprocals.
    baseline_trials is an optional previous run, not the baseline side of a trial.
    """
    if not (
        math.isfinite(faster_threshold)
        and math.isfinite(slower_threshold)
        and 0 < faster_threshold <= 1 <= slower_threshold
        and faster_threshold < slower_threshold
    ):
        raise ValueError("thresholds must satisfy 0 < faster <= 1 <= slower and faster < slower")
    metrics = tuple(metrics)
    if not metrics or len(set(metrics)) != len(metrics):
        raise ValueError("metrics must be nonempty and unique")
    groups = _summarize(trials, metrics, faster_threshold, slower_threshold)
    previous = (
        _summarize(baseline_trials, metrics, faster_threshold, slower_threshold)
        if baseline_trials is not None
        else None
    )
    all_groups = list(groups.values()) + (list(previous.values()) if previous else [])
    visible_metrics = [
        metric
        for metric in metrics
        if any(
            trial.get("baseline", {}).get(metric) is not None
            and trial.get("candidate", {}).get(metric) is not None
            for group in all_groups
            for trial in group["trials"]
        )
    ]
    totals = sum((group["counts"] for group in groups.values()), Counter())
    lines = [
        f"# Benchmark report: {sum(totals.values())} trials across {len(groups)} scenarios",
        "",
        "Totals: " + ", ".join(f"{status}={totals[status]}" for status in STATUSES) + ".",
        "",
        "Ratios are candidate / baseline (lower is better); speedup is baseline / candidate.",
        "Metric cells show geomean ratio [min, max]; geomean speedup; contributing trial count.",
        f"Correct trials only; faster < {faster_threshold:g}, slower > {slower_threshold:g}.",
        "",
        "| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported |"
        + "".join(f" {_text(metric)} |" for metric in visible_metrics),
        "|---|" + "---:|" * (7 + len(visible_metrics)),
    ]
    for scenario, group in sorted(groups.items()):
        cells = [_text(scenario), str(len(group["trials"]))]
        cells.extend(str(group["counts"][status]) for status in STATUSES)
        cells.extend(_metric_cell(group["ratios"].get(metric)) for metric in visible_metrics)
        lines.append("| " + " | ".join(cells) + " |")
    lines.extend(("", "## Non-correct trials", ""))
    bad = [
        trial
        for _, group in sorted(groups.items())
        for trial in group["trials"]
        if trial["status"] not in CORRECT
    ]
    if bad:
        lines.extend(
            (
                "| scenario | seed | status | detail |",
                "|---|---|---|---|",
            )
        )
        for trial in bad:
            lines.append(
                "| "
                + " | ".join(
                    _text(trial.get(key, "")) for key in ("scenario", "seed", "status", "detail")
                )
                + " |"
            )
    else:
        lines.append("None.")
    if previous is not None:
        lines.extend(
            (
                "",
                "## Change against previous run",
                "",
                "Delta is current / previous geomean ratio minus one; negative means improvement.",
                "",
                "| scenario | metric | previous ratio | current ratio | delta |",
                "|---|---|---:|---:|---:|",
            )
        )
        regressions = []
        for scenario in sorted(groups.keys() | previous.keys()):
            if scenario not in groups:
                regressions.append(f"{_text(scenario)}: missing current results.")
            for metric in visible_metrics:
                old = previous.get(scenario, {}).get("ratios", {}).get(metric, [])
                new = groups.get(scenario, {}).get("ratios", {}).get(metric, [])
                old_mean = _gmean(old) if old else None
                new_mean = _gmean(new) if new else None
                change = (
                    new_mean / old_mean if old_mean is not None and new_mean is not None else None
                )
                cells = [
                    _text(scenario),
                    _text(metric),
                    f"{old_mean:.3f}" if old_mean is not None else "-",
                    f"{new_mean:.3f}" if new_mean is not None else "-",
                    f"{(change - 1) * 100:+.1f}%" if change is not None else "-",
                ]
                lines.append("| " + " | ".join(cells) + " |")
                if change is not None and change > slower_threshold:
                    regressions.append(
                        f"{_text(scenario)} / {_text(metric)}: ratio worsened "
                        f"{(change - 1) * 100:.1f}% (> {(slower_threshold - 1) * 100:g}%)."
                    )
            if scenario in groups:
                counts = groups[scenario]["counts"]
                old_counts = previous.get(scenario, {}).get("counts", Counter())
                for status in ("incorrect", "failed"):
                    if counts[status] > old_counts[status]:
                        regressions.append(
                            f"{_text(scenario)}: {status} count increased "
                            f"{old_counts[status]} -> {counts[status]}."
                        )
        lines.extend(("", "## Regressions", ""))
        if regressions:
            lines.extend(f"- {item}" for item in regressions)
        else:
            lines.append("None.")
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--baseline-json", type=Path)
    parser.add_argument("--metrics", nargs="+", default=DEFAULT_METRICS)
    parser.add_argument("--faster-threshold", type=float, default=0.95)
    parser.add_argument("--slower-threshold", type=float, default=1.05)
    args = parser.parse_args(argv)
    try:
        trials = json.loads(args.results.read_text(encoding="utf-8-sig"))["trials"]
        previous = (
            json.loads(args.baseline_json.read_text(encoding="utf-8-sig"))["trials"]
            if args.baseline_json
            else None
        )
        report = format_report(
            trials,
            args.metrics,
            baseline_trials=previous,
            faster_threshold=args.faster_threshold,
            slower_threshold=args.slower_threshold,
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.error(str(exc))
    print(report, end="")


if __name__ == "__main__":
    main()
