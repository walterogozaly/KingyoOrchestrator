"""Offline tests for the benchmark harness (bench/). Skipped unless the `bench` extra (duckdb) is installed.

The data is a handful of hand-written SSB-format rows, so no BenchBox generator or network is needed.
"""

import sys
import tempfile
from pathlib import Path

import pytest

pytest.importorskip("duckdb")
BENCH = Path(__file__).resolve().parents[1] / "bench"
sys.path.insert(0, str(BENCH))

import harness  # noqa: E402
import pipeline  # noqa: E402
import ssb_data  # noqa: E402


def _write_tiny_ssb(d: Path) -> None:
    d.mkdir(parents=True, exist_ok=True)
    (d / "customer.tbl").write_text(
        "1|Customer#1|addr|UNITED KI1|UNITED KINGDOM|EUROPE|11|BUILDING\n"
        "2|Customer#2|addr|CHINA0|CHINA|ASIA|22|AUTOMOBILE\n"
    )
    (d / "supplier.tbl").write_text(
        "1|Supplier#1|addr|UNITED KI5|UNITED KINGDOM|EUROPE|11\n2|Supplier#2|addr|CHINA1|CHINA|ASIA|22\n"
    )
    (d / "part.tbl").write_text(
        "1|Part 1|MFGR#1|MFGR#12|MFGR#1201|red|PROMO|5|BOX\n2|Part 2|MFGR#2|MFGR#22|MFGR#2221|blue|PROMO|7|BAG\n"
    )
    import datetime as dt

    days = [dt.date(1998, 12, 1) + dt.timedelta(days=i) for i in range(31)]
    (d / "date.tbl").write_text(
        "".join(
            f"{x:%Y%m%d}|{x:%Y-%m-%d}|Mon|December|{x.year}|{x:%Y%m}|Dec{x.year}|1|{x.day}|{x.day}|12|49|Winter|0|0|0|1\n"
            for x in days
        )
    )
    rows = []
    for i in range(1, 201):
        day = days[i % 25]
        rows.append(
            f"{i}|1|{1 + i % 2}|{1 + i % 2}|{1 + i % 2}|{day:%Y%m%d}|1-URGENT|0|{10 + i % 30}|{1000 + i}|9999|{i % 10}|{900 + i}|{50 + i % 7}|2|{day:%Y%m%d}|AIR\n"
        )
    (d / "lineorder.tbl").write_text("".join(rows))


@pytest.fixture(scope="module")
def base(tmp_path_factory):
    d = tmp_path_factory.mktemp("bench")
    _write_tiny_ssb(d / "data")
    con = harness.open_db(d / "base.duckdb")
    ssb_data.load_sources(con, d / "data")
    models = pipeline.load_repo(harness.HERE / "ssb_repo")
    pipeline.build_all(con, models)
    con.execute("CHECKPOINT")
    con.close()
    return d / "base.duckdb"


def test_repo_loads_and_is_synthetic():
    models = pipeline.load_repo(harness.HERE / "ssb_repo")
    assert {"lineorder", "fact_sales", "q1_1", "q4_3"} <= set(models)
    assert models["lineorder"].type == "declaration"
    assert models["fact_sales"].deps == ["stg_lineorder", "customer", "supplier", "part", "dates"]


@pytest.mark.parametrize("name", [n for n, s in harness.SCENARIOS.items() if not s.requires])
def test_full_rebuild_control_is_correct(base, name):
    """The control candidate must always equal the baseline; ratios near 1 show the harness is neutral."""
    with tempfile.TemporaryDirectory() as wd:
        t = harness.run_trial(
            base, harness.HERE / "ssb_repo", harness.SCENARIOS[name], 0, Path(wd), 0
        )
    assert t["status"] in ("faster", "same", "slower"), t


@pytest.mark.parametrize("candidate", ["kingyo-prototype", "kingyo-prototype-columns"])
@pytest.mark.parametrize("name", [n for n, s in harness.SCENARIOS.items() if not s.requires])
def test_prototype_matches_full_rebuild(base, name, candidate):
    """Correctness gate: the in-repo prototype must reproduce the baseline on every supported scenario."""
    pytest.importorskip("sqlglot")
    with tempfile.TemporaryDirectory() as wd:
        t = harness.run_trial(
            base,
            harness.HERE / "ssb_repo",
            harness.SCENARIOS[name],
            0,
            Path(wd),
            0,
            candidate=candidate,
        )
    assert t["status"] in ("faster", "same", "slower"), t


def test_unsupported_scenario_is_reported_not_hidden(base):
    with tempfile.TemporaryDirectory() as wd:
        t = harness.run_trial(
            base, harness.HERE / "ssb_repo", harness.SCENARIOS["model_sql_change"], 0, Path(wd), 0
        )
    assert t["status"] == "unsupported"


def test_compare_detects_differences(base):
    models = pipeline.load_repo(harness.HERE / "ssb_repo")
    import shutil

    shutil.copy(base, base.with_name("copy.duckdb"))
    a, b = harness.open_db(base), harness.open_db(base.with_name("copy.duckdb"))
    assert harness.compare(a, b, models) == []
    b.execute("UPDATE daily_revenue SET revenue = revenue + 1 WHERE rowid = 0")
    assert any("daily_revenue" in p for p in harness.compare(a, b, models))


@pytest.mark.parametrize("layout", ["order_date", "unpartitioned"])
def test_layout_variants_stay_correct(layout, tmp_path):
    """Source partitioning variants change the signal, not the answer."""
    pytest.importorskip("sqlglot")
    import upstream_layouts

    repo = upstream_layouts.make_layout_repo(harness.HERE / "ssb_repo", tmp_path / "repo", layout)
    _write_tiny_ssb(tmp_path / "data")
    con = harness.open_db(tmp_path / "b.duckdb")
    ssb_data.load_sources(con, tmp_path / "data")
    pipeline.build_all(con, pipeline.load_repo(repo))
    con.execute("CHECKPOINT")
    con.close()
    wd = tmp_path / "wd"
    wd.mkdir()
    t = harness.run_trial(
        tmp_path / "b.duckdb",
        repo,
        harness.SCENARIOS["late_updates_scattered"],
        0,
        wd,
        0,
        candidate="kingyo-prototype",
        layout=layout,
    )
    assert t["status"] in ("faster", "same", "slower"), t


def test_repeats_zero_skips_timing_but_keeps_correctness_and_work(base):
    """The fast loop (--repeats 0) still compares outputs and counts rows; it only omits wall time."""
    with tempfile.TemporaryDirectory() as wd:
        t = harness.run_trial(
            base, harness.HERE / "ssb_repo", harness.SCENARIOS["new_day"], 0, Path(wd), 0, repeats=0
        )
    assert t["status"] in ("faster", "same", "slower"), t
    assert t["baseline"]["rows_scanned"] > 0
    assert "seconds" not in t["baseline"] and "time_ratio" not in t
    assert "| - |" in harness.summarize([t])


def test_parallel_jobs_need_untimed_runs():
    with pytest.raises(SystemExit):
        harness.main(["--jobs", "2", "--repeats", "1"])
