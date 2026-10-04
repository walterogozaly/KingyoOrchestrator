"""Kingyo pipeline-benchmark harness (offline DuckDB backend; BigQuery backend plugs in via the Backend protocol).

For every (scenario, seed): copy a fully built warehouse twice, apply the same source mutation to both,
  baseline  = plain Dataform behaviour: rebuild every model from scratch (pipeline.build_all)
  candidate = Kingyo: signal the change, run_once()
then require candidate output == baseline output (multiset equality per model) before any speed credit.
Work is measured as rows scanned by TABLE_SCAN operators (DuckDB profiling) plus statement count and wall time;
on BigQuery the same slots hold bytes processed/billed and total_slot_ms (see README, `BQ_METRICS`).
"""

from __future__ import annotations

import json
import math
import random
import shutil
import statistics
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

import pipeline  # noqa: E402
from candidates import CANDIDATES  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402

BQ_METRICS = (
    "total_bytes_processed",
    "total_bytes_billed",
    "total_slot_ms",
    "elapsed_ms",
    "n_jobs",
    "shuffle/spill from query plan",
)


# ---------------------------------------------------------------- metering
@dataclass
class Work:
    rows_scanned: int = 0
    statements: int = 0
    seconds: float = 0.0


class Meter:
    """Connection proxy: every execute() is profiled and its table-scan rows added to `work`."""

    def __init__(self, con, profile=True):
        self._con, self.work, self._profile = con, Work(), profile
        self._f = tempfile.NamedTemporaryFile(suffix=".json", delete=False).name
        if profile:
            con.execute("PRAGMA enable_profiling='json'")
            con.execute(f"PRAGMA profiling_output='{self._f}'")

    def execute(self, q, params=None):
        r = self._con.execute(q, params) if params is not None else self._con.execute(q)
        self.work.statements += 1
        if self._profile:
            try:
                self.work.rows_scanned += _scanned(json.load(open(self._f)))
            except (ValueError, OSError):
                pass
        return r


def _scanned(node):
    n = node.get("operator_rows_scanned", 0) if "SCAN" in str(node.get("operator_type", "")) else 0
    return n + sum(_scanned(c) for c in node.get("children", []))


# ---------------------------------------------------------------- build / compare
def open_db(path: Path):
    """Warehouse connection. Small row groups + tables clustered by partition expression make DuckDB zone maps
    behave like partition pruning: `d IN (DATE ...)` reads only the matching row groups, `CAST(d AS VARCHAR) IN` reads all.
    This is a proxy for BigQuery pruning, not a measurement of it."""
    con = duckdb.connect()
    con.execute(f"ATTACH '{path}' AS w (ROW_GROUP_SIZE 2048)")
    con.execute("USE w")
    return con


def build_base(db: Path, repo: Path, data_dir: Path, sf: float):
    from ssb_data import generate, load_sources

    d = generate(sf, data_dir)
    con = open_db(db)
    load_sources(con, d)
    models = pipeline.load_repo(repo)
    pipeline.build_all(con, models)
    for n in pipeline.topo_order(models):  # cluster every table by its partition expression
        if models[n].partition_expr:
            con.execute(
                f"CREATE OR REPLACE TABLE {n} AS SELECT * FROM {n} ORDER BY {models[n].partition_expr}"
            )
    con.execute("CHECKPOINT")
    con.close()


def compare(con_a, con_b, models):
    """Multiset equality of every derived model; returns list of problems (empty == identical)."""
    bad = []
    for n in pipeline.topo_order(models):
        if models[n].type == "declaration":
            continue
        ca = [
            r[0]
            for r in con_a.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name='{n}' ORDER BY ordinal_position"
            ).fetchall()
        ]
        cb = [
            r[0]
            for r in con_b.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name='{n}' ORDER BY ordinal_position"
            ).fetchall()
        ]
        if ca != cb:
            bad.append(f"{n}: columns differ")
            continue
        # compare via exported rows (separate connections); canonical sort makes it a multiset comparison
        ra = sorted(map(repr, con_a.execute(f"SELECT * FROM {n}").fetchall()))
        rb = sorted(map(repr, con_b.execute(f"SELECT * FROM {n}").fetchall()))
        if ra != rb:
            bad.append(f"{n}: {len(ra)} vs {len(rb)} rows, content differs")
    return bad


def _copy(base, work_dir, tag):
    p = Path(work_dir) / f"{tag}.duckdb"
    shutil.copy(base, p)
    return p


# ---------------------------------------------------------------- one trial
def run_trial(
    base: Path,
    repo: Path,
    scenario,
    seed: int,
    work_dir: Path,
    order_seed: int,
    repeats: int = 1,
    candidate: str = "full-rebuild",
):
    models = pipeline.load_repo(repo)
    cand = CANDIDATES[candidate]()
    out = {"scenario": scenario.name, "seed": seed, "status": None, "detail": ""}
    if not scenario.requires <= cand.capabilities:
        out.update(
            status="unsupported",
            detail=f"candidate lacks {sorted(scenario.requires - cand.capabilities)}",
        )
        return out

    def run_side(side, profile):
        p = _copy(base, work_dir, f"{side}_{profile}")
        raw = open_db(p)
        sigs = scenario.mutate(
            raw, random.Random(seed)
        )  # identical mutation on both sides (same seed)
        m = Meter(raw, profile)
        t = time.perf_counter()
        if side == "baseline":
            pipeline.build_all(m, models)
        else:
            cand.run(m, repo, models, sigs)
        m.work.seconds = time.perf_counter() - t
        return raw, m.work, p

    try:
        # correctness + work pass (profiled)
        rb, wb, _ = run_side("baseline", True)
        rc, wc, _ = run_side("candidate", True)
        bad = compare(rc, rb, models)
        # timing pass (unprofiled), randomized order, repeated, median
        tb, tc = [], []
        rng = random.Random(order_seed)
        for _ in range(repeats):
            order = ["baseline", "candidate"]
            rng.shuffle(order)
            for s in order:
                _, w, _ = run_side(s, False)
                (tb if s == "baseline" else tc).append(w.seconds)
    except Exception as e:  # a crash is a result, not a harness bug
        out.update(status="failed", detail=f"{type(e).__name__}: {str(e)[:200]}")
        return out
    out.update(
        baseline={
            "rows_scanned": wb.rows_scanned,
            "statements": wb.statements,
            "seconds": statistics.median(tb),
        },
        candidate={
            "rows_scanned": wc.rows_scanned,
            "statements": wc.statements,
            "seconds": statistics.median(tc),
        },
        scan_ratio=wc.rows_scanned / max(wb.rows_scanned, 1),
        time_ratio=statistics.median(tc) / max(statistics.median(tb), 1e-9),
    )
    if bad:
        out.update(
            status="incorrect",
            detail="; ".join(bad[:4]) + (f" (+{len(bad) - 4} more)" if len(bad) > 4 else ""),
        )
    else:
        r = out["scan_ratio"]
        out["status"] = "faster" if r < 0.95 else ("same" if r <= 1.05 else "slower")
    return out


# ---------------------------------------------------------------- report
def gmean(xs):
    xs = [max(x, 1e-6) for x in xs]
    return math.exp(sum(map(math.log, xs)) / len(xs)) if xs else float("nan")


def summarize(trials):
    rows = {}
    for t in trials:
        rows.setdefault(t["scenario"], []).append(t)
    lines = [
        "| scenario | trials | faster | same | slower | INCORRECT | failed | unsupported | scan ratio (gmean, correct only) | time ratio (gmean) | baseline rows | candidate rows |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for s, ts in rows.items():
        c = lambda k, ts=ts: sum(t["status"] == k for t in ts)  # noqa: E731
        ok = [t for t in ts if t["status"] in ("faster", "same", "slower")]
        sr = f"{gmean([t['scan_ratio'] for t in ok]):.3f}" if ok else "-"
        tr = f"{gmean([t['time_ratio'] for t in ok]):.2f}" if ok else "-"
        br = f"{int(statistics.mean(t['baseline']['rows_scanned'] for t in ok)):,}" if ok else "-"
        cr = f"{int(statistics.mean(t['candidate']['rows_scanned'] for t in ok)):,}" if ok else "-"
        lines.append(
            f"| {s} | {len(ts)} | {c('faster')} | {c('same')} | {c('slower')} | {c('incorrect')} | {c('failed')} | {c('unsupported')} | {sr} | {tr} | {br} | {cr} |"
        )
    return "\n".join(lines)


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, default=0.05)
    ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--scenarios", nargs="*")
    ap.add_argument("--candidate", default="full-rebuild", choices=list(CANDIDATES))
    ap.add_argument("--repo", default=str(HERE / "ssb_repo"))
    ap.add_argument("--cache", default=str(Path(tempfile.gettempdir()) / "kingyo_bench_cache"))
    ap.add_argument("--out", default=str(HERE / "results" / "latest"))
    a = ap.parse_args(argv)
    cache = Path(a.cache)
    cache.mkdir(parents=True, exist_ok=True)
    base = cache / f"base_sf{a.sf}.duckdb"
    if not base.exists():
        build_base(base, Path(a.repo), cache / f"ssb_sf{a.sf}", a.sf)
    names = a.scenarios or list(SCENARIOS)
    trials = []
    with tempfile.TemporaryDirectory() as wd:
        for n in names:
            for seed in range(a.seeds):
                t = run_trial(
                    base,
                    Path(a.repo),
                    SCENARIOS[n],
                    seed,
                    Path(wd),
                    order_seed=seed,
                    repeats=a.repeats,
                    candidate=a.candidate,
                )
                trials.append(t)
                print(
                    f"{n:<24} seed {seed}: {t['status']:<11} scan {t.get('scan_ratio', float('nan')):.3f} time {t.get('time_ratio', float('nan')):.2f} {t['detail'][:100]}",
                    flush=True,
                )
    md = summarize(trials)
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(str(a.out) + ".json").write_text(
        json.dumps({"sf": a.sf, "seeds": a.seeds, "trials": trials}, indent=1, default=str)
    )
    Path(str(a.out) + ".md").write_text(md + "\n")
    print("\n" + md)


if __name__ == "__main__":
    main()
