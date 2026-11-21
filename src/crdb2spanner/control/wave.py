"""Run per-database steps (assess, convert) across many databases from one job template.

The template is a normal job YAML in which ``{db}`` is replaced by each database name. Each
database gets its own generated config and artefact directory; a summary table is written to
``<work_dir>/waves/<wave>/summary.md``.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

import yaml

STEPS = ("assess", "convert")


def render_configs(template: Path, databases: list[str], out_dir: Path) -> dict[str, Path]:
    text = template.read_text()
    if "{db}" not in text:
        raise ValueError("template must contain {db} placeholders (job_id, source.database, dsn ...)")
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = {}
    for db in databases:
        p = out_dir / f"{db}.yaml"
        p.write_text(text.replace("{db}", db))
        paths[db] = p
    return paths


def _job_dir(cfg_path: Path) -> Path:
    raw = yaml.safe_load(cfg_path.read_text())
    return Path(raw.get("work_dir", "./.local/jobs")) / raw["job_id"]


def run(template: Path, databases: list[str], steps: list[str], wave: str, work_dir: Path, log=print) -> Path:
    bad = [s for s in steps if s not in STEPS]
    if bad:
        raise ValueError(f"unsupported wave steps {bad}; supported: {', '.join(STEPS)}")
    wdir = work_dir / "waves" / wave
    configs = render_configs(template, databases, wdir / "configs")
    rows = []
    for db, cfg in configs.items():
        status = "ok"
        for step in steps:
            log(f"[{db}] {step}")
            r = subprocess.run([sys.executable, "-m", "crdb2spanner.cli.main", step, "-c", str(cfg)],
                               capture_output=True, text=True)
            (wdir / f"{db}.{step}.log").write_text(r.stdout + r.stderr)
            if r.returncode != 0:
                status = f"{step} failed (see {db}.{step}.log)"
                log(f"[{db}] {status}")
                break
        rows.append(_summarise(db, _job_dir(cfg), status))
    summary = wdir / "summary.md"
    summary.write_text(_render(wave, rows))
    log(f"summary -> {summary}")
    return summary


def _summarise(db: str, jd: Path, status: str) -> dict:
    row = {"db": db, "status": status, "tables": "", "blocker": 0, "high": 0, "medium": 0, "info": 0,
           "issues": ""}
    cat = jd / "catalog.json"
    if cat.exists():
        row["tables"] = len(json.loads(cat.read_text()).get("tables", []))
    fnd = jd / "assessment.json"
    if fnd.exists():
        c = Counter(f["severity"] for f in json.loads(fnd.read_text()))
        row.update({k: c.get(k, 0) for k in ("blocker", "high", "medium", "info")})
    iss = jd / "conversion_issues.json"
    if iss.exists():
        row["issues"] = len(json.loads(iss.read_text()))
    return row


def _render(wave: str, rows: list[dict]) -> str:
    lines = [f"# Wave `{wave}`", "",
             "| Database | Status | Tables | Blocker | High | Medium | Info | Conversion issues |",
             "|---|---|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        lines.append(f"| {r['db']} | {r['status']} | {r['tables']} | {r['blocker']} | {r['high']} | "
                     f"{r['medium']} | {r['info']} | {r['issues']} |")
    return "\n".join(lines) + "\n"
