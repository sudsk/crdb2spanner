"""crdb2spanner CLI.

Every command takes ``--config job.yaml``; artefacts land in ``<work_dir>/<job_id>/``:

    catalog.json  assessment.md/.json  ddl_tables.sql  ddl_indexes.sql  ddl_post.sql
    mapping.json  smt_input.sql  snapshot.json  state.json

Beam/Dataflow options go after ``--``, e.g.
    crdb2spanner load -c job.yaml -- --runner DataflowRunner --project P --region R ...
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from ..config import JobConfig, load_job_config
from ..control.state import JobState

app = typer.Typer(no_args_is_help=True, add_completion=False, help="CockroachDB -> Spanner (PG) migration")
cf_app = typer.Typer(no_args_is_help=True, help="Manage CockroachDB changefeeds")
schema_app = typer.Typer(no_args_is_help=True, help="Apply Spanner DDL")
dev_app = typer.Typer(no_args_is_help=True, help="Local test-environment helpers")
app.add_typer(cf_app, name="changefeed")
app.add_typer(schema_app, name="schema")
app.add_typer(dev_app, name="dev")

ConfigOpt = typer.Option(..., "--config", "-c", help="job YAML", exists=True, dir_okay=False)
BEAM_CTX = {"allow_extra_args": True, "ignore_unknown_options": True}


def _ctx(config: Path) -> tuple[JobConfig, Path, JobState]:
    cfg = load_job_config(config)
    jd = cfg.job_dir
    return cfg, jd, JobState.load(jd, cfg.job_id)


def _mapping(jd: Path):
    from ..schema.mapping import MappingSpec

    p = jd / "mapping.json"
    if not p.exists():
        raise typer.BadParameter(f"{p} not found - run `crdb2spanner convert` first")
    return MappingSpec.load(p)


def _tables(cfg: JobConfig, mapping, table: Optional[list[str]]) -> list[str]:
    names = [t.source for t in mapping.tables if cfg.selects(t.source)]
    if table:
        names = [n for n in names if n in table]
    return names


# --------------------------------------------------------------------------------------
@app.command()
def assess(config: Path = ConfigOpt):
    """Introspect the source and write catalog.json + assessment report."""
    from ..assess.rules import assess as run_rules
    from ..assess.rules import render_json, render_markdown
    from ..crdb import connect
    from ..crdb.catalog import introspect

    cfg, jd, st = _ctx(config)
    with connect(cfg.source.resolve_dsn()) as conn:
        cat = introspect(conn, cfg.source.database, cfg.source.schema)
    (jd / "catalog.json").write_text(cat.to_json())
    findings = run_rules(cat, cfg.tables or None, cfg.exclude_tables)
    (jd / "assessment.md").write_text(render_markdown(cat, findings))
    (jd / "assessment.json").write_text(render_json(findings))
    st.advance(jd, "ASSESSED", tables=len(cat.tables), findings=len(findings))
    blockers = [f for f in findings if f.severity == "blocker"]
    typer.echo(f"{len(cat.tables)} tables, {len(findings)} findings ({len(blockers)} blockers) "
               f"-> {jd / 'assessment.md'}")


@app.command()
def convert(config: Path = ConfigOpt,
            smt: bool = typer.Option(True, help="also write smt_input.sql for the SMT UI")):
    """Generate Spanner PG-dialect DDL + mapping.json (built-in converter)."""
    from ..crdb.catalog import Catalog
    from ..schema.converter import convert as run_convert
    from ..schema.smt_bridge import crdb_to_pg_ddl

    cfg, jd, st = _ctx(config)
    cat = Catalog.load(jd / "catalog.json")
    res = run_convert(cat, only=cfg.tables or None, exclude=cfg.exclude_tables,
                      strict_shadow=(cfg.apply.mode == "strict"),
                      shadow_prefix=cfg.apply.shadow_prefix, uuid_native=cfg.uuid_native,
                      target_database=cfg.target.database)
    for phase in ("tables", "indexes", "post"):
        (jd / f"ddl_{phase}.sql").write_text(res.render(phase))
    (jd / "mapping.json").write_text(res.mapping.to_json())
    (jd / "conversion_issues.json").write_text(json.dumps([i.__dict__ for i in res.issues], indent=2))
    if smt:
        src = "\n".join(t.create_statement.rstrip(";") + ";" for t in cat.tables if cfg.selects(t.name))
        (jd / "smt_input.sql").write_text(crdb_to_pg_ddl(src))
    st.advance(jd, "CONVERTED", issues=len(res.issues))
    typer.echo(f"wrote DDL + mapping for {len(res.mapping.tables)} tables, {len(res.issues)} issues -> {jd}")


@schema_app.command("apply")
def schema_apply(config: Path = ConfigOpt,
                 phase: str = typer.Option("tables", help="tables | indexes | post"),
                 create_database: bool = typer.Option(False, help="create the PG-dialect DB if missing")):
    """Apply a DDL phase to Spanner."""
    from ..spanner.admin import apply_ddl, ensure_database, split_ddl

    cfg, jd, st = _ctx(config)
    if create_database and ensure_database(cfg.target):
        typer.echo(f"created PostgreSQL-dialect database {cfg.target.database}")
    apply_ddl(cfg.target, split_ddl((jd / f"ddl_{phase}.sql").read_text()), log=typer.echo)
    next_phase = {"tables": "SCHEMA_APPLIED", "indexes": "INDEXES_APPLIED"}.get(phase, st.phase)
    st.advance(jd, next_phase, ddl_phase=phase)


@app.command()
def snapshot(config: Path = ConfigOpt, t0: Optional[str] = typer.Option(None, help="reuse an HLC")):
    """Pick T0 and EXPORT every mapped table AS OF SYSTEM TIME T0 to Parquet."""
    from ..crdb import connect
    from ..crdb.snapshot import gc_ttl_seconds, take_snapshot

    cfg, jd, st = _ctx(config)
    mapping = _mapping(jd)
    mapping.tables = [t for t in mapping.tables if cfg.selects(t.source)]
    with connect(cfg.source.resolve_dsn()) as conn:
        for t in mapping.tables:
            ttl = gc_ttl_seconds(conn, cfg.source.database, t.source)
            if ttl is not None and ttl < 3600:
                typer.secho(f"warning: {t.source} gc.ttlseconds={ttl}; snapshot + changefeed start "
                            "must complete inside this window", fg="yellow")
        man = take_snapshot(conn, cfg.source.database, cfg.source.schema, mapping,
                            cfg.snapshot.export_uri, t0=t0, log=typer.echo)
    man.save(jd / "snapshot.json")
    st.t0 = man.t0
    st.advance(jd, "SNAPSHOT_TAKEN", t0=man.t0, rows=man.rows)
    typer.echo(f"T0 = {man.t0}  rows = {sum(man.rows.values())}")


@app.command(context_settings=BEAM_CTX)
def load(ctx: typer.Context, config: Path = ConfigOpt,
         table: Optional[list[str]] = typer.Option(None, "--table", "-t")):
    """Dataflow/Beam batch: snapshot Parquet -> Spanner."""
    from ..pipelines import bulk_load

    cfg, jd, st = _ctx(config)
    mapping = _mapping(jd)
    tables = _tables(cfg, mapping, table)
    bulk_load.run(cfg, mapping, cfg.snapshot.read_path, tables, list(ctx.args))
    st.advance(jd, "LOADED", tables=tables)
    typer.echo(f"bulk load finished for {len(tables)} tables")


@cf_app.command("create")
def changefeed_create(config: Path = ConfigOpt,
                      table: Optional[list[str]] = typer.Option(None, "--table", "-t"),
                      cursor: Optional[str] = typer.Option(None, help="defaults to snapshot T0"),
                      dry_run: bool = typer.Option(False, help="print SQL only")):
    """Create one changefeed per table starting at cursor=T0."""
    from ..crdb import connect
    from ..crdb.changefeed import create, create_statement

    cfg, jd, st = _ctx(config)
    mapping = _mapping(jd)
    cursor = cursor or st.t0
    if not cursor:
        raise typer.BadParameter("no cursor: run `snapshot` first or pass --cursor")
    tables = _tables(cfg, mapping, table)
    if dry_run:
        for t in tables:
            typer.echo(create_statement(cfg.source.database, cfg.source.schema, t, cfg.cdc, cursor) + ";")
        return
    with connect(cfg.source.resolve_dsn()) as conn:
        for t in tables:
            if t in st.changefeeds:
                typer.echo(f"{t}: changefeed {st.changefeeds[t]} already recorded, skipping")
                continue
            jid = create(conn, cfg.source.database, cfg.source.schema, t, cfg.cdc, cursor)
            st.changefeeds[t] = jid
            typer.echo(f"{t}: changefeed job {jid}")
    st.advance(jd, "REPLICATING", cursor=cursor)


@cf_app.command("list")
def changefeed_list(config: Path = ConfigOpt):
    """Show changefeed jobs and their lag (from the high-water timestamp)."""
    from ..crdb import connect
    from ..crdb.changefeed import list_jobs

    cfg, jd, st = _ctx(config)
    ids = set(st.changefeeds.values())
    with connect(cfg.source.resolve_dsn()) as conn:
        for j in list_jobs(conn):
            mark = "*" if j.job_id in ids else " "
            lag = f"{j.lag_seconds:8.1f}s" if j.lag_seconds is not None else "       -"
            typer.echo(f"{mark} {j.job_id}  {j.status:<10} lag={lag}  {j.description[:90]}")


@cf_app.command("cancel")
def changefeed_cancel(config: Path = ConfigOpt,
                      table: Optional[list[str]] = typer.Option(None, "--table", "-t")):
    """Cancel this job's changefeeds (all, or the given tables)."""
    from ..crdb import connect
    from ..crdb.changefeed import cancel

    cfg, jd, st = _ctx(config)
    with connect(cfg.source.resolve_dsn()) as conn:
        for t, jid in list(st.changefeeds.items()):
            if table and t not in table:
                continue
            cancel(conn, jid)
            st.changefeeds.pop(t)
            typer.echo(f"{t}: cancelled {jid}")
    st.save(jd)


@app.command(context_settings=BEAM_CTX)
def replicate(ctx: typer.Context, config: Path = ConfigOpt,
              source: Optional[str] = typer.Option(None, help="pubsub | files (default: cdc.sink)"),
              follow: bool = typer.Option(False, help="files source: keep watching for new files"),
              table: Optional[list[str]] = typer.Option(None, "--table", "-t")):
    """Dataflow/Beam CDC apply: changefeed -> Spanner."""
    from ..pipelines import cdc_apply

    cfg, jd, st = _ctx(config)
    mapping = _mapping(jd)
    tables = _tables(cfg, mapping, table)
    cdc_apply.run(cfg, mapping, tables, list(ctx.args), source=source or cfg.cdc.sink, follow=follow)


@app.command()
def validate(config: Path = ConfigOpt,
             table: Optional[list[str]] = typer.Option(None, "--table", "-t"),
             as_of: Optional[str] = typer.Option(None, help="HLC to compare at (default: now)"),
             buckets: int = typer.Option(1024)):
    """Compare source and target at one timestamp (counts + bucketed row hashes)."""
    from ..validate.compare import run as run_validate

    cfg, jd, st = _ctx(config)
    mapping = _mapping(jd)
    results = run_validate(cfg, mapping, _tables(cfg, mapping, table), as_of, buckets, log=typer.echo)
    (jd / "validation.json").write_text(json.dumps([r.__dict__ for r in results], indent=2))
    if all(r.ok for r in results):
        st.advance(jd, "VALIDATED", tables=len(results))
        typer.secho("validation passed", fg="green")
    else:
        typer.secho("validation FAILED", fg="red")
        raise typer.Exit(1)


@app.command()
def status(config: Path = ConfigOpt):
    """Job phase, T0, changefeeds."""
    cfg, jd, st = _ctx(config)
    typer.echo(f"job {st.job_id}: phase={st.phase} t0={st.t0}")
    for t, j in st.changefeeds.items():
        typer.echo(f"  changefeed {t}: {j}")
    typer.echo(f"  artefacts: {jd}")


@dev_app.command("init-emulator")
def dev_init_emulator(config: Path = ConfigOpt):
    """Create the emulator instance + PG-dialect database from the job config."""
    from ..spanner.admin import ensure_database, ensure_emulator_instance

    cfg = load_job_config(config)
    if not cfg.target.emulator_host:
        raise typer.BadParameter("target.emulator_host is not set - refusing to touch a real instance")
    ensure_emulator_instance(cfg.target)
    created = ensure_database(cfg.target)
    typer.echo(f"emulator instance {cfg.target.instance} ready; database {cfg.target.database} "
               f"{'created' if created else 'exists'} (PostgreSQL dialect)")


if __name__ == "__main__":
    app()
