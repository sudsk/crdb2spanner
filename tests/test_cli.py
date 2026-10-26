from typer.testing import CliRunner

from crdb2spanner.cli.main import app

runner = CliRunner()


def test_help_lists_commands():
    r = runner.invoke(app, ["--help"])
    assert r.exit_code == 0
    for cmd in ("assess", "convert", "snapshot", "load", "replicate", "validate", "changefeed", "schema"):
        assert cmd in r.output


def test_convert_and_changefeed_dry_run(tmp_path, movr_catalog):
    cfg = tmp_path / "job.yaml"
    cfg.write_text(f"""
job_id: t1
work_dir: {tmp_path}/jobs
source: {{database: movr, dsn: "postgresql://root@localhost:26257/movr?sslmode=disable"}}
target: {{project: p, instance: i, database: movr, emulator_host: localhost:9010}}
cdc: {{sink: files, files_sink_uri: "nodelocal://1/cdc", files_read_path: ./x}}
""")
    jd = tmp_path / "jobs" / "t1"
    jd.mkdir(parents=True)
    (jd / "catalog.json").write_text(movr_catalog.to_json())

    r = runner.invoke(app, ["convert", "-c", str(cfg)])
    assert r.exit_code == 0, r.output
    for f in ("ddl_tables.sql", "ddl_indexes.sql", "ddl_post.sql", "mapping.json", "smt_input.sql"):
        assert (jd / f).exists(), f

    r = runner.invoke(app, ["changefeed", "create", "-c", str(cfg), "--cursor", "1.0", "--dry-run", "-t", "users"])
    assert r.exit_code == 0, r.output
    assert "CREATE CHANGEFEED FOR TABLE \"movr\".\"public\".\"users\"" in r.output

    r = runner.invoke(app, ["status", "-c", str(cfg)])
    assert "phase=CONVERTED" in r.output
