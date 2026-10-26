import pytest

from crdb2spanner.config import CdcConfig
from crdb2spanner.crdb.changefeed import create_statement, sink_uri
from crdb2spanner.crdb.snapshot import export_statement


def test_pubsub_sink_uri():
    cdc = CdcConfig(sink="pubsub", pubsub_project="proj", pubsub_region="europe-west2", topic_prefix="c2s-")
    assert sink_uri(cdc, "users") == "gcpubsub://proj?topic_name=c2s-users&region=europe-west2&AUTH=implicit"


def test_files_sink_uri_keeps_query():
    cdc = CdcConfig(sink="files", files_sink_uri="gs://bkt/cdc?AUTH=implicit")
    assert sink_uri(cdc, "rides") == "gs://bkt/cdc/rides?AUTH=implicit"


def test_create_statement():
    cdc = CdcConfig(sink="files", files_sink_uri="nodelocal://1/cdc")
    sql = create_statement("movr", "public", "users", cdc, "1712345678901234567.0000000000")
    assert sql.startswith('CREATE CHANGEFEED FOR TABLE "movr"."public"."users" INTO \'nodelocal://1/cdc/users\'')
    for opt in ("cursor = '1712345678901234567.0000000000'", "updated", "mvcc_timestamp",
                "resolved = '10s'", "on_error = 'pause'", "schema_change_policy = 'stop'"):
        assert opt in sql


def test_pubsub_requires_project():
    with pytest.raises(ValueError):
        sink_uri(CdcConfig(sink="pubsub"), "t")


def test_export_statement_lists_columns_explicitly():
    sql = export_statement("movr", "public", "events", ["rowid", "kind"], "nodelocal://1/snapshot/events", "5.0")
    assert sql == ('EXPORT INTO PARQUET \'nodelocal://1/snapshot/events\' FROM SELECT "rowid", "kind" '
                   'FROM "movr"."public"."events" AS OF SYSTEM TIME \'5.0\'')
