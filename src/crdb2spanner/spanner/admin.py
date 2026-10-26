"""Spanner admin helpers: create PG-dialect database (and emulator instance), apply DDL."""

from __future__ import annotations

import re

from ..config import TargetConfig


def _client(target: TargetConfig):
    target.apply_emulator_env()
    from google.cloud import spanner

    return spanner.Client(project=target.project)


def ensure_emulator_instance(target: TargetConfig) -> None:
    """Emulator only: create the instance if missing."""
    from google.api_core.exceptions import AlreadyExists

    client = _client(target)
    inst = client.instance(target.instance, configuration_name=f"projects/{target.project}/instanceConfigs/emulator-config",
                           display_name=target.instance, node_count=1)
    try:
        inst.create().result(120)
    except AlreadyExists:
        pass


def ensure_database(target: TargetConfig) -> bool:
    """Create a PostgreSQL-dialect database if it does not exist. Returns True if created."""
    from google.api_core.exceptions import AlreadyExists
    from google.cloud.spanner_admin_database_v1 import DatabaseDialect

    client = _client(target)
    db = client.instance(target.instance).database(
        target.database, database_dialect=DatabaseDialect.POSTGRESQL)
    if db.exists():
        return False
    try:
        db.create().result(300)  # PG databases are created empty; DDL is applied after
        return True
    except AlreadyExists:
        return False


def split_ddl(text: str) -> list[str]:
    text = re.sub(r"--[^\n]*", "", text)
    return [s.strip() for s in text.split(";") if s.strip()]


def apply_ddl(target: TargetConfig, statements: list[str], timeout_s: int = 1800, log=print) -> None:
    if not statements:
        log("no DDL to apply")
        return
    client = _client(target)
    db = client.instance(target.instance).database(target.database)
    existing = {s.split("(")[0].strip().lower() for s in db.ddl_statements} if db.exists() else set()
    todo = [s for s in statements if s.split("(")[0].strip().lower() not in existing]
    if not todo:
        log("all DDL already applied")
        return
    log(f"applying {len(todo)} DDL statement(s)")
    db.update_ddl(todo).result(timeout_s)
