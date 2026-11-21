"""Quick target-side data helpers: exact row counts and emptying tables between test runs."""

from __future__ import annotations

from ..config import TargetConfig


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _db(target: TargetConfig):
    target.apply_emulator_env()
    from google.cloud import spanner

    return spanner.Client(project=target.project).instance(target.instance).database(target.database)


def count_rows(target: TargetConfig, tables: list[str]) -> dict[str, int | str]:
    db = _db(target)
    out: dict[str, int | str] = {}
    for t in tables:
        try:
            with db.snapshot() as snap:
                out[t] = list(snap.execute_sql(f"SELECT count(*) FROM {_q(t)}"))[0][0]
        except Exception as e:  # noqa: BLE001 - show the error in place of the count
            out[t] = f"error: {e}"
    return out


def truncate(target: TargetConfig, tables: list[str], log=print) -> None:
    """Delete every row with partitioned DML (no transaction-size limit)."""
    db = _db(target)
    for t in tables:
        try:
            n = db.execute_partitioned_dml(f"DELETE FROM {_q(t)} WHERE true")
            log(f"{t}: deleted {n} rows")
        except Exception as e:  # noqa: BLE001
            log(f"{t}: FAILED {e}")
