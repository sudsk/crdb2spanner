from .catalog import Catalog, Column, ForeignKey, Index, Table  # noqa: F401


def connect(dsn: str):
    """Open an autocommit psycopg3 connection to CockroachDB."""
    import psycopg

    return psycopg.connect(dsn, autocommit=True, application_name="crdb2spanner")
