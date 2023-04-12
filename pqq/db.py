from enum import Enum
import psycopg
from psycopg.types.enum import EnumInfo, register_enum

# import signal


SELECT_TABLES = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = '{}' ORDER BY table_schema,table_name".format(
    "public"
)


def create_conn(dsn: str, autocommit=True) -> psycopg.Connection:
    conn = psycopg.connect(dsn, autocommit=autocommit)
    return conn


def async_create_conn(dsn, autocommit=True):
    aconn = psycopg.AsyncConnection.connect(dsn, autocommit=autocommit)
    return aconn


def run_migration(dsn, fp: str):
    conn = create_conn(dsn, autocommit=False)
    try:
        with open(fp, "r") as f:
            _ddl = f.read()
        conn.execute(_ddl)
    except BaseException:
        conn.rollback()
    else:
        conn.commit()
    finally:
        conn.close()


def register_db_enum(conn, sql_name: str, enum_type: Enum):
    enum = EnumInfo(conn, sql_name)
    register_enum(enum, conn, enum_type)


def select_tables(conn, schema="public"):
    stmt = SELECT_TABLES.format(schema)
    rows = conn.execute(stmt).fetchall()
    return rows
