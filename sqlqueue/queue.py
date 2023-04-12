from datetime import datetime
from typing import Any, Dict, List, Union
import time
from psycopg import Connection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from sqlqueue import types


class Queue:
    def __init__(self, name: str, conn: Connection):
        self.name = name
        self.conn = conn
        self.conn.row_factory = dict_row
        self.conn.autocommit = False

    def create(self):
        txt = sql.SQL(
            f"CREATE TABLE IF NOT EXISTs {self.name}() INHERITS (base_queue);"
        )
        self.conn.execute(txt)
        self.conn.commit()

    def put(self, payload: Dict[str, Any]):
        txt = sql.SQL(
            "insert into {table} (payload) values (%s);".format(table=self.name)
        )
        self.conn.execute(txt, [Jsonb(payload)])
        self.conn.commit()

    def change_state(self, jobid: int, state: str):
        txt = sql.SQL(
            "UPDATE {table} SET state = %s, updated_at = %s where id = %s;".format(
                table=self.name
            )
        )
        now = datetime.utcnow()
        self.conn.execute(txt, [state, now, jobid])
        self.conn.commit()

    def get(self, block=True, timeout=10.0) -> types.Job:
        started = time.time()
        if block and timeout:
            waiting = True
            while waiting:
                rsp = self.get_nowait()
                if rsp:
                    return rsp
                elapsed = time.time() - started
                if elapsed > timeout:
                    raise TimeoutError()
                time.sleep(0.1)
        rsp = self.get_nowait()
        if not rsp:
            raise KeyError()
        return rsp

    def get_nowait(self) -> Union[types.Job, None]:
        now = datetime.utcnow()
        txt = sql.SQL(
            "UPDATE {table} SET state = 'active', updated_at = %s WHERE id = "
            "(select id from {table} where state = 'inactive'"
            " limit 1 for update skip locked) RETURNING *;".format(table=self.name)
        )
        j = None
        row = self.conn.execute(txt, [now]).fetchone()
        if row:
            j = types.Job(**row)
            # self.change_state(j.id, "active")
            # j.state = "active"
            self.conn.commit()
        return j

    def get_all(self):
        txt = sql.SQL("select * from {table};".format(table=self.name))
        rows = self.conn.execute(txt).fetchall()
        return rows

    def clean_failed(self):
        txt = sql.SQL("delete from {table} where state = %s;".format(table=self.name))
        self.conn.execute(txt, ["failed"])
        self.conn.commit()

    def clean_finished(self):
        txt = sql.SQL("delete from {table} where state = %s;".format(table=self.name))
        curr = self.conn.cursor()
        with self.conn.transaction():
            curr.execute(txt, ["finished"])
        self.conn.commit()

    def delete_queue(self):
        txt = sql.SQL("DROP TABLE {table};".format(table=self.name))
        curr = self.conn.cursor()
        with self.conn.transaction():
            curr.execute(txt)
        self.conn.commit()

    def clean(self):
        txt = sql.SQL("delete from {table};".format(table=self.name))
        curr = self.conn.cursor()
        with self.conn.transaction():
            curr.execute(txt)
        self.conn.commit()

    def delete_job(self, jobid: int):
        txt = sql.SQL("delete from {table} where id = %s;".format(table=self.name))
        curr = self.conn.cursor()
        with self.conn.transaction():
            curr.execute(txt, [jobid])
        self.conn.commit()

    def get_job(self, jobid: int) -> types.Job:
        txt = sql.SQL(
            "select * from {table} where id = %s limit 1;".format(table=self.name)
        )
        row = self.conn.execute(txt, [jobid]).fetchone()
        return types.Job(**row)

    def close(self):
        self.conn.close()
