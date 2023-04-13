import time
from datetime import datetime
from typing import Any, Dict, List, Union

from psycopg import AsyncConnection, Connection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pqq import types


def _queue_query(prefix):
    return f"SELECT table_schema,table_name FROM information_schema.tables WHERE table_name like '{prefix}%' ORDER BY table_schema,table_name"


def get_queues(conn: Connection, prefix="pqq_") -> List[types.Table]:
    stmt = sql.SQL(_queue_query(prefix))
    rows = conn.execute(stmt).fetchall()
    return [types.Table(schema_=r[0], name=r[1]) for r in rows]


async def async_get_queues(conn: AsyncConnection, prefix="pqq_") -> List[types.Table]:
    async with conn.cursor() as acur:
        stmt = sql.SQL(_queue_query(prefix))
        await acur.execute(stmt)
        rows = await acur.fetchall()
    return [types.Table(schema_=r[0], name=r[1]) for r in rows]


class Queue:
    def __init__(self, name: str, conn: Connection, sql_prefix="pqq_"):
        self._qname = name
        self.conn = conn
        self.conn.row_factory = dict_row
        self.conn.autocommit = False
        self._prefix = sql_prefix

    @property
    def name(self) -> str:
        return f"{self._prefix}{self._qname}"

    def create(self):
        txt = sql.SQL(
            f"CREATE TABLE IF NOT EXISTs {self.name}() INHERITS (base_queue);"
        )
        self.conn.execute(txt)
        self.conn.commit()

    def put(self, payload: Dict[str, Any]) -> types.Job:
        txt = sql.SQL(
            "insert into {table} (payload) values (%s) returning *;".format(
                table=self.name
            )
        )
        row = self.conn.execute(txt, [Jsonb(payload)]).fetchone()
        self.conn.commit()
        return types.Job(**row)

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


class AsyncQueue:
    def __init__(self, name: str, conn: AsyncConnection):
        self.name = name
        self.conn = conn
        self.conn.row_factory = dict_row

    @classmethod
    async def create(cls, name: str, conn: AsyncConnection) -> "AsyncQueue":
        conn.row_factory = dict_row
        await conn.set_autocommit(False)
        txt = sql.SQL(f"CREATE TABLE IF NOT EXISTs {name}() INHERITS (base_queue);")
        async with conn.cursor() as acur:
            await acur.execute(txt)
            await conn.commit()

        obj = cls(name=name, conn=conn)
        return obj

    async def put(self, payload: Dict[str, Any]) -> types.Job:
        txt = sql.SQL(
            "insert into {table} (payload) values (%s) returning *;".format(
                table=self.name
            )
        )
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [Jsonb(payload)])
            row = await acur.fetchone()
            await self.conn.commit()
        return types.Job(**row)

    async def change_state(self, jobid: int, state: str):
        txt = sql.SQL(
            "UPDATE {table} SET state = %s, updated_at = %s where id = %s;".format(
                table=self.name
            )
        )
        now = datetime.utcnow()
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [state, now, jobid])
            await self.conn.commit()

    async def get(self, block=True, timeout=10.0) -> types.Job:
        raise NotImplementedError()

    async def get_nowait(self) -> Union[types.Job, None]:
        now = datetime.utcnow()
        txt = sql.SQL(
            "UPDATE {table} SET state = 'active', updated_at = %s WHERE id = "
            "(select id from {table} where state = 'inactive'"
            " limit 1 for update skip locked) RETURNING *;".format(table=self.name)
        )
        j = None
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [now])
            row = await acur.fetchone()
            if row:
                j = types.Job(**row)
                # self.change_state(j.id, "active")
                # j.state = "active"
                await self.conn.commit()
        return j

    async def get_all(self) -> List[types.Job]:
        txt = sql.SQL("select * from {table};".format(table=self.name))
        async with self.conn.cursor() as acur:
            await acur.execute(txt)
            rows = await acur.fetchall()
        return [types.Job(**r) for r in rows]

    async def clean_failed(self):
        txt = sql.SQL("delete from {table} where state = %s;".format(table=self.name))

        async with self.conn.cursor() as acur:
            await acur.execute(txt, ["failed"])
            await self.conn.commit()

    async def clean_finished(self):
        txt = sql.SQL("delete from {table} where state = %s;".format(table=self.name))
        async with self.conn.cursor() as acur:
            await acur.execute(txt, ["finished"])
            await self.conn.commit()

    async def delete_queue(self):
        txt = sql.SQL("DROP TABLE {table};".format(table=self.name))
        async with self.conn.cursor() as acur:
            await acur.execute(txt)
            await self.conn.commit()

    async def clean(self):
        txt = sql.SQL("delete from {table};".format(table=self.name))
        async with self.conn.cursor() as acur:
            await acur.execute(txt)
            await self.conn.commit()

    async def delete_job(self, jobid: int):
        txt = sql.SQL("delete from {table} where id = %s;".format(table=self.name))
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [jobid])
            await self.conn.commit()

    async def get_job(self, jobid: int) -> types.Job:
        txt = sql.SQL(
            "select * from {table} where id = %s limit 1;".format(table=self.name)
        )

        async with self.conn.cursor() as acur:
            await acur.execute(txt, [jobid])
            row = await acur.fetchone()
        return types.Job(**row)

    async def close(self):
        await self.conn.close()
