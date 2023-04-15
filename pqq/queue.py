import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from psycopg import AsyncConnection, Connection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pqq import db, defaults, errors, types


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
        self.alias = name
        self.conn = conn
        self.conn.row_factory = dict_row
        self.conn.autocommit = False
        self._prefix = sql_prefix

    @property
    def name(self) -> str:
        return f"{self._prefix}{self.alias}"

    @classmethod
    def create(cls, name: str, conn: Connection, sql_prefix="pqq_") -> "Queue":
        txt = sql.SQL(
            f"""CREATE TABLE IF NOT EXISTS {sql_prefix}{name}(
                 LIKE {defaults.BASE_TABLE} INCLUDING INDEXES
            ) INHERITS ({defaults.BASE_TABLE});"""
        )
        conn.execute(txt)
        conn.commit()
        db.register_db_enum(conn, defaults.ENUM_STATE, types.JobStatus)

        obj = cls(name=name, conn=conn, sql_prefix=sql_prefix)
        return obj

    def put(self, req: types.JobRequest) -> types.Job:
        txt = sql.SQL(
            f"insert into {self.name} (payload, timeout, max_tries, priority, alias) values (%(payload)s, %(timeout)s, %(max_tries)s, %(priority)s, %(alias)s) returning *;"
        )

        _dict = req.dict()
        _dict["payload"] = Jsonb(req.payload)
        row = self.conn.execute(txt, _dict).fetchone()
        self.conn.commit()
        return types.Job(**row)

    def change_state(self, jobid: int, state: types.JobStatus):
        txt = sql.SQL(
            "UPDATE {table} SET state = %s, updated_at = %s where id = %s;".format(
                table=self.name
            )
        )
        now = datetime.utcnow()
        self.conn.execute(txt, [state, now, jobid])
        self.conn.commit()

    def set_result(self, jobid: str, state: types.JobStatus, result: Dict[str, Any]):
        txt = sql.SQL(
            f"UPDATE {self.name} SET state = %s, updated_at = %s, result = %s "
            "where jobid = %s;"
        )
        now = datetime.utcnow()
        self.conn.execute(txt, [state, now, Jsonb(result), jobid])
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
                    raise errors.EmptyQueue(self.alias)
                time.sleep(0.1)
        rsp = self.get_nowait()
        if not rsp:
            raise errors.EmptyQueue(self.alias)
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

    def get_all(self, filter_by: Optional[types.JobStatus] = None) -> List[types.Job]:
        if filter_by:
            txt = sql.SQL(f"select * from {self.name} where state=%s;")
            rows = self.conn.execute(txt, [filter_by]).fetchall()
        else:
            txt = sql.SQL("select * from {table};".format(table=self.name))
            rows = self.conn.execute(txt).fetchall()

        return [types.Job(**r) for r in rows]

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

    def delete_job(self, jobid: str):
        txt = sql.SQL(f"delete from {self.name} where jobid = %s;")
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
    def __init__(self, name: str, conn: AsyncConnection, sql_prefix="pqq_"):
        self.alias = name
        self.conn = conn
        self.conn.row_factory = dict_row
        self._prefix = sql_prefix

    @property
    def name(self) -> str:
        return f"{self._prefix}{self.alias}"

    @classmethod
    async def create(
        cls, name: str, conn: AsyncConnection, sql_prefix="pqq_"
    ) -> "AsyncQueue":
        conn.row_factory = dict_row
        await conn.set_autocommit(False)

        txt = sql.SQL(
            f"""CREATE TABLE IF NOT EXISTS {sql_prefix}{name}(
                 LIKE {defaults.BASE_TABLE} INCLUDING INDEXES
            ) INHERITS ({defaults.BASE_TABLE});"""
        )

        async with conn.cursor() as acur:
            await acur.execute(txt)
            await conn.commit()

        await db.async_register_db_enum(conn, defaults.ENUM_STATE, types.JobStatus)
        obj = cls(name=name, conn=conn, sql_prefix=sql_prefix)
        return obj

    async def put(self, req: types.JobRequest) -> types.Job:
        txt = sql.SQL(
            "insert into {table} (payload, timeout, max_tries, priority, alias) values (%(payload)s, %(timeout)s, %(max_tries)s, %(priority)s, %(alias)s) returning *;".format(
                table=self.name
            )
        )
        async with self.conn.cursor() as acur:
            _dict = req.dict()
            _dict["payload"] = Jsonb(req.payload)
            await acur.execute(txt, _dict)
            row = await acur.fetchone()
            await self.conn.commit()
        return types.Job(**row)

    async def change_state(self, jobid: int, state: types.JobStatus):
        txt = sql.SQL(
            "UPDATE {table} SET state = %s, updated_at = %s where id = %s;".format(
                table=self.name
            )
        )
        now = datetime.utcnow()
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [state, now, jobid])
            await self.conn.commit()

    async def set_result(
        self, jobid: str, state: types.JobStatus, result: Dict[str, Any]
    ):
        txt = sql.SQL(
            f"UPDATE {self.name} SET state = %s, updated_at = %s, result = %s "
            "where jobid = %s;"
        )
        now = datetime.utcnow()
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [state, now, Jsonb(result), jobid])
            await self.conn.commit()

    async def get(self, block=True, timeout=10.0) -> types.Job:
        fut = asyncio.create_task(self.get_nowait())
        if block:
            try:
                res = await asyncio.wait_for(fut, timeout=timeout)
                return res
            except TimeoutError as e:
                raise errors.EmptyQueue(self.alias) from e
        else:
            res = await fut
            if not res:
                raise errors.EmptyQueue(self.alias)
            return res

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

    async def get_all(
        self, filter_by: Optional[types.JobStatus] = None
    ) -> List[types.Job]:
        async with self.conn.cursor() as acur:
            if filter_by:
                txt = sql.SQL(f"select * from {self.name} where state = %s;")
                await acur.execute(txt, [filter_by])
            else:
                txt = sql.SQL(f"select * from {self.name} where state = %s;")
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

    async def delete_job(self, jobid: str):
        txt = sql.SQL(f"delete from {self.name} where jobid = %s;")
        async with self.conn.cursor() as acur:
            await acur.execute(txt, [jobid])
            await self.conn.commit()

    async def get_job(self, jobid: str) -> types.Job:
        txt = sql.SQL(f"select * from {self.name} where jobid = %s limit 1;")

        async with self.conn.cursor() as acur:
            await acur.execute(txt, [jobid])
            row = await acur.fetchone()
        return types.Job(**row)

    async def close(self):
        await self.conn.close()
