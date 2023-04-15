import asyncio
import inspect
import logging.config
import os
import time
import traceback
from datetime import datetime
from functools import partial
from typing import Dict, List
from multiprocessing import Pool

from pydantic import BaseModel

from pqq import AsyncQueue, Queue, db, types, utils
from pqq.log import LOGGING_CONFIG_DEFAULTS, error_logger, logger


class _Task(BaseModel):
    job: types.Job
    future: asyncio.Task

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True


def _is_a_executable_job(j: types.Job) -> bool:
    if isinstance(j.payload, dict):
        if j.payload.get("func"):
            return True
    return False


def _exec_task(sql_conf, qname: str, job: types.Job):
    logger.info("Executing job %s [%s]", job.alias, job.id)
    _conn = db.create_conn(sql_conf)
    queue = Queue.create(qname, _conn)

    result = None
    payload = types.Payload(**job.payload)
    fn = utils.get_function(payload)
    kwargs = utils.get_kwargs_from_func(payload, fn)
    status = types.JobStatus.active
    result = None
    try:
        if inspect.iscoroutinefunction(fn):
            raise AttributeError("Async functions not supported yet")
        else:
            result = fn(**kwargs)
        status = types.JobStatus.finished
    except Exception as e:
        err = traceback.format_exc()
        error_logger.error("Job error %s [%s]: %s", job.alias, job.id, e)
        result = {"error": err}
    finally:
        job.updated_at = datetime.utcnow()
        job.state = status
        queue.set_result(job.jobid, status, result)
        logger.info("job %s [%s] finished as %s", job.alias, job.id, job.state.value)
    os._exit(0)


class AsyncWorker:
    def __init__(
        self, loop, base_package, queues: List[AsyncQueue], max_jobs=5, wait_for=0.8
    ):
        self.queues: Dict[str, AsyncQueue] = {q.alias: q for q in queues}
        self._max_jobs = max_jobs
        self._loop = loop
        self._base_package = base_package
        self.tasks: Dict[int, _Task] = {}
        self._wait_for = wait_for

    def start_task(self, qname: str, job: types.Job) -> asyncio.Task:
        _task = self._loop.create_task(self.exec_task(qname, job))
        self.tasks[job.id] = _Task(job=job, future=_task)
        return _task

    async def exec_task(self, qname: str, job: types.Job):
        # ctx = get_context("fork")
        # with concurrent.futures.ProcessPoolExecutor(mp_context=ctx) as pool:
        logger.info("Executing job %s [%s]", job.alias, job.id)
        result = None
        payload = types.Payload(**job.payload)
        fn = utils.get_function(payload)

        kwargs = utils.get_kwargs_from_func(payload, fn)
        status = types.JobStatus.active
        result = None
        try:
            # await self._update_status(task, status)
            if inspect.iscoroutinefunction(fn):
                result = await fn(**kwargs)
            else:
                result = await self._loop.run_in_executor(None, partial(fn, **kwargs))
            status = types.JobStatus.finished
        except asyncio.exceptions.TimeoutError as e:
            err = traceback.format_exc()
            error_logger.error("Job timeout error %s [%s]: %s", job.alias, job.id, e)
            result = {"error": err}
            status = types.JobStatus.failed
        except Exception as e:
            err = traceback.format_exc()
            error_logger.error("Job error %s [%s]: %s", job.alias, job.id, e)
            result = {"error": err}
        finally:
            logger.debug("SETTING RESULT")
            await self._set_result(qname, job, result=result, status=status)

        return result

    async def _set_result(
        self, qname: str, job: types.Job, *, result, status: types.JobStatus
    ):
        job.updated_at = datetime.utcnow()
        job.state = status
        await self.queues[qname].set_result(job.jobid, status, result)

        # await self.backend.set_result(task.id, result=result, status=status)

    async def _sentinel(self):
        logger.debug("> Cleaning")
        to_delete = []
        for k, task in self.tasks.items():
            if task.future.done():
                to_delete.append(k)
        for x in to_delete:
            del self.tasks[x]

    async def run(self):
        logger.info("> Starting worker")
        # await self.init_backend()
        sem = asyncio.Semaphore(self._max_jobs)
        while True:
            async with sem:
                for k, q in self.queues.items():
                    job = await q.get_nowait()
                    if not job:
                        await self._sentinel()
                    else:
                        await sem.acquire()
                        if _is_a_executable_job(job):
                            _task = self.start_task(k, job)
                            logger.info("job %s [%s] added", job.alias, job.id)
                            _task.add_done_callback(lambda _: sem.release())
                        else:
                            logger.warning(
                                "It's not a executable <Job id: %s>, 'func' param is missing.",
                                job.jobid,
                            )
                            await self.queues[k].change_state(
                                job.id, types.JobStatus.finished
                            )

                await asyncio.sleep(self._wait_for)


def _fork_workhorse(sql_conf, queue: str, job: types.Job):
    newpid = os.fork()
    if newpid == 0:
        _exec_task(sql_conf, queue, job)
        os._exit(0)
    else:
        pids = (os.getpid(), newpid)
        logger.debug("parent: %d, child: %d\n" % pids)


def _cpu_worker_wrapper(sql_conf, queues: List[str]):
    _queues: Dict[str, Queue] = {}
    for q in queues:
        logger.info(">> initializing queue %s", q)
        _conn = db.create_conn(sql_conf)
        _q = Queue.create(q, _conn)
        _queues[_q.alias] = _q

    logger.info("> Starting worker")
    while True:
        for k, q2 in _queues.items():
            job = q2.get_nowait()
            if job:
                if _is_a_executable_job(job):
                    logger.info("job %s [%s] added", job.alias, job.id)
                    _fork_workhorse(sql_conf, k, job)
                else:
                    logger.warning(
                        "It's not a executable <Job id: %s>, 'func' param is missing.",
                        job.jobid,
                    )
                    q2.change_state(job.id, types.JobStatus.finished)
            else:
                time.sleep(1)


def run_cpu(sql_conf, queues: List[str], configure_logging=True, log_config=None):
    pid = os.getpid()
    if configure_logging:
        dict_config = log_config or LOGGING_CONFIG_DEFAULTS
        logging.config.dictConfig(dict_config)  # type: ignore

    logger.info(">> CPU Bound worker reporting for duty: %s", pid)
    try:
        _cpu_worker_wrapper(sql_conf, queues)
    except KeyboardInterrupt:
        logger.info("Shutting down %s", pid)
    logger.info("Stopping CPU bound worker [%s]. Goodbye", pid)


async def _io_worker_wrapper(loop, sql_conf, queues: List[str], max_jobs):
    _queues = []
    for q in queues:
        logger.info(">> initializing queue %s", q)
        _conn = await db.async_create_conn(sql_conf)
        _q = await AsyncQueue.create(q, _conn)
        _queues.append(_q)

    w = AsyncWorker(loop, base_package="pqq", queues=_queues, max_jobs=max_jobs)
    await w.run()


def run_io(
    sql_conf, queues: List[str], max_jobs=5, configure_logging=True, log_config=None
):
    pid = os.getpid()
    if configure_logging:
        dict_config = log_config or LOGGING_CONFIG_DEFAULTS
        logging.config.dictConfig(dict_config)  # type: ignore

    logger.info(">> IO Bound worker reporting for duty: %s", pid)
    loop = asyncio.new_event_loop()
    try:
        # loop.create_task(scheduler.exec_task(task))
        loop.run_until_complete(_io_worker_wrapper(loop, sql_conf, queues, max_jobs))
    except KeyboardInterrupt:
        logger.info("Shutting down %s", pid)
    # finally:
    #    scheduler.finish_pending_tasks()
    logger.info("Stopping IO bound worker [%s]. Goodbye", pid)
