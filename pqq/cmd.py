import secrets
import sys
from time import time
from typing import List

from pqq import Queue, db, errors, types, utils, worker
from pqq.queue import get_queues

try:
    import click
    from rich.console import Console
    from rich.table import Table
except ImportError:
    print(
        "You need to install pqq with the click and rich dependencies: \n\t pip install pqq[click]"
    )
    sys.exit(-1)

console = Console()


@click.group()
def cli():
    """
    PQQ command line tool
    """
    pass


@click.command()
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
def migrate(sql):
    pkg = utils.get_package_dir("pqq")
    # conn = db.create_conn(sql, autocommit=False)
    db.run_migration(sql, f"{pkg}/migrations/pg.up.sql")


@click.command()
@click.argument("queue")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
@click.option("--param", "-p", multiple=True, help="define a param as <key>=<value>")
@click.option("--alias", "-a", default=secrets.token_urlsafe(8))
def send(queue, param, alias, sql):
    conn = db.create_conn(sql)
    q = Queue.create(queue, conn)
    params = {}
    for p in param:
        k, v = p.split("=")
        params[k] = v

    req = types.JobRequest(payload=params, alias=alias)
    job = q.put(req)
    print(job)


@click.command()
@click.argument("queue")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
@click.option(
    "--wait",
    default=False,
    is_flag=True,
    show_default=True,
    help="wait for a message",
)
@click.option(
    "--stream",
    default=False,
    is_flag=True,
    show_default=True,
    help="stream mode",
)
@click.option(
    "--timeout",
    "-t",
    default=10,
    show_default=True,
    help="timeout for a message",
)
def get(queue, sql, wait, timeout, stream):
    conn = db.create_conn(sql)
    q = Queue.create(queue, conn)
    job = None
    if wait:
        job = _wait(q, timeout)
    elif stream:
        while True:
            try:
                job = _wait(q, 60)
                if job:
                    print(job)
            except KeyboardInterrupt:
                q.close()

    else:
        job = q.get_nowait()

    if not job:
        print("Any message found")
    else:
        print(job)


def _wait(queue, timeout):
    job = None
    try:
        job = queue.get(timeout=timeout)
    except errors.EmptyQueue:
        pass
    return job


@click.command(name="worker")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
@click.option("--queue", "-q", default=["default"], multiple=True, help="queues names")
@click.option(
    "--w-type", "-t", default="io", type=click.Choice(["io", "cpu"]), help="Worker type"
)
def worker_cmd(queue, sql, w_type):
    if w_type == "io":
        worker.run_io(sql, queue)
    else:
        worker.run_cpu(sql, queue)


@click.command(name="list")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
@click.option("--queue", "-q", default="default", help="queue name")
def list_cmd(queue, sql):
    table = Table(title="List of jobs")
    table.add_column("jobid")
    table.add_column("alias")
    table.add_column("state")
    table.add_column("elapsed")
    conn = db.create_conn(sql)
    q = Queue.create(queue, conn)
    jobs = q.get_all()
    for t in jobs:
        if t.state == types.JobStatus.active or t.state == types.JobStatus.inactive:
            elapsed = round(utils.elapsed_time_from_start(t))
        else:
            elapsed = round(utils.elapsed_time_from_finish(t))
        if elapsed > 120:
            elapsed = f"~{round(elapsed / 60)}m"
        else:
            elapsed = f"{elapsed}s"

        table.add_row(
            f"{t.id}",
            f"{t.alias}",
            f"{t.state}",
            f"{elapsed}",
        )

    console.print(table)


@click.command(name="del")
@click.argument("jobid")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
@click.option("--queue", "-q", default="default", help="queue name")
def del_cmd(queue, sql, jobid):
    conn = db.create_conn(sql)
    q = Queue.create(queue, conn)
    q.delete_job(jobid)
    console.print(f"Jobid: {jobid} deleted")


@click.command(name="queues")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
def queues_cmd(sql):
    """list created queues"""
    conn = db.create_conn(sql)
    queues = get_queues(conn)
    table = Table(title="List of queues")
    table.add_column("qname")
    for t in queues:
        table.add_row(t.name)
    console.print(table)


@click.command(name="run-job")
@click.option(
    "--sql",
    default="host=localhost dbname=postgres user=postgres password=password",
    show_default=True,
    help="sql string connection",
)
@click.option("--param", "-p", multiple=True, help="define a param as <key>=<value>")
@click.option("--alias", "-a", default=secrets.token_urlsafe(8))
@click.option("--queue", "-a", default="default")
@click.option("--func", "-f", help="Name of the function")
def run_job(sql, param, alias, queue, func):
    conn = db.create_conn(sql)
    q = Queue.create(queue, conn)
    params = {}
    for p in param:
        k, v = p.split("=")
        params[k] = v
    payload = {"func": func, "params": params}

    req = types.JobRequest(payload=payload, alias=alias)
    job = q.put(req)
    console.print(job)


cli.add_command(migrate)
cli.add_command(send)
cli.add_command(worker_cmd)
cli.add_command(get)
cli.add_command(list_cmd)
cli.add_command(del_cmd)
cli.add_command(queues_cmd)
cli.add_command(run_job)

if __name__ == "__main__":
    cli()
