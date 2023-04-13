# pqq

[![PyPI - Version](https://img.shields.io/pypi/v/sqlqueue.svg)](https://pypi.org/project/sqlqueue)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sqlqueue.svg)](https://pypi.org/project/sqlqueue)

-----

## Description

`pqq` stands for PostgreSQL Queue. It's a simple queue impletation using postgres. There are different implementations of this. The main inspirations come from:

- https://metacpan.org/release/SRI/Minion-10.25/source/lib/Minion/Backend/resources/migrations/pg.sql
- https://dev.to/mikevv/simple-queue-with-postgresql-1ngc

One of the benefits of using postgres is that it should be easy to implement in other languages. For that reason this repository is called py-pqq.

## Quickstart Installing

```console
pip install pqq
```

Async example (assuming IPython env): 

```python
from pqq import Queue, AsyncQueue, db

aconn = await db.async_create_conn("host=localhost dbname=postgres user=postgres password=***")
aq = await AsyncQueue.create("test2", aconn)

j = await aq.put({"name": "testing"})
Job(id=3, payload={'name': 'testing'}, try_count=0, timeout=60, max_tries=3, state='inactive', created_at=datetime.datetime(2023, 4, 13, 3, 41, 7, 865429), updated_at=datetime.datetime(2023, 4, 13, 3, 41, 7, 865429), priority=0, result=None)

# getting the job:
j = await aq.get_nowait()
```

Sync example:

```python
from pqq import Queue, AsyncQueue, db

conn = db.create_conn("host=localhost dbname=postgres user=postgres password=****")

q = Queue("test", conn)
q.create()

j = q.put({"name": "testing"})
Job(id=3, payload={'name': 'testing'}, try_count=0, timeout=60, max_tries=3, state='inactive', created_at=datetime.datetime(2023, 4, 13, 3, 41, 7, 865429), updated_at=datetime.datetime(2023, 4, 13, 3, 41, 7, 865429), priority=0, result=None)

```

## License

`pqq` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
