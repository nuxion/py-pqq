# pqq

[![PyPI - Version](https://img.shields.io/pypi/v/sqlqueue.svg)](https://pypi.org/project/sqlqueue)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sqlqueue.svg)](https://pypi.org/project/sqlqueue)

-----

## Description

`pqq` stands for PostgreSQL Queue. It's a simple queue impletation using postgres. There are different implementations of this. The main inspirations come from:

- https://metacpan.org/release/SRI/Minion-10.25/source/lib/Minion/Backend/resources/migrations/pg.sql
- https://dev.to/mikevv/simple-queue-with-postgresql-1ngc

One of the benefits of using postgres is that it should be easy to implement in other languages. For that reason this repository is called py-pqq.

## Installing

```console
pip install pqq
```

## License

`pqq` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
