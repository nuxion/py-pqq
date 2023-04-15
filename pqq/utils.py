import importlib
from datetime import datetime
from importlib import import_module
from typing import Callable, Dict

from pydantic import BaseModel

from pqq.types import Job, Payload


def _get_function(fullname) -> Callable:
    mod, name = fullname.rsplit(".", maxsplit=1)
    pkg = mod.split(".", maxsplit=1)[0]
    try:
        module = import_module(mod, pkg)
    except (ModuleNotFoundError, AttributeError):
        raise KeyError(fullname)
    return getattr(module, name)


def elapsed_time_from_finish(job: Job):
    n = datetime.utcnow()
    return (n - job.updated_at).total_seconds()


def elapsed_time_from_start(job: Job):
    n = datetime.utcnow()
    return (n - job.created_at).total_seconds()


def elapsed_time_from_start2finish(job: Job):
    return (job.updated_at - job.created_at).total_seconds()


def get_kwargs_from_func(payload: Payload, fn: Callable) -> Dict[str, BaseModel]:
    annot = list(fn.__annotations__.keys())
    kwargs = {}
    if annot:
        params_key = annot[0]
        params = fn.__annotations__[params_key](**payload.params)
        kwargs = {params_key: params}
    return kwargs


def get_function(payload: Payload) -> Callable:
    fn = _get_function(payload.func)
    return fn


def get_package_dir(pkg):
    spec = importlib.util.find_spec(pkg)
    return spec.submodule_search_locations[0]
