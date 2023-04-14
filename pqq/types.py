import secrets
from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field


def _default_alias() -> str:
    return secrets.token_urlsafe(8)


class Table(BaseModel):
    schema_: str
    name: str


class JobStatus(Enum):
    inactive = "inactive"
    active = "active"
    failed = "failed"
    finished = "finished"


class Payload(BaseModel):
    func: str
    params: Dict[str, Any] = Field(default_factory=dict)


class JobRequest(BaseModel):
    payload: Dict[str, Any]
    timeout: int = 60
    max_tries: int = 3
    # created_at: datetime = Field(default_factory=datetime.utcnow)
    priority: int = 0
    alias: str = Field(default_factory=_default_alias)


class Job(BaseModel):
    id: int
    # payload: Union[Payload, Dict[str, Any]]
    jobid: str
    payload: Dict[str, Any]
    try_count: int
    timeout: int
    max_tries: int
    state: JobStatus
    created_at: datetime
    updated_at: datetime
    priority: int
    alias: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

    class Config:
        use_enum_values = True

    # arbitrary_types_allowed = True
