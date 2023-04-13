from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field


class Table(BaseModel):
    schema_: str
    name: str


class JobStatus(Enum):
    inactive = auto()
    active = auto()
    failed = auto()
    finished = auto()


class Payload(BaseModel):
    func: str
    params: Dict[str, Any] = Field(default_factory=dict)


class Job(BaseModel):
    id: int
    # payload: Union[Payload, Dict[str, Any]]
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

    # class Config:
