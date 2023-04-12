from datetime import datetime
from enum import Enum, auto
from typing import Any, Dict, Optional

from pydantic import BaseModel


class JobStatus:
    inactive = auto()
    active = auto()
    failed = auto()
    finished = auto()


class Job(BaseModel):
    id: int
    payload: Dict[str, Any]
    try_count: int
    timeout: int
    max_tries: int
    state: str
    created_at: datetime
    updated_at: datetime
    priority: int
    result: Optional[Dict[str, Any]] = None
