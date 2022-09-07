from dataclasses import dataclass
from datetime import datetime


@dataclass
class Session:
    pid: int
    username: str
    starttime: datetime
    status: str
