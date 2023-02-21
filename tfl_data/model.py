from dataclasses import dataclass
from datetime import datetime


@dataclass
class LineStatus:
    timestamp: datetime
    mode_name: str
    line_name: str
    status_severity: int
    status_description: str
    status_reason: str
