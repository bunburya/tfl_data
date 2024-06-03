import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from types import TracebackType
from typing import Type, Optional, Any, Sequence

import pandas as pd
from sqlalchemy import Table, MetaData, Column, DateTime, String, Integer, ForeignKey, Engine, Connection, \
    insert, create_engine, UniqueConstraint, select, Select, Row, text
from sqlalchemy.sql.functions import count

from tfl_data.parse import DataParser

# The mode names and status descriptions that I have observed in the data.

MODES = ["bus", "national-rail", "tube", "river-bus", "dlr", "cable-car", "overground", "tflrail", "tram",
         "elizabeth-line"]
STATUSES = ["Good Service", "Minor Delays", "Severe Delays", "Special Service", "Reduced Service", "No Service",
            "Suspended", "Part Suspended", "Service Closed", "Planned Closure", "Part Closure", "Bus Service"]

# Schema metadata

metadata = MetaData()

mode_table = Table("mode", metadata, Column("name", String, primary_key=True))
line_table = Table(
    "line",
    metadata,
    Column("mode", String, ForeignKey("mode.name"), nullable=False),
    Column("line", String, nullable=False),
    UniqueConstraint("mode", "line")
)
status_table = Table(
    "status",
    metadata,
    Column("description", String, primary_key=True)
)

line_observation_table = Table(
    "line_observation",
    metadata,
    Column("id", Integer, autoincrement=True, primary_key=True),
    Column("timestamp", DateTime, nullable=False),
    Column("mode", String, ForeignKey("mode.name"), nullable=False),
    Column("line", String, nullable=False),
    UniqueConstraint("timestamp", "mode", "line")
)

# disruption_table = Table(
#     "disruption",
#     metadata,
#     Column("parent", ForeignKey("line_status.id"), nullable=False),
#     Column("category", String, nullable=False),
#     Column("description", String),
#     Column("additional_info", String)
# )
line_status_table = Table(
    "line_status",
    metadata,
    Column("id", Integer, autoincrement=True, primary_key=True),
    Column("parent", Integer, ForeignKey("line_observation.id"), nullable=False),
    Column("description", String, ForeignKey("status.description"), nullable=False),
    Column("severity", Integer, nullable=False),
    Column("reason", String),
    Column("disruption_category", String),
    Column("disruption_description", String),
    Column("disruption_additional_info", String)
)


@dataclass
class DateRange:
    """A simple class representing a date(time) range. A datetime is considered to be in the range if it is between
    (and including) `start` and (but excluding) `end`.
    """
    start: datetime
    end: datetime
    duration: timedelta = field(init=False)

    def __post_init__(self):
        self.duration = self.end - self.start

    def __contains__(self, item) -> bool:
        return self.start <= item < self.end

    @classmethod
    def year(cls, y: int) -> "DateRange":
        """Generate a [DateRange] representing the given year."""
        return cls(datetime(y, 1, 1), datetime(y + 1, 1, 1))


class _DatabaseManager:
    """Base class for classes that interact with the database in some way."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self.conn: Optional[Connection] = None

    def __enter__(self):
        self.conn = self.engine.connect()
        return self

    def __exit__(self, exc_type: Type[Exception], exc_val: Exception, exc_tb: TracebackType):
        self.conn.close()
        self.conn = None


class DatabaseWriter(_DatabaseManager):
    """A class to create and populate a database from external data."""

    def __init__(self, engine: Engine):
        super().__init__(engine)
        self._lines_cache: dict[str, set[str]] = {}

    def create_tables(self):
        metadata.create_all(self.conn)

    def add_mode_line(self, mode: str, line: str):
        """Add a mode and line to the appropriate tables."""
        if mode not in self._lines_cache:
            self.conn.execute(insert(mode_table).values(name=mode))
            self.conn.execute(insert(line_table).values(mode=mode, line=line))
            self._lines_cache[mode] = {line}
        elif line not in self._lines_cache[mode]:
            self.conn.execute(insert(line_table).values(mode=mode, line=line))
            self._lines_cache[mode].add(line)

    def add_line_status(self, parent_id: int, status: dict[str, Any], commit: bool = False):
        """Add a single line status to the database."""

        disruption = status.get("disruption", {})
        disruption_category = disruption.get("category")
        disruption_description = disruption.get("description")
        disruption_additional_info = disruption.get("additionalInfo")

        self.conn.execute(insert(line_status_table).values(
            parent=parent_id,
            description=status.get("statusSeverityDescription"),
            severity=status.get("statusSeverity"),
            reason=status.get("reason"),
            disruption_category=disruption_category,
            disruption_description=disruption_description,
            disruption_additional_info=disruption_additional_info
        ))
        if commit:
            self.conn.commit()

    def add_line_observation(self, timestamp: datetime, line: dict[str, Any], commit: bool = False) -> int:
        """Add all statuses for a single line at a single point in time. Returns the number of rows added."""
        count = 0
        mode_name = line["modeName"]
        line_name = line["name"]
        self.add_mode_line(mode_name, line_name)
        result = self.conn.execute(insert(line_observation_table).values(
            timestamp=timestamp,
            mode=mode_name,
            line=line_name
        ))
        pk = result.inserted_primary_key[0]
        for s in line["lineStatuses"]:
            self.add_line_status(pk, s)
            count += 1
        if commit:
            self.conn.commit()
        return count

    def add_from_dict(self, dt: datetime, data: list[dict[str, Any]], commit: bool = False) -> int:
        """Add all line statuses from a dict parsed from a single file."""
        count = 0
        for line in data:
            count += self.add_line_observation(dt, line)
        if commit:
            self.conn.commit()
        return count

    def add_from_data(self, data_dir: str, commit: bool = False) -> int:
        """Add all line statuses from JSON files to database."""
        count = 0
        parser = DataParser(data_dir)
        for dt, d in parser.walk_category("lines"):
            if d is None:
                continue
            count += self.add_from_dict(dt, d)
        if commit:
            self.conn.commit()
        return count


class DatabaseReader(_DatabaseManager):

    def get_lines(self, mode: str) -> Sequence[str]:
        return [r[0] for r in self.conn.execute(
            select(text("line")).select_from(line_table).where(line_table.c.mode == mode)
        )]

    def observations_df(self, mode: str) -> pd.DataFrame:
        return pd.read_sql(select(line_observation_table).where(line_observation_table.c.mode == mode), self.conn)

    def statuses_df(self) -> pd.DataFrame:
        return pd.read_sql(select(line_status_table), self.conn)

    def query_observations(
            self,
            to_select: Any = "*",
            date_range: Optional[DateRange] = None,
            modes: Optional[set[str]] = None,
            lines: Optional[set[str]] = None,
            statuses: Optional[set[str]] = None
    ) -> Select:
        """Generate a query to that will return all observations meeting the specified criteria.

        :param to_select: What to select from the matching rows. Can be any value that `sqlalchemy.select` will accept.
        :param date_range: Filter to observations within the given range.
        :param modes: Filter to observations relating to any of the given modes.
        :param lines: Filter to observations relating to any of the given lines.
        :param statuses: Filter to observations having any of the given statuses.
        """
        query = (select(to_select).select_from(
            line_observation_table
        ).join_from(
            line_observation_table,
            line_status_table
        ))

        if date_range is not None:
            query = (query
                     .where(line_observation_table.c.timestamp >= date_range.start)
                     .where(line_observation_table.c.timestamp < date_range.end))
        if modes is not None:
            query = query.where(line_observation_table.c.mode.in_(modes))
        if lines is not None:
            query = query.where(line_observation_table.c.line.in_(lines))
        if statuses is not None:
            query = query.where(line_status_table.c.description.in_(statuses))

        return query.distinct()

    def get_observations(
            self,
            date_range: Optional[DateRange] = None,
            modes: Optional[set[str]] = None,
            lines: Optional[set[str]] = None,
            statuses: Optional[set[str]] = None
    ) -> Sequence[Row]:
        """Get all observations meeting the specified criteria. See docs for `query_observation` method."""
        return self.conn.execute(self.query_observations("*", date_range, modes, lines, statuses)).all()

    def count_observations(
            self,
            date_range: Optional[DateRange] = None,
            modes: Optional[set[str]] = None,
            lines: Optional[set[str]] = None,
            statuses: Optional[set[str]] = None
    ) -> int:
        """Count how many observations meet the specified criteria. See docs for `query_observation` method."""
        return self.conn.execute(self.query_observations(count(), date_range, modes, lines, statuses)).one()[0]


def get_sqlite_reader(db_fpath: str) -> DatabaseReader:
    engine = create_engine(f"sqlite:///{db_fpath}")
    return DatabaseReader(engine)


def create_database(data_dir: str, db_fpath: str):
    engine = create_engine(f"sqlite:///{db_fpath}")
    dw = DatabaseWriter(engine)
    count = 0
    with dw:
        dw.create_tables()
        count += dw.add_from_data(data_dir, commit=True)
    print(f"Added {count} line statuses.")


if __name__ == "__main__":
    create_database(sys.argv[1], sys.argv[2])
