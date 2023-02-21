import json
import sqlite3 as sql
from datetime import datetime
from typing import Optional, Any, Tuple, Type, List

import pandas as pd

from tfl_data.parse import DataParser





class DatabaseManager:

    # Character we use to separate status descriptions in the "statuses" column of the line_statuses table.
    # Must not appear in any status description reported by the TfL API.
    LINE_STATUS_DELIM = ';'

    MODE_NAME_TABLE = """
        CREATE TABLE IF NOT EXISTS \"mode_names\" (
            mode_name TEXT PRIMARY KEY
        )
    """

    LINE_NAME_TABLE = """
        CREATE TABLE IF NOT EXISTS \"line_names\" (
            mode_name TEXT NOT NULL,
            line_name TEXT NOT NULL,
            FOREIGN KEY (mode_name) REFERENCES mode_names (mode_name),
            PRIMARY KEY (mode_name, line_name)
        )
    """

    LINE_STATUS_TABLE = """
        CREATE TABLE IF NOT EXISTS \"line_statuses\" (
            timestamp TEXT NOT NULL,
            mode_name TEXT NOT NULL,
            line_name TEXT NOT NULL,
            statuses TEXT NOT NULL,
            FOREIGN KEY (mode_name, line_name) REFERENCES line_names (mode_name, line_name)
        )
    """

    ADD_STATUS = """
        INSERT INTO \"line_statuses\" (
            timestamp,
            mode_name,
            line_name,
            statuses
        )
        VALUES(?, ?, ?, ?)
    """

    ADD_MODE = """
        INSERT OR IGNORE INTO \"mode_names\" (
            mode_name
        )
        VALUES (?)
    """

    ADD_LINE = """
        INSERT OR IGNORE INTO \"line_names\" (
            mode_name,
            line_name
        )
        VALUES (?, ?)
    """

    def __init__(self, db_fpath: str):
        self.connection = sql.connect(db_fpath, detect_types=sql.PARSE_DECLTYPES | sql.PARSE_COLNAMES)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA foreign_keys = ON')
        self.create_tables()

    def _parse_line_entries(
            self, timestamp: datetime, mode_name: str, line_name: str, statuses: str
    ) -> tuple[datetime, str, str, set[str]]:
        """Parse the output of an SQL query on the database and return a tuple representing properly structured data."""
        return timestamp, mode_name, line_name, set(statuses.split(self.LINE_STATUS_DELIM))

    @property
    def table_names(self) -> list[str]:
        """Get the names of all tables in the database."""
        self.cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
        return [r[0] for r in self.cursor.fetchall()]

    def get_column_names(self, table_name: str) -> list[str]:
        """Get the names of all columns in the given table."""
        # Sanity check to ensure table_name is actually a table name
        if table_name not in self.table_names:
            raise ValueError(f'{table_name} is not a valid table name.')
        self.cursor.execute(f'PRAGMA table_info({table_name})')
        return [r[1] for r in self.cursor.fetchall()]

    def create_tables(self, commit: bool = True):
        self.cursor.execute(self.MODE_NAME_TABLE)
        self.cursor.execute(self.LINE_NAME_TABLE)
        self.cursor.execute(self.LINE_STATUS_TABLE)
        if commit:
            self.connection.commit()

    def add_mode_line(self, mode_name: str, line_name: str, commit: bool = True):
        """Add given mode name and line name to the relevant tables."""
        self.cursor.execute(self.ADD_MODE, (mode_name,))
        self.cursor.execute(self.ADD_LINE, (mode_name, line_name))
        if commit:
            self.connection.commit()


    def add_line(self, timestamp: datetime, mode_name: str, line_name: str,
                 statuses: list[str], commit: bool = True):
        self.cursor.execute(
            self.ADD_STATUS,
            (
                timestamp,
                mode_name,
                line_name,
                self.LINE_STATUS_DELIM.join(statuses),
            )
        )
        if commit:
            self.connection.commit()

    def lines_from_dict(self, timestamp: datetime, data: dict, commit: bool = True):
        """Add line statuses from a dict containing data for a particular line."""
        for line in data:
            self.add_mode_line(line['modeName'], line['name'], commit=False)
            self.add_line(timestamp, line['modeName'], line['name'],
                          [s['statusSeverityDescription'] for s in line['lineStatuses']], commit=False)
        if commit:
            self.connection.commit()

    def get_line_statuses(self,
                          from_date: Optional[datetime] = None,
                          to_date: Optional[datetime] = None,
                          mode_name: Optional[str] = None,
                          line_name: Optional[str] = None) -> pd.DataFrame:
        """Return a Pandas DataFrame containing the line statuses. Keyword arguments allow filtering of results."""
        where: list[str] = []
        params: list[Any] = []
        if from_date and to_date:
            where.append('datetime(timestamp) BETWEEN ? and ?')
            params += [from_date, to_date]
        elif from_date:
            where.append('date(date_time) >= ?')
            params.append(from_date)
        elif to_date:
            where.append('date(date_time) <= ?')
            params.append(to_date)
        if mode_name is not None:
            where.append('mode_name = ?')
            params.append(mode_name)
        if line_name is not None:
            where.append('line_name = ?')
            params.append(line_name)
        query = 'SELECT * FROM "line_statuses"'
        if where:
            query += ' WHERE ' + ' AND '.join(where)
        query += 'ORDER BY timestamp'
        self.cursor.execute(query, params)
        results = self.cursor.fetchall()
        return pd.DataFrame((self._parse_line_entries(*r) for r in results),
                            columns=['timestamp', 'mode_name', 'line_name', 'statuses'])

def lines_to_db(data_dir: str, db_fpath: str):
    """Parse line status information from JSON files and add to a database.

    :param data_dir: The top-level directory, containing the TFL data.
    :param db_fpath: Path to the SQLite file to use.

    """
    parser = DataParser(data_dir)
    dbm = DatabaseManager(db_fpath)
    for dt, d in parser.walk_category('lines'):
        if d is None:
            continue
        dbm.lines_from_dict(dt, d, commit=False)
        dbm.connection.commit()

if __name__ == '__main__':
    from sys import argv
    lines_to_db(argv[1], argv[2])
