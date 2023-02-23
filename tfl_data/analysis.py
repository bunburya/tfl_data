from typing import Collection, Union

import pandas as pd

from tfl_data.db import DatabaseManager

PLANNED_CLOSED_STATUSES = {'Part Closure', 'Planned Closure'}
SUSPENDED_STATUSES = {'Suspended', 'Part Suspended'}
DELAY_STATUSES = {'Minor Delays', 'Severe Delays'}

def has_status(row: pd.Series, status: str) -> bool:
    return status in row['statuses']

def has_any_status(row: pd.Series, statuses: Collection[str]) -> bool:
    """Check if the given row contains any of the given statuses."""
    return bool(row['statuses'] & set(statuses))

def has_all_statuses(row: pd.Series, statuses: Collection[str]) -> bool:
    """Check if the given row contains all of the given statuses."""
    return set(statuses).issubset(row)

def summarize_tube_line(dbm: DatabaseManager, line: str) -> dict[str, Union[str, int]]:
    print(f'summarising line: {line}')
    # Get DataFrame
    df = dbm.get_line_statuses(mode_name='tube', line_name=line)
    # Drop rows outside of regular service hours
    df = df[~df.apply(lambda r: has_status(r, 'Service Closed'), axis='columns')]

    return {
        'line': line,
        'total_count': df.shape[0],
        'delayed_count': df.apply(lambda r: has_any_status(r, DELAY_STATUSES), axis='columns').sum(),
        'part_suspended_count': df.apply(lambda r: has_status(r, 'Part Suspended'), axis='columns').sum(),
        'full_suspended_count': df.apply(lambda r: has_status(r, 'Suspended'), axis='columns').sum(),
        'suspended_count': df.apply(lambda r: has_any_status(r, SUSPENDED_STATUSES), axis='columns').sum(),
        'part_closed_count': df.apply(lambda r: has_status(r, 'Part Closure'), axis='columns').sum(),
        'planned_closed_count': df.apply(lambda r: has_status(r, 'Planned Closure'), axis='columns').sum(),
        'closed_count': df.apply(lambda r: has_any_status(r, PLANNED_CLOSED_STATUSES), axis='columns').sum()

    }

def get_tube_summary(dbm: DatabaseManager) -> pd.DataFrame:
    """Return a DataFrame summarising the line status statistics for each tube line."""


    data = [summarize_tube_line(dbm, line) for line in dbm.get_line_names('tube')]
    column_names = list(data[0])
    return pd.DataFrame(data, columns=column_names)


def main():
    from sys import argv
    dbm = DatabaseManager(argv[1])
    print(get_tube_summary(dbm))

if __name__ == '__main__':
    main()
