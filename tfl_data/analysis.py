from typing import Collection

import pandas as pd

from tfl_data.db import DatabaseManager

CLOSED_STATUSES = {'Service Closed', 'Planned Closure'}
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

def summarize_tube_line(dbm: DatabaseManager, line: str):
    # Get DataFrame
    df = dbm.get_line_statuses(mode_name='tube', line_name=line)
    # Drop rows outside of regular service hours
    df = df[~df.apply(lambda r: has_status(r, 'Service Closed'), axis='columns')]

    total_count = df.shape[0]
    print(f'Total: {total_count}')

    delayed_count = df.apply(lambda r: has_any_status(r, DELAY_STATUSES), axis='columns').sum()
    print(f'Delays: {delayed_count} ({(delayed_count / total_count) * 100}%)')

    suspended_count = df.apply(lambda r: has_any_status(r, SUSPENDED_STATUSES), axis='columns').sum()
    print(f'Suspended: {suspended_count} ({(suspended_count / total_count) * 100}%)')

if __name__ == '__main__':
    main()
