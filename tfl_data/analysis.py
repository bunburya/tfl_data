from datetime import datetime
from typing import Union

import pandas as pd

from tfl_data.db import DatabaseReader, get_sqlite_reader

PLANNED_CLOSED_STATUSES = {'Part Closure', 'Planned Closure'}
SUSPENDED_STATUSES = {'Suspended', 'Part Suspended'}
DELAYED_STATUSES = {'Minor Delays', 'Severe Delays'}
DISRUPTED_STATUSES = SUSPENDED_STATUSES | DELAYED_STATUSES

def summarize_tube_line(dr: DatabaseReader, line: str) -> dict[str, Union[str, int]]:

    start = datetime.now()
    mode = "tube"
    data = {
        'line': line,
        'total_count': dr.count_observations(modes={mode}, lines={line}),
        'unplanned_disruption_count': dr.count_observations(modes={mode}, lines={line}, statuses=DISRUPTED_STATUSES),
        'planned_disruption_count': dr.count_observations(modes={mode}, lines={line}, statuses=PLANNED_CLOSED_STATUSES)
    }
    end = datetime.now()
    duration = end - start
    print(f"Summarised {mode}/{line} in {duration}")
    return data


def get_tube_summary(dr: DatabaseReader) -> pd.DataFrame:
    """Return a DataFrame summarising the line status statistics for each tube line."""

    with dr:
        data = [summarize_tube_line(dr, line) for line in dr.get_lines('tube')]
    column_names = list(data[0])
    df = pd.DataFrame(data, columns=column_names)
    df['unplanned_disruption_pct'] = 100 * (df['unplanned_disruption_count'] / df['total_count'])
    df['planned_disruption_pct'] = 100 * (df['planned_disruption_count'] / df['total_count'])
    return df


def main():
    from sys import argv
    dr = get_sqlite_reader(argv[1])
    print(get_tube_summary(dr))


if __name__ == '__main__':
    main()
