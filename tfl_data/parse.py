import json
import logging
import os
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Generator


def listdir(*args, **kwargs) -> list[str]:
    """Wrapper around os.listdir which sorts the results."""
    return sorted(os.listdir(*args, **kwargs))

class DataParser:

    CATEGORIES = ('air_quality', 'bikes', 'charge_connectors', 'lines')

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def extract_data(self, fpath: str) -> Optional[dict]:
        """Extract and parse JSON data from gzip-compressed tar file.

        :param fpath: Path to tar file.
        :return: A dict containing the data from the file.
        """
        logging.info(f'Extracting data from {fpath}')
        fname = os.path.basename(fpath)
        json_fname = fname.rstrip('.tar.gz')
        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(fpath) as tf:
                tf.extractall(tmpdir)
            extracted = os.path.join(tmpdir, json_fname)
            if os.path.getsize(extracted) == 0:
                return None
            with open(os.path.join(tmpdir, json_fname)) as jf:
                return json.load(jf)


    def walk_category(self, category: str) -> Generator[dict, None, None]:
        """Walk through all files in a particular data category, yielding for each file a tuple
        of a datetime object and a dict containing the data for that date and time.

        :param category: The category of data we want to inspect.
        """
        root_dir = os.path.join(self.data_dir, category)
        for year in listdir(root_dir):
            year_dir = os.path.join(root_dir, year)
            for month in listdir(year_dir):
                month_dir = os.path.join(year_dir, month)
                for day in listdir(month_dir):
                    day_dir = os.path.join(month_dir, day)
                    for fname in listdir(day_dir):
                        fpath = os.path.join(day_dir, fname)
                        hour = fname[11:13]
                        minute = fname[14:16]
                        dt = datetime(int(year), int(month), int(day), int(hour), int(minute))
                        data = self.extract_data(fpath)
                        yield dt, data
