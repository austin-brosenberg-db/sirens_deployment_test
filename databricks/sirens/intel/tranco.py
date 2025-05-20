import csv
import datetime
import json
from io import BytesIO
from zipfile import ZipFile

import requests

from .schema import raw_schema


class TrancoCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        r = requests.get('https://tranco-list.eu/top-1m.csv.zip', timeout=300)
        r.raise_for_status()

        results = []
        with ZipFile(BytesIO(r.content)) as zipfile:
            for name in zipfile.namelist():
                if name == 'top-1m.csv':
                    with zipfile.open(name, 'r') as inner:
                        lines = [line.decode('utf-8') for line in inner]
                        for record in csv.DictReader(lines, fieldnames=['rank', 'domain']):
                            results.append(
                                (f'tranco', 'indicator', json.dumps(record), collection_time)
                            )

        return self.spark.createDataFrame(results, schema=raw_schema)
