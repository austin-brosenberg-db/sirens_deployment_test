import datetime
import json

import requests

from .schema import raw_schema


class PhishTankCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        resp = requests.get('http://data.phishtank.com/data/online-valid.json', timeout=300)
        results = []
        if resp.status_code == 200:
            for record in resp.json():
                results.append(
                    (f'PhishTank', 'indicator', json.dumps(record), collection_time)
                )
        else:
            print(f'Encounted status code: {resp.status_code}, response = {resp.text}')
        return self.spark.createDataFrame(results, schema=raw_schema)
