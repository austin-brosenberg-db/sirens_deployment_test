import requests
import csv
import json
import re
import datetime

from .schema import raw_schema


def parse_abusech_csv(text, header_identifier='first_seen_utc'):
    '''
    This function parses the CSV files from abuse.ch.  They have a non standard format
    '''
    lines = text.splitlines()
    headers = [line for line in lines if line.startswith('#')]
    if len(headers) == 0:
        print('WARNING: No headers found in abuse.ch CSV file, CSV file may be empty')
        return []

    main_header = [line for line in headers if header_identifier in line][0].strip('# ')
    fieldnames = [f.strip('"') for f in main_header.split(',')]
    records = [line for line in lines if not line.startswith('#')]

    results = []
    for line in records:
        values = re.findall(r'"([^"]*)"', line)
        record = dict(zip(fieldnames, values))
        results.append(record)
    return results


class AbuseChThreatfoxCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        url = 'https://threatfox.abuse.ch/export/csv/recent/'
        results = []
        resp = requests.get(url)
        if resp.status_code == 200:
            for record in parse_abusech_csv(resp.text):
                results.append(
                    ('threatfox.abuse.ch', 'indicator', json.dumps(record), collection_time)
                )
        else:
            print(f'Error downloading from {url}: status_code={resp.status_code}')
        return self.spark.createDataFrame(results, schema=raw_schema)


class AbuseChUrlhausCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        url = 'https://urlhaus.abuse.ch/downloads/csv_recent/'
        results = []
        resp = requests.get(url)
        if resp.status_code == 200:
            for record in parse_abusech_csv(resp.text, 'id,dateadded,url'):
                results.append(
                    ('urlhaus.abuse.ch', 'indicator', json.dumps(record), collection_time)
                )
        else:
            print(f'Error downloading from {url}: status_code={resp.status_code}')
        return self.spark.createDataFrame(results, schema=raw_schema)


class AbuseChFeodotrackerCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        url = 'https://feodotracker.abuse.ch/downloads/ipblocklist.csv'
        results = []
        resp = requests.get(url)
        if resp.status_code == 200:
            reader = csv.DictReader([line for line in resp.text.splitlines() if not line.startswith('#')])
            for record in reader:
                results.append(
                    ('feodotracker.abuse.ch', 'indicator', json.dumps(record), collection_time)
                )
        else:
            print(f'Error downloading from {url}: status_code={resp.status_code}')
        return self.spark.createDataFrame(results, schema=raw_schema)


class AbuseChMalwareBazaarCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        results = []
        url = 'https://bazaar.abuse.ch/export/csv/recent/'
        resp = requests.get(url)
        if resp.status_code == 200:
            for record in parse_abusech_csv(resp.text):
                results.append(
                    ('bazaar.abuse.ch', 'indicator', json.dumps(record), collection_time)
                )
        else:
            print(f'Error downloading from {url}: status_code={resp.status_code}')
        return self.spark.createDataFrame(results, schema=raw_schema)


if __name__ == "__main__":
    import pyspark

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    collectors = {
        'threatfox': AbuseChThreatfoxCollector(spark),
        'urlhaus': AbuseChUrlhausCollector(spark),
        'feodotracker': AbuseChFeodotrackerCollector(spark),
        'malwarebazaar': AbuseChMalwareBazaarCollector(spark)
    }
    for name, collector in collectors.items():
        print(name)
        df = collector.collect()
        df.show(10, False)
        print('--\n')
