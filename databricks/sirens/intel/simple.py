import requests
import json
import re
import datetime

from .schema import raw_schema


class SimpleCollector():
    _DEFAULT_IGNORE_PATTERN = r"^#.*"

    @classmethod
    def get_default_ignore_pattern(cls):
        return cls._DEFAULT_IGNORE_PATTERN

    @classmethod
    def set_default_ignore_pattern(cls, pattern):
        cls._DEFAULT_IGNORE_PATTERN = pattern

    def __init__(self, spark, url, source_name, ignore_pattern=None, user_agent=None, match_pattern=None, **kwargs):
        self.spark = spark
        self.url = url
        self.source_name = source_name
        self.ignore_pattern = ignore_pattern
        self.user_agent = user_agent
        self.match_pattern = match_pattern
        self.kwargs = kwargs

    def collect(self):
        collection_time = datetime.datetime.now()

        headers = {}
        if self.user_agent:
            headers = {'User-Agent': self.user_agent}
        resp = requests.get(self.url, headers=headers)
        resp.raise_for_status()
        results = []
        for line in resp.text.splitlines():
            indicator = line.strip()

            if indicator and (self.ignore_pattern is None or not re.match(self.ignore_pattern, indicator)):
                record = None

                if self.match_pattern is not None:
                    mat = re.match(self.match_pattern, indicator)
                    if mat:
                        record = mat.groupdict()
                        # allow for tag extraction via match_pattern
                        if 'tags' in record and isinstance(record['tags'], str):
                            record['tags'] = [record['tags']]

                        for k, v in self.kwargs.items():
                            if k not in record:
                                record[k] = v
                            elif k == 'tags':
                                record[k] = list(set(record[k] + v))
                else:
                    record = {
                        'indicator': indicator,
                        **self.kwargs
                    }
                if record is not None:
                    results.append(
                        (self.source_name, 'indicator', json.dumps(record), collection_time)
                    )
        return self.spark.createDataFrame(results, schema=raw_schema)


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    default_ignore_pattern = r"^#.*"
    feed = {
        "url": "https://raw.githubusercontent.com/infobloxopen/threat-intelligence/main/combined_malicious_indicators.csv",
        "source_name": "inflobloxopen/threat-intelligence",
        "type": "domain",
        "indicator_type": "mal_domain",
        "match_pattern": r'^"(?P<type>domain)","(?P<indicator>[^"]+)","(?P<tags>[^"]+)",".*$',
    }

    c = SimpleCollector(spark, **feed)
    df = c.collect()
    df.show(truncate=False)
