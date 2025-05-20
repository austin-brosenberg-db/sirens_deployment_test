import re
import json
from zipfile import ZipFile
from io import BytesIO
from pyspark.sql.functions import *
import datetime

import requests


class MispWarninglistsCollector():
    def __init__(self, spark):
        self.spark = spark

    def collect(self):
        collection_time = datetime.datetime.now()
        r = requests.get('https://github.com/MISP/misp-warninglists/archive/refs/heads/main.zip', timeout=300)
        r.raise_for_status()

        all_lists = []

        with ZipFile(BytesIO(r.content)) as zipfile:
            for name in zipfile.namelist():
                if re.match(r'(misp-warninglists-main/lists/.*\.json)', name) and not re.search(
                    r'(tranco|google-chrome-crux-1million)', name):
                    with zipfile.open(name) as inner:
                        rec = json.load(inner)
                        rec['list_name'] = name.split('/')[2]
                        all_lists.append(rec)

        orig_schema = '''
            description string,
            list array<string>,
            matching_attributes array<string>,
            name string,
            type string,
            version int,
            list_name string
        '''
        return (self.spark.createDataFrame(all_lists, schema=orig_schema)
                .withColumn('_source', lit('misp-warninglists'))
                .withColumn('_type', lit('benign-indicator'))
                .withColumn('_collection_ts', lit(collection_time))
                .withColumn('value', explode(col('list')))
                .drop(col('list'))
                .withColumn('_raw_record', expr('''
                to_json(named_struct(
                    'value', value,
                    'type', type,
                    'description', description,
                    'list_name', list_name,
                    'matching_attributes', matching_attributes,
                    'version', version
                ))
            '''))
                .select('_source', '_type', '_raw_record', '_collection_ts')
                )
