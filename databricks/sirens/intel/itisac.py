import json
from datetime import datetime, timedelta
from trustar import datetime_to_millis, log, TruStar

from .schema import raw_schema


class ItIsacCollector:
    def __init__(self, spark, user_api_key, user_api_secret):
        self.spark = spark
        self.user_api_key = user_api_key
        self.user_api_secret = user_api_secret

    def collect(self, lookback_hours=24):
        collection_time = datetime.now()
        config = {
            'user_api_key': self.user_api_key,
            'user_api_secret': self.user_api_secret,
        }
        ts = TruStar(config=config)

        to_time = datetime.now()
        from_time = to_time - timedelta(hours=lookback_hours)

        to_time = datetime_to_millis(to_time)
        from_time = datetime_to_millis(from_time)

        reports = ts.get_reports(from_time=from_time,
                                 to_time=to_time,
                                 is_enclave=True,
                                 enclave_ids=ts.enclave_ids)
        report_records = []
        indicator_records = []

        for report in reports:
            report_rec = report.to_dict()
            report_rec['tags'] = list([tag.name for tag in ts.get_enclave_tags(report.id)])
            report_records.append(report_rec)

            for indicator in ts.get_indicators_for_report(report.id):
                indicator_rec = indicator.to_dict()
                indicator_rec['report_id'] = report_rec['id']
                indicator_rec['report_title'] = report_rec['title']
                indicator_rec['tags'] = report_rec['tags']
                indicator_records.append(indicator_rec)

        rows = []
        for rec in report_records:
            rows.append(
                ('it-isac', 'report', json.dumps(rec), collection_time)
            )
        for rec in indicator_records:
            rows.append(
                ('it-isac', 'indicator', json.dumps(rec), collection_time)
            )
        return self.spark.createDataFrame(rows, schema=raw_schema)
