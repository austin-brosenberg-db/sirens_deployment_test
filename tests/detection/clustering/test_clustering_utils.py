from datetime import datetime

from pyspark.sql.functions import size
import json
from tests.utils import *
from databricks.sirens.utils.clustering_utils import *


@pytest.mark.usefixtures("spark_session")
def _alerts(spark_session):
    def parse_date(date):
        if date not in x:
            return None
        year, month, day = x[date].split('-')
        return datetime(int(year), int(month), int(day))

    def parse_timestamp(timestamp):
        if timestamp not in x:
            return None
        date, time = x[timestamp].split(' ')
        year, month, day = date.split('-')
        hour, minute, second = time.split(':')
        return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))

    data = json.load(open('tests/detection/clustering/alerts.json'))
    for x in data:
        x['eventTime'] = parse_timestamp('eventTime')
        x['alertedTime'] = parse_timestamp('alertedTime')
        x['rawTime'] = parse_timestamp('rawTime')
        x['eventDate'] = parse_date('eventDate')

    return spark_session.createDataFrame(data=data, schema=AlertSchema)


@pytest.mark.usefixtures("spark_session")
def test_cluster_alerts(spark_session):
    fields = [
        'name',
        'summary',
        'source',
        'eventTime',
        'actor',
        'target',
        'attacks',
        'severity',
        'alertClass',
        'observables',
        'uuid'
    ]
    alerts = _alerts(spark_session)
    alerts = ClusteringUtils.cluster_alerts(alerts, ['actor.id', 'target.id'], fields)

    assert (
               alerts
               .filter('key = "-2075130288@example.com"')
               .select('key', size('alerts').alias('total_alerts'))
               .collect()[0].total_alerts
           ) == 1
    assert (
               alerts
               .filter('key = "-2140846185@gmail.com"')
               .select('key', size('alerts').alias('total_alerts'))
               .collect()[0].total_alerts
           ) == 9
    assert alerts.count() == 7


@pytest.mark.usefixtures("spark_session")
def test_cluster_alerts_limit(spark_session):
    fields = [
        'name',
        'summary',
        'source',
        'eventTime',
        'actor',
        'target',
        'attacks',
        'severity',
        'alertClass',
        'observables',
        'uuid'
    ]
    alerts = _alerts(spark_session)

    # create a duplicated alert with specific "actor.id" or "target.id" combination
    isolated_alert = alerts.limit(1)

    large_alerts = alerts.limit(1)
    # repeat the duplicated alert to create more than 500 of them
    for _ in range(51):
        large_alerts = large_alerts.union(isolated_alert)

    assert large_alerts.count() == 52

    # run the large_alerts through the cluster_alerts function
    clustered_alerts = ClusteringUtils.cluster_alerts(large_alerts, ['actor.id', 'target.id'], fields)

    # assert the amount of "detection" is no more than 500
    assert clustered_alerts.count() <= 50
