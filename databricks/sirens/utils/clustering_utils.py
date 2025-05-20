from pyspark.sql.functions import collect_list, struct, col, desc, row_number
from pyspark.sql.window import Window


class ClusteringUtils:
    @staticmethod
    def cluster_alerts(alerts, grouping_keys, schema_fields):
        alerts = alerts.select(*schema_fields)

        partitioned_alerts = (
            alerts
            .withColumn('key', col(grouping_keys[0]))
            .filter('key IS NOT NULL')
        )
        for x in grouping_keys[1:]:
            partitioned_alerts = (
                partitioned_alerts.union(
                    alerts
                    .withColumn('key', col(x))
                    .filter('key IS NOT NULL')
                )
            )
        partitioned_alerts = partitioned_alerts.withColumn('alert_key_occurrence', row_number().over(
            Window.partitionBy('key', 'name').orderBy(desc('eventTime')))).filter('alert_key_occurrence <= 50')

        alerts = partitioned_alerts.withColumn('detection', struct(*schema_fields)).select('key', 'detection')

        return (
            alerts
            .orderBy('key', 'eventTime')
            .groupBy('key')
            .agg(
                collect_list('detection').alias('alerts')
            )
        )
