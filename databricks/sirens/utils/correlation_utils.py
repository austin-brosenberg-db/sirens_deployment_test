from pyspark.sql.window import Window
from pyspark.sql.functions import col, expr, from_json, struct, lead
from pyspark.sql.types import StructType, ArrayType, StringType, StructField

from databricks.sirens.detection.alert import AlertSchema


class CorrelationUtils:
    @staticmethod
    def parse_alerts(alerts):
        schema = StructType([
            StructField('key', StringType()),
            StructField('alerts', ArrayType(AlertSchema))
        ])
        return (
            alerts
            .withColumn('parsed_json', from_json('rawJson', schema))
            .select('*', 'parsed_json.*')
            .drop('parsed_json')
        )

    @staticmethod
    def get_clustering_metadata(alerts, scoring_table_name, clustering_table_name, spark):
        scoring_history = (
            spark.sql(f'DESCRIBE HISTORY {scoring_table_name}')
            .select(
                struct(
                    col('timestamp').alias('start'),
                    lead('timestamp', 1).over(Window.partitionBy('userId').orderBy('timestamp')).alias('end')
                ).alias('time_range')
            )
        )

        clustering_history = (
            spark.sql(f'DESCRIBE HISTORY {clustering_table_name}')
            .withColumn(
                'time_range',
                struct(
                    col('timestamp').alias('start'),
                    lead('timestamp', 1).over(Window.partitionBy('userId').orderBy('timestamp')).alias('end')
                )
            )
        )

        return (
            alerts
            .join(
                scoring_history,
                expr('(alertedTime BETWEEN time_range.start AND time_range.end) OR (alertedTime >= time_range.start AND time_range.end IS NULL)'),
                'left'
            )
            .withColumn('scoring_starting_timestamp', col('time_range.start'))
            .drop('time_range')
            .join(
                clustering_history,
                expr('(scoring_starting_timestamp BETWEEN time_range.start AND time_range.end) OR (scoring_starting_timestamp >= time_range.start AND time_range.end IS NULL)'),
                'left'
            )
        )
