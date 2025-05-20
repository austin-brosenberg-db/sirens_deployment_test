from dataclasses import dataclass
from functools import reduce
from typing import Dict, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, from_json, when, lit, struct, to_json, udf
from pyspark.sql.types import BooleanType

from databricks.sirens.detection.pipeline import *
from databricks.sirens.detection.alert import *
from databricks.sirens.detection.rule.detection_ruleset import DetectionRuleset
from databricks.sirens.detection.rule.rule_entity import *
from databricks.sirens.exceptions import SirensDetectionException
from databricks.sirens.utils.base_utils import BaseUtils


@dataclass
class Detector:
    """ 
    @param: module_name = name of the detection module
    @param: detection_rules = rules for detection
    @param: output_path = alert table path
    @param: output_table = alert table name
    @param: partitionBy = columns to partition by in alert write/writeStream
    """

    def __init__(self, detection_rules: DetectionRuleset, partitionBy: Optional[list] = None,
                 output_path: Optional[str] = None, output_table: Optional[str] = None,
                 dedup_on: Optional[Union[list, dict]] = None, module_name: str = "",
                 df: Optional[DataFrame] = None, spark: Optional[SparkSession] = None):
        if output_table is not None and output_path is not None:
            raise Exception("Can't use both an output path and an output table at the same time.")
        self.df = df
        self.spark = spark
        self.dedup_on = dedup_on
        if self.dedup_on is not None:
            if isinstance(self.dedup_on, list):
                self.dedup_on = {'columns': self.dedup_on}
            elif isinstance(self.dedup_on, dict):
                if 'columns' not in self.dedup_on.keys():
                    raise Exception("columns must be in the dedup_on keys")
                for key in self.dedup_on.keys():
                    if key not in ['columns', 'df', 'method']:
                        raise Exception(f"""Unrecognized dedup_on input: {key}.
                        Possible inputs are: columns, df, method""")

            if 'df' in self.dedup_on.keys():
                if not BaseUtils.is_spark_dataframe(self.dedup_on['df']):
                    raise Exception(f'dedup_on df must be of type Dataframe. Type given: {type(self.dedup_on["df"])}')
                self.dedup_on['df'] = self.dedup_on['df']

            self.dedup_on['method'] = self.dedup_on.get('method', 'left_anti')

        self.detection_rules: Dict = detection_rules.rules
        self.partitionBy = partitionBy
        self.module_name = module_name
        self.output_table = output_table
        self.output_path = output_path
        self.alert = SirensAlert(module_name, detection_rules, output_path, output_table)

    def check_conf(self, pipeline: 'Pipeline') -> None:
        if not self.module_name:
            self.update_name(pipeline.module_name)
        if not self.spark:
            self.spark = pipeline.spark

    def _correlation_select(self, df, rule):
        if 'eventTime' in rule._schema_fields:
            event_times = expr(
                '''
                ARRAY_REMOVE(
                    AGGREGATE(
                        alerts, 
                        ARRAY(TO_TIMESTAMP("1996-09-07")), (acc, x) -> ARRAY_UNION(acc, ARRAY(COALESCE(x.eventTime, TO_TIMESTAMP("1996-09-07"))))
                    ), TO_TIMESTAMP("1996-09-07")
                )
                '''
            )
        else:
            event_times = lit(None)

        if 'uuid' in rule._schema_fields:
            uuids = expr(
                '''
                ARRAY_REMOVE(
                    AGGREGATE(
                        alerts, 
                        ARRAY(''), (acc, x) -> ARRAY_UNION(acc, ARRAY(COALESCE(x.uuid, '')))
                    ), ''
                )
                '''
            )
        else:
            uuids = lit(None)

        if any(['observables' in x for x in rule._schema_fields]):
            observables = struct(
                *[
                    expr(
                        f'''
                        ARRAY_REMOVE(
                            AGGREGATE(
                                alerts, 
                                ARRAY(''), (acc, x) -> ARRAY_UNION(acc, COALESCE(x.observables.{x}, ARRAY('')))
                            ), ''
                        )
                        '''
                    ).alias(x)
                    for x in ['ipAddresses', 'domains', 'fileHashes', 'filePaths', 'urls', 'processes']
                ]
            )
        else:
            observables = lit(None)

        df = (
            df
            .select(
                'key',
                expr(f'FILTER(alerts, x -> {rule.pyspark_fields["window"]})').alias(
                    'alerts'),
                rule.sequences.alias('sequences'),
                lit(rule.pyspark_fields["window"]).alias('window'),
                lit(rule.filter).alias('filter')
            )
        )

        to_select = [
            'key',
            'alerts',
            expr('ARRAY_MIN(eventTimes)').alias('__first_seen'),
            expr('ARRAY_MAX(eventTimes)').alias('__last_seen'),
            uuids.alias('__uuids'),
            observables.alias('__observables'),
            'window',
            'filter'
        ]

        if rule.collect_fields:
            for key, val in rule.collect_fields.items():
                to_select.append(expr(val).alias(key))

        return (
            df
            .filter(rule.filter)
            .withColumn(
                'eventTimes',
                event_times
            )
            .select(*to_select)
        )

    def detect(self, df: Optional[DataFrame] = None):
        if len(self.detection_rules.keys()) == 0:
            raise SirensDetectionException("No detection rules specified.")

        if self.df is not None:
            df = self.df

        correlation_alerts = []
        is_correlation = None
        new_columns = []
        for idx, rule_id in enumerate(self.detection_rules):
            rule = self.detection_rules[rule_id]

            if rule.original_fields['alertClass'] == 'CORRELATION':
                if is_correlation is False:
                    raise Exception("Cannot mix correlation rules with other rules in the same pipeline.")
                is_correlation = True
            else:
                if is_correlation is True:
                    raise Exception("Cannot mix correlation rules with other rules in the same pipeline.")
                is_correlation = False

            if is_correlation is False:
                new_columns.append(
                    when(
                        expr(rule.filter),
                        to_json(
                            self.alert.make_alert(df, rule_id, is_struct=True)
                        )
                    ).alias(f'rule_{idx}')
                )
            else:
                self.spark.udf.register("sequential_filtering", RuleEntity.sequential_filtering)

                alert = self._correlation_select(df, rule)
                alert = self.alert.make_alert(alert, rule_id, is_struct=False)
                correlation_alerts.append(alert)

        if is_correlation is False:
            rules_strings = ",".join(map(lambda x: f"rule_{x}", range(len(self.detection_rules))))

            if len(new_columns) != 0:
                df = df.select('*', *new_columns)
                df = (
                    df
                    .select(
                        expr(f'stack({len(self.detection_rules.keys())}, {rules_strings}) AS alerts')
                    )
                    .filter('alerts IS NOT NULL')
                    .select(from_json('alerts', self.alert.schema).alias('alerts'))
                    .select('alerts.*')
                )
        else:
            df = reduce(DataFrame.unionAll, correlation_alerts)

        if self.dedup_on is not None:
            if 'df' not in self.dedup_on.keys():
                if self.output_table is not None:
                    dedup_table = self.spark.table(self.output_table)
                else:
                    dedup_table = self.spark.read.load(self.output_path)
                self.dedup_on['df'] = dedup_table

            df = (
                df
                .dropDuplicates(self.dedup_on['columns'])
                .join(self.dedup_on['df'], self.dedup_on['columns'], self.dedup_on['method'])
            )

        return df

    def send_alert(self, alert: DataFrame, outputMode: str = "append",
                   checkpointLocation: Optional[str] = None, format: str = "delta",
                   processingTime: Optional[str] = None, continuous: Optional[str] = None):
        """call alert object to send the alert
        @param: alert_df: the alert dataframe will be updated to the configured alert table"""

        self.alert.send_alert(alert, outputMode, checkpointLocation, format, processingTime, continuous,
                              self.partitionBy)

    def update_name(self, module_name: str):
        """Update the module name if name is specified upon pipeline creation"""

        self.module_name = module_name
        self.alert.update_name(module_name)
