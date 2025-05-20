"""DBDF Pipeline class"""

from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp
from databricks.sirens.detection.detector import Detector


class Pipeline:
    """DBDF Pipeline class
    @param: module_name = pipeline name
    @param: df = ingest dataframe
    @param: spark = SparkSession for detector operations
    @param: outputMode = mode to output data
    @param: checkpointLocation = optional checkpoint location if output fails before completing
    @param: processingTime = optional trigger argument for outputting data
    @param: continuous = optional trigger argument for outputting data
    @param: format = format to output data
    @param: detector = optional detector object
    """

    def __init__(self, module_name: str, df: DataFrame, spark: SparkSession, output_path: Optional[str] = None,
                 heartbeat_table_path: Optional[str] = None, ingestTime: str = "ingestTime",
                 outputMode: str = "append", checkpointLocation: Optional[str] = None,
                 processingTime: Optional[str] = None, continuous: Optional[str] = None, format: str = "delta",
                 detector: Optional[Detector] = None):

        if outputMode != "append" and outputMode != "complete" and outputMode != "update":
            raise Exception("Output mode must be 'append', 'complete', or 'update'.")

        self.module_name = module_name
        self.output_path = output_path
        self.heartbeat_table_path = heartbeat_table_path
        self.ingestTime = ingestTime
        self.df = df
        self.spark = spark
        self.outputMode = outputMode
        self.checkpointLocation = checkpointLocation
        self.format = format
        self.detectors = []

        if continuous is not None and processingTime is not None:
            raise Exception("Can't use continuous and processing time triggers at the same time.")
        self.continuous = continuous
        self.processingTime = processingTime

        if detector:
            """update detector name if its left unspecified"""
            detector.check_conf(self)
            self.detectors = [detector]

    def add_detector(self, detector: Detector) -> None:
        """update detector name if its left unspecified"""

        if detector:
            detector.check_conf(self)
            self.detectors.append(detector)

    def write_sink(self, df):
        if df.isStreaming:
            if self.checkpointLocation is None:
                self.checkpointLocation = f"{self.output_path}_checkpoint"

            df = df \
                .writeStream \
                .format(self.format) \
                .option("checkpointLocation", self.checkpointLocation)
            if self.continuous is not None:
                df = df.trigger(continuous=self.continuous)
            if self.processingTime is not None:
                df = df.trigger(processingTime=self.processingTime)
            return (
                df
                    .outputMode(self.outputMode)
                    .option("path", self.output_path)
                    .start()
            )
        else:
            df.write.format(self.format).mode(self.outputMode).save(self.output_path)

    def update_heartbeat(self, cur_batch:DataFrame, ingestTime:str, module_name:str, heartbeat_table_path:str):
        from delta.tables import DeltaTable

        if DeltaTable.isDeltaTable(self.spark, heartbeat_table_path) is True:
            deltaTable = DeltaTable.forPath(self.spark, heartbeat_table_path)
            grouped = cur_batch.withColumn("moduleName",lit(module_name)).groupBy("moduleName").count()
            return (
                    deltaTable.alias("base")
                             .merge(grouped.alias("batch"), f"base.moduleName = '{module_name}'")
                             .whenMatchedUpdate(set = {ingestTime: current_timestamp(),"batchSize":"batch.count"})
                             .whenNotMatchedInsert(values =
                                 {"moduleName":lit(module_name),ingestTime:current_timestamp(),"batchSize":"batch.count"})
                             .execute()
            )

    def batch_proc(self, cur_batch: DataFrame, _):
        """streaming batch proc"""
        if self.output_path is not None:
            self.write_sink(cur_batch)

        for detector in self.detectors:
            alert_df = detector.detect(cur_batch)
            detector.send_alert(alert_df, self.outputMode, self.checkpointLocation, self.format,
                                self.processingTime, self.continuous)
        if self.heartbeat_table_path:
            self.update_heartbeat(cur_batch, self.ingestTime, self.module_name, self.heartbeat_table_path)

    def start_streaming(self):
        """streaming starting function"""

        df = self.df

        if df.isStreaming:
            return df.writeStream.foreachBatch(self.batch_proc).start()
        else:
            return self.batch_proc(df, 0)

    def start(self):
        self.start_streaming()
