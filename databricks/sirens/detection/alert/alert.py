"""Detection Alert Module"""
from typing import Optional
from pyspark.sql import DataFrame
from abc import ABC, abstractmethod


class Alert(ABC):
    """Alert base class"""

    def __init__(self, module_name: str, detection_rules,
                 output_path: Optional[str] = None, output_table: Optional[str] = None):
        """
        @param: module_name = name of the detection module
        @param: detection_rules = rules for detection
        @param: output_path = alert table path
        """
        if output_table is not None and output_path is not None:
            raise Exception("Can't use both an output path and an output table at the same time.")

        self.rule_set = detection_rules.rules
        self.module_name = module_name
        self.output_path = output_path
        self.output_table = output_table

    @abstractmethod
    def make_alert(self, detected_df: DataFrame, key: str):
        raise NotImplementedError("Calling from abstract method.")

    def send_alert(self, alert: DataFrame, outputMode: str = "append",
                   checkpointLocation: Optional[str] = None, format: str = "delta",
                   processingTime: Optional[str] = None, continuous: Optional[str] = None,
                   partitionBy: Optional[list] = None):
        """Send an alert"""
        if alert.isStreaming:
            if checkpointLocation is None:
                checkpointLocation = f"{self.output_path}_checkpoint"

            alert = alert \
                .writeStream \
                .format(format) \
                .option("checkpointLocation", checkpointLocation)
            if partitionBy is not None:
                alert = alert.partitionBy(*partitionBy)
            if continuous is not None:
                alert = alert.trigger(continuous=continuous)
            if processingTime is not None:
                alert = alert.trigger(processingTime=processingTime)

            alert = alert.outputMode(outputMode)
            if self.output_table is not None:
                alert.toTable(self.output_table)
            else:
                alert.option("path", self.output_path).start()

        else:
            alert = alert.write.format(format).mode(outputMode)
            if partitionBy is not None:
                alert = alert.partitionBy(*partitionBy)

            if self.output_table is not None:
                alert.saveAsTable(self.output_table)
            else:
                alert.save(self.output_path)

    def update_name(self, module_name: str):
        """Update the module name if name is specified upon pipeline creation"""

        self.module_name = module_name
