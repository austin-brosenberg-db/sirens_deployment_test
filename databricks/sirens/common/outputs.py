from databricks.sirens.common.entities import OutputFormat, AnalysisItem
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.logging import get_logger
from pyspark.sql import DataFrame, SparkSession
from typing import List, Dict
from dataclasses import asdict
import pyspark.sql.functions as F
import json
logger = get_logger(__name__)

spark = SparkSession.getActiveSession()
def _get_ipython__display():
    try:
        from IPython.display import HTML
        return HTML
    except ImportError:
        return None

HTML = _get_ipython__display()

def sirens_analytics():
    """Simple wrapper function to convert analysis commands into the specified output format 
       defaults to DataFrame
    """
    def wrapper(func):
     
        def wrapped(*args, **kwargs):
            output_format = kwargs.get("output_format", OutputFormat.asDataFrame)

            result = func(*args, **kwargs)
            
            return AnalysisOutput.get_outputs(result, output_format)
            
        return wrapped
    return wrapper

class AnalysisOutput:
    """Return analysis/command results in one of the specified formats.
    """
    @staticmethod
    def get_outputs(result, format):
        if 'py_dataclass' in format.value:
            return AnalysisOutput.asObject(result)
        if 'dataframe' in format.value:
            return AnalysisOutput.asDataFrame(result)
        if 'json' in format.value:
            return AnalysisOutput.asJson(result)
        if 'html' in format.value:
            return AnalysisOutput.asHTML(result)
    
    @staticmethod
    def _dataframe_as_dict(data: DataFrame) -> List[Dict]:
        data = data.select(*[F.col(x).cast("string") for x in data.columns])
        return [row.asDict() for row in data.collect()]
        #return list(map(lambda row: row.asDict(), data.collect()))
    
    @staticmethod
    def _as_dict(data: list):
        data_as_dict = []
        [data_as_dict.append(asdict(line)) for line in data]
        return data_as_dict

    @staticmethod
    def asObject(result):
        if BaseUtils.is_spark_dataframe(result):
            items = []
            r = AnalysisOutput._dataframe_as_dict(result)
            [items.append(AnalysisItem(entry)) for entry in r]
            return items
        else:
            return result

    @staticmethod
    def asDataFrame(result):
        if BaseUtils.is_spark_dataframe(result):
            return result
        if isinstance(result, list):
            return spark.createDataFrame(AnalysisOutput._as_dict(result), result[0].schema)
        return result
    
    @staticmethod
    def asJson(result):
        if BaseUtils.is_spark_dataframe(result):
            new = AnalysisOutput._dataframe_as_dict(result)
            print(new)
            return json.dumps(new)
        if isinstance(result, list):
            return json.dumps(AnalysisOutput._as_dict(result))
        return json.dumps(result)
    
    @staticmethod
    def asHTML(result):
        if not HTML:
            logger.warning("No ipython.display HTML available")

        if BaseUtils.is_spark_dataframe(result):
            return HTML(result.toPandas().to_html(index=False,col_space="40px", classes=('table', 'table-striped')))
        return result
