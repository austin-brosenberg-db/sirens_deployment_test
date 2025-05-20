import pytest, json
from databricks.sirens.parsers.internal.keyvalue_parser import GenericKVParser
#from pyspark.testing import assertDataFrameEqual


@pytest.mark.usefixtures("spark_session")
def test_kv_parser(spark_session):
    field_names = ["date", "time", "logid", "type", "subtype", "level"]
    kv_parser = GenericKVParser()
    
    line = 'date=2019-05-13 time=11:20:54 logid="0100032001" type="event" subtype="system" level="information" vd="vdom1" eventtime=1557771654587081441 logdesc="Admin login successful" sn="1557771654" user="admin" ui="ssh(172.16.200.254)" method="ssh" srcip=172.16.200.254 dstip=172.16.200.2 action="login" status="success" reason="none" profile="super_admin" msg="Administrator admin logged in successfully from ssh(172.16.200.254)"'
    df = spark_session.createDataFrame([(line,)], ["value"])
    kv_df = kv_parser.parse(df)
    assert kv_df.count() == 1
    # HITS: see SPARK-5063.
    #result = kv_df.collect()
    #assert result[0]["record"]["date"] == "2019-05-13"

    silver_df = kv_parser.expand_fields(kv_df, fieldNames=field_names)
    assert len(silver_df.columns) == len(field_names) + 1
    #result = silver_df.collect()
    #assert result[0]['subtype'] == "system"
    #assert result[0]['level'] == "information"
    #assert result[0]['type'] == "event"
    #assert result[0]['logid'] == "0100032001"
