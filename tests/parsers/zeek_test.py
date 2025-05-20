import json
import os

from databricks.sirens.datasource import DataSource
from databricks.sirens.parsers import zeek
from databricks.sirens import normalize

def test_http_json(spark_session, mocker):
    file = f"{os.curdir}/tests/samples/parsers/zeek/http.log"

    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="zeek", sourcetype="http_json")
    config = dataSourceObj.read()

    df = spark_session.read.json(file)
    parser = zeek.Parse(spark_session)
    bronze_df = parser.toBronze(df, dataSourceObj)
    cols = bronze_df.columns
    event_dict = json.loads(bronze_df.limit(1).toJSON().first())

    expected_cols = ['_event_date', '_event_time', '_ingest_time', '_source', '_source_file', '_sourcetype',
                     'dvc_hostname', 'host', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p',
                     'method', 'request_body_len']
    assert all(col in cols for col in expected_cols)

    silver_df = parser.toSilver(bronze_df, dataSourceObj)
    cols = silver_df.columns

    expected_cols = ['_event_date', '_event_time', '_ingest_time', '_source', '_source_file', '_sourcetype',
                     'dvc_hostname', 'host', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p', 'method',
                     'request_body_len', 'resp_fuids', 'resp_mime_types', 'response_body_len', 'status_code',
                     'status_msg', 'tags', 'trans_depth', 'ts', 'uid', 'uri', 'version']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(silver_df.limit(1).toJSON().first())
    assert event_dict['_event_date'] == '2022-10-17'

    normalizer = normalize.Normalizer(spark_session, dataSourceObj)

    table_tranformations = config.get("transforms").get("silver").get("event_type")
    target_table = table_tranformations[0]['target_table']   # web
    event_filtered_df = normalizer.filter_frame(df=silver_df, target_table=target_table)
    cim_df = normalizer.transform_frame(df=event_filtered_df, target_table=target_table)
    cols = cim_df.columns

    expected_cols = ['_event_date', '_event_time', '_source', '_sourcetype', 'dest', 'dest_host', 'dest_ip', 'dest_port',
                     'dvc_hostname', 'event_message', 'event_result', 'event_schema_file', 'event_severity', 'event_type',
                     'http_content_type', 'http_request_length', 'http_request_method', 'http_response_length', 'http_status_code',
                     'http_version', 'src', 'src_ip', 'src_port', 'uid', 'url']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(cim_df.limit(1).toJSON().first())
    assert event_dict['src'] == '10.10.17.101'
    assert event_dict['dest'] == 'axilapodiumz.com'

def test_dns_json(spark_session, mocker):
    file = f"{os.curdir}/tests/samples/parsers/zeek/dns.log"

    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="zeek", sourcetype="dns_json")
    config = dataSourceObj.read()

    df = spark_session.read.json(file)
    parser = zeek.Parse(spark_session)
    bronze_df = parser.toBronze(df, dataSourceObj)
    cols = bronze_df.columns
    event_dict = json.loads(bronze_df.limit(1).toJSON().first())

    expected_cols = ['AA', 'RA', 'RD', 'TC', 'TTLs', 'Z', '_event_date', '_event_time', '_ingest_time',
                     '_source', '_source_file', '_sourcetype', 'answers', 'dvc_hostname', 'id_orig_h',
                     'id_orig_p', 'id_resp_h', 'id_resp_p', 'proto', 'qclass', 'qclass_name', 'qtype',
                     'qtype_name', 'query', 'rcode', 'rcode_name', 'rejected', 'rtt', 'trans_id', 'ts', 'uid']
    assert all(col in cols for col in expected_cols)

    silver_df = parser.toSilver(bronze_df, dataSourceObj)
    cols = silver_df.columns

    expected_cols = ['AA', 'RA', 'RD', 'TC', 'TTLs', 'Z', '_event_date', '_event_time', '_ingest_time', '_source',
                     '_source_file', '_sourcetype', 'answers', 'dvc_hostname', 'id_orig_h', 'id_orig_p', 'id_resp_h',
                     'id_resp_p', 'proto', 'qclass', 'qclass_name', 'qtype', 'qtype_name', 'query', 'rcode',
                     'rcode_name', 'rejected', 'rtt', 'trans_id', 'ts', 'uid']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(silver_df.limit(1).toJSON().first())
    assert event_dict['_event_date'] == '2022-10-17'

    normalizer = normalize.Normalizer(spark_session, dataSourceObj)

    table_tranformations = config.get("transforms").get("silver").get("event_type")
    target_table = table_tranformations[0]['target_table']   # dns
    event_filtered_df = normalizer.filter_frame(df=silver_df, target_table=target_table)
    cim_df = normalizer.transform_frame(df=event_filtered_df, target_table=target_table)
    cols = cim_df.columns

    expected_cols = ['_event_date', '_event_time', '_source', '_sourcetype', 'dest', 'dest_ip_addr', 'dest_port',
                     'dns_query', 'dns_query_type', 'dns_query_type_name', 'dns_response_answers',
                     'dns_response_code', 'dns_response_name', 'dvc_hostname', 'event_message', 'event_result',
                     'event_schema_file', 'event_severity', 'event_type', 'network_protocol', 'src', 'src_ip_addr',
                     'src_port', 'uid']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(cim_df.limit(1).toJSON().first())
    assert event_dict['src'] == '10.10.17.101'
    assert event_dict['dest'] == '10.10.17.1'

def test_files_json(spark_session, mocker):
    file = f"{os.curdir}/tests/samples/parsers/zeek/files.log"

    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="zeek", sourcetype="files_json")
    config = dataSourceObj.read()

    df = spark_session.read.json(file)
    parser = zeek.Parse(spark_session)
    bronze_df = parser.toBronze(df, dataSourceObj)
    cols = bronze_df.columns
    event_dict = json.loads(bronze_df.limit(1).toJSON().first())

    expected_cols = ['_event_date', '_event_time', '_ingest_time', '_source', '_source_file', '_sourcetype',
                     'analyzers', 'conn_uids', 'depth', 'duration', 'dvc_hostname', 'fuid', 'is_orig', 'mime_type',
                     'missing_bytes', 'overflow_bytes', 'rx_hosts', 'seen_bytes', 'source', 'timedout',
                     'total_bytes', 'ts', 'tx_hosts']
    assert all(col in cols for col in expected_cols)

    silver_df = parser.toSilver(bronze_df, dataSourceObj)
    cols = silver_df.columns

    expected_cols = ['_event_date', '_event_time', '_ingest_time', '_source', '_source_file', '_sourcetype',
                     'analyzers', 'conn_uids', 'depth', 'duration', 'dvc_hostname', 'fuid', 'is_orig', 'mime_type',
                     'missing_bytes', 'overflow_bytes', 'rx_hosts', 'seen_bytes', 'source', 'timedout', 'total_bytes',
                     'ts', 'tx_hosts']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(silver_df.limit(1).toJSON().first())
    assert event_dict['_event_date'] == '2022-10-17'

    normalizer = normalize.Normalizer(spark_session, dataSourceObj)

    table_tranformations = config.get("transforms").get("silver").get("event_type")
    target_table = table_tranformations[0]['target_table']   # file
    event_filtered_df = normalizer.filter_frame(df=silver_df, target_table=target_table)
    cim_df = normalizer.transform_frame(df=event_filtered_df, target_table=target_table)
    cols = cim_df.columns

    expected_cols = ['_event_date', '_event_time', '_source', '_sourcetype', 'dest', 'dest_hostname', 'dvc_hostname',
                     'event_message', 'event_result', 'event_schema_file', 'event_type', 'file_identifier', 'file_source',
                     'src', 'src_file_mime_type', 'src_hostname', 'target_file_hash_md5', 'target_file_hash_sha1',
                     'target_file_hash_sha256', 'target_file_name', 'target_file_size', 'uid']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(cim_df.limit(1).toJSON().first())
    assert event_dict['src'] == '138.197.152.110'
    assert event_dict['dest'] == '10.10.17.101'

def test_conn_json(spark_session, mocker):
    file = f"{os.curdir}/tests/samples/parsers/zeek/conn.log"

    dataSourceObj = DataSource(spark=spark_session, database="sirens", source="zeek", sourcetype="conn_json")
    config = dataSourceObj.read()

    df = spark_session.read.json(file)
    parser = zeek.Parse(spark_session)
    bronze_df = parser.toBronze(df, dataSourceObj)
    cols = bronze_df.columns
    event_dict = json.loads(bronze_df.limit(1).toJSON().first())

    expected_cols = ['_event_date', '_event_time', '_ingest_time', '_source', '_source_file', '_sourcetype', 'conn_state',
                     'duration', 'dvc_hostname', 'history', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p',
                     'missed_bytes', 'orig_bytes', 'orig_ip_bytes', 'orig_pkts', 'proto', 'resp_bytes',
                     'resp_ip_bytes', 'resp_pkts', 'service', 'ts', 'uid']
    assert all(col in cols for col in expected_cols)

    silver_df = parser.toSilver(bronze_df, dataSourceObj)
    cols = silver_df.columns

    expected_cols = ['_event_date', '_event_time', '_ingest_time', '_source', '_source_file', '_sourcetype', 'conn_state',
                     'duration', 'dvc_hostname', 'history', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p', 'missed_bytes',
                     'orig_bytes', 'orig_ip_bytes', 'orig_pkts', 'proto', 'resp_bytes', 'resp_ip_bytes', 'resp_pkts', 'service',
                     'ts', 'uid']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(silver_df.limit(1).toJSON().first())
    assert event_dict['_event_date'] == '2022-10-17'

    normalizer = normalize.Normalizer(spark_session, dataSourceObj)

    table_tranformations = config.get("transforms").get("silver").get("event_type")
    target_table = table_tranformations[0]['target_table']   # file
    event_filtered_df = normalizer.filter_frame(df=silver_df, target_table=target_table)
    cim_df = normalizer.transform_frame(df=event_filtered_df, target_table=target_table)
    cols = cim_df.columns

    expected_cols = ['_event_date', '_event_time', '_source', '_sourcetype', 'dest', 'dest_bytes',
                     'dest_ip_addr', 'dest_packets', 'dest_port', 'dvc_action', 'dvc_hostname',
                     'event_message', 'event_result', 'event_schema_file', 'event_severity',
                     'event_type', 'network_application', 'network_duration', 'network_protocol',
                     'src', 'src_bytes', 'src_ip_addr', 'src_packets', 'src_port', 'uid']

    assert all(col in cols for col in expected_cols)

    event_dict = json.loads(cim_df.limit(1).toJSON().first())
    assert event_dict['src'] == '10.10.17.101'
    assert event_dict['dest'] == '10.10.17.1'


