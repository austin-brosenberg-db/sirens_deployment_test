import json

from tests.utils import *
from pyspark.sql.types import Row
import pyspark.sql.functions as func

from pyspark.sql.types import *
from databricks.sirens.features.okta import OktaFeatures

okta_schema = (
  StructType()
  .add('uuid', StringType())
  .add('published', TimestampType())
  .add('eventType', StringType())
  .add('version', StringType())
  .add('severity', StringType())
  .add('legacyEventType', StringType())
  .add('displayMessage', StringType())
  .add('actor', StructType()
       .add('id', StringType())
       .add('type', StringType())
       .add('alternateId', StringType())
       .add('displayName', StringType())
       .add('detailEntry', StringType())
       )
  .add('client', StructType()
       .add('userAgent', StructType()
            .add('rawUserAgent', StringType())
            .add('os', StringType())
            .add('browser', StringType())
            )
       .add('geographicalContext', StructType()
            .add('geolocation', StructType()
                 .add('lat', DoubleType())
                 .add('lon', DoubleType())
                 )
            .add('city', StringType())
            .add('state', StringType())
            .add('country', StringType())
            .add('postalCode', StringType())
            )
       .add('zone', StringType())
       .add('ipAddress', StringType())
       .add('device', StringType())
       .add('id', StringType())
       )
  .add('outcome', StructType()
       .add('result', StringType())
       .add('reason', StringType())
       )
  .add('target', ArrayType(
       StructType([
                   StructField('id', StringType()),
                   StructField('type', StringType()),
                   StructField('alternateId', StringType()),
                   StructField('displayName', StringType()),
                   StructField('detailentry', StringType())
                   ]))
       )
  .add('transaction', StructType()
       .add('id', StringType())
       .add('type', StringType())
       .add('detail', StringType())
       )
  .add('debugContext', StructType()
       .add('debugData', MapType(StringType(), StringType()))
       )
  .add('authenticationContext', StructType()
       .add('authenticationProvider', StringType())
       .add('credentialProvider', StringType())
       .add('credentialType', StringType())
       .add('issuer', StructType()
            .add('id', StringType())
            .add('type', StringType())
            )
       .add('externalSessionId', StringType())
       .add('interface', StringType())
       )
  .add('securityContext', StructType()
       .add('asNumber', IntegerType())
       .add('asOrg', StringType())
       .add('isp', StringType())
       .add('domain', StringType())
       .add('isProxy', BooleanType())
       )
  .add('request', StructType()
       .add('ipChain', ArrayType(
            StructType([
                        StructField('ip', StringType()),
                        StructField('geographicalContext', StructType()
                                    .add('geolocation', StructType()
                                         .add('lat', DoubleType())
                                         .add('lon', DoubleType())
                                         )
                                    .add('city', StringType())
                                    .add('state', StringType())
                                    .add('country', StringType())
                                    .add('postalCode', StringType())
                                    ),
                        StructField('version', StringType()),
                        StructField('source', StringType())
                        ])
            ))
       )
)


def create_test_data(spark_session):
    with open("tests/data/okta/parsed_records.json", "r") as f:
        jsons = json.loads(f.read())
        rows = [Row(rec=json.dumps(x)) for x in jsons]
        full_schema = okta_schema
        return (spark_session.createDataFrame(rows)
                .withColumn("parsed", func.from_json(func.col("rec"), full_schema))
                .select("parsed.*")
                )


@pytest.mark.usefixtures("spark_session")
def test_parsing(spark_session):
    df = create_test_data(spark_session)
    assert "firstname.lastname@example.com" == df.head().actor.alternateId


@pytest.mark.usefixtures("spark_session")
def test_targetUser(spark_session):
    result = create_test_data(spark_session).withColumn("targetUser", OktaFeatures.targetUser()).head().targetUser
    assert "first.last@otherexample.com" == result


@pytest.mark.usefixtures("spark_session")
def test_targetUserGroup(spark_session):
    result = create_test_data(spark_session).withColumn("targetUserGroup",
                                                        OktaFeatures.targetUserGroup()).head().targetUserGroup
    assert "my user group" == result


@pytest.mark.usefixtures("spark_session")
def test_privilegeGranted(spark_session):
    df = spark_session.createDataFrame(Row(rec={
        "eventType": "user.account.privilege.grant",
        "actor": {"alternateId": "admin@example.com"},
        "target": [{"alternateId": "user@example.com"}],
        "debugContext": {"debugData": {"privilegeGranted": "Read only admin"}}
    }), schema=okta_schema)
    df = df.withColumn("privilegeGranted", OktaFeatures.privilegeGranted())
    assert "Read only admin" == df.head().privilegeGranted


@pytest.mark.usefixtures("spark_session")
def test_targetApp(spark_session):
    df = create_test_data(spark_session).withColumn("targetApp", OktaFeatures.targetApp())
    assert "PagerDuty" == df.head().targetApp

@pytest.mark.usefixtures("spark_session")
def test_requestApiTokenId(spark_session):
    result = create_test_data(spark_session).withColumn("requestApiTokenId",
                                                        OktaFeatures.requestApiTokenId()).head().requestApiTokenId
    assert "ABCDEF" == result