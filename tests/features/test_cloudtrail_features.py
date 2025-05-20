from datetime import datetime, timezone
from pyspark.sql.functions import to_timestamp

from databricks.sirens.features.cloudtrail import *
from tests.utils import *


@pytest.mark.usefixtures("spark_session")
def test_username_assumerole(spark_session):
    """Test extract username with email address form from userIdentity.principalId"""
    user_identity = spark_session.createDataFrame(
        Row(
            rec={
                "userIdentity": {
                    "principalId": "YUGiAAAAAAAAAAAAAAG8w=:firstname.lastname@example.com"
                }
            }
        ),
        schema=CloudtrailSchema,
    )
    assert "firstname.lastname@example.com" == \
           user_identity.withColumn("user", CloudTrailFeatures.username()).head().user


@pytest.mark.usefixtures("spark_session")
def test_username(spark_session):
    """Test extract username from userIdentity.accountId"""
    user_identity = spark_session.createDataFrame(
        Row(
            rec={
                "userIdentity": {
                    "principalId": "YUGiAAAAAAAAAAAAAAG8w",
                    "accountId": "12345678",
                }
            }
        ),
        schema=CloudtrailSchema,
    )
    assert "12345678" == user_identity.withColumn("user", CloudTrailFeatures.username()).head().user


@pytest.mark.usefixtures("spark_session")
def test_username_other(spark_session):
    """Test if missing principleId infomation"""
    user_identity = spark_session.createDataFrame(
        Row(
            rec={
                "userIdentity": {
                    "accountId": "12345678",
                }
            }
        ),
        schema=CloudtrailSchema,
    )
    assert "12345678" == user_identity.withColumn("user", CloudTrailFeatures.username()).head().user


@pytest.mark.usefixtures("spark_session")
def test_assume_role_extract_role(spark_session):
    """Extract role from requestParameters.roleArn"""
    assume_role_request_role = spark_session.createDataFrame(
        Row(
            rec={
                "eventName": "AssumeRole",
                "requestParameters": {
                    "roleArn": "arn:aws:iam::123456789012:role/test_role"
                },
            }
        ),
        schema=CloudtrailSchema,
    )
    assert "test_role" == assume_role_request_role.withColumn("role", CloudTrailFeatures.request_roleArn()).head().role


@pytest.mark.usefixtures("spark_session")
def test_assume_role_extract_recipient_account(spark_session):
    """Extract role from requestParameters.roleArn"""
    assume_role_request_account = spark_session.createDataFrame(
        Row(
            rec={
                "eventName": "AssumeRole",
                "requestParameters": {
                    "roleArn": "arn:aws:iam::123456789012/myspecialrole"
                },
            }
        ),
        schema=CloudtrailSchema,
    )
    assert "123456789012" == assume_role_request_account \
        .withColumn("request_recipient_account_id", CloudTrailFeatures.request_recipient_account_id()) \
        .head().request_recipient_account_id


@pytest.mark.usefixtures("spark_session")
def test_assume_role_extract_no_role(spark_session):
    """Extract role from requestParameters.roleArn"""
    assume_role_request_role = spark_session.createDataFrame(
        Row(
            rec={
                "eventName": "AssumeRole",
                "requestParameters": {"roleArn": "arn:aws:iam::123456789012"},
            }
        ),
        schema=CloudtrailSchema,
    )
    assert "" == assume_role_request_role.withColumn("role", CloudTrailFeatures.request_roleArn()).head().role


@pytest.mark.usefixtures("spark_session")
def test_assume_role_response(spark_session):
    """Extract AssumeRole ResponseElements extractions"""
    assume_role_response = spark_session.createDataFrame(
        Row(
            rec={
                "eventName": "AssumeRole",
                "responseElements": {
                    "credentials": """{"accessKeyId":"AS1234567890ABCDEFGH",
                                       "expiration":"2020-12-01 00:35:10",
                                       "sessionToken": "testToken"}""",
                    "assumedRoleUser": """{"assumedRoleId":"1234567890ABCDEFGH",
                                           "arn":"testARNhere"}""",
                },
            }
        ),
        schema=CloudtrailSchema,
    )
    row = (
        assume_role_response.withColumn(
            "credentials", CloudTrailFeatures.assumeRole_response("credentials")
        )
        .select("credentials.*")
        .withColumn(
            "expiration", to_timestamp("expiration")
        )
        .head()
    )
    assert "AS1234567890ABCDEFGH" == row.accessKeyId
    assert (datetime(2020, 12, 1, 0, 35, 10, tzinfo=timezone.utc) == row.expiration.astimezone(timezone.utc))
    assert "testToken" == row.sessionToken

    row = (
        assume_role_response.withColumn(
            "assumedRoleUser",
            CloudTrailFeatures.assumeRole_response("assumedRoleUser"),
        )
        .select("assumedRoleUser.*")
        .head()
    )

    assert "1234567890ABCDEFGH" == row.assumedRoleId
    assert "testARNhere" == row.arn


@pytest.mark.usefixtures("spark_session")
def test_credentials(spark_session):
    credentials = spark_session.createDataFrame(
        Row(
            rec={
                "responseElements": {"credentials": """{"sessionToken": "fake token", "col2": "_"}"""},
            }
        ),
        schema=CloudtrailSchema
    )

    assert credentials.withColumn("credentials",
                                  CloudTrailFeatures.credentials()).head().credentials['sessionToken'] == "fake token"
