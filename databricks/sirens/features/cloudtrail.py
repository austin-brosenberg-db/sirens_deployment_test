"""CloudTrail Features Extraction"""
from pyspark.sql import Column
from pyspark.sql.functions import expr, from_json, when, col
from pyspark.sql.types import *


class CloudTrailFeatures:
    """Collections of  Cloudtrail Features extraction function"""

    @staticmethod
    def username():
        """extract userIdentity email address from principalId otherwise use accountId or type
        >>> CloudTrailFeatures.username()
        "aws accountId"
        """
        return (
            when(expr("userIdentity.principalId LIKE '%:%@%'"), expr("SPLIT(userIdentity.principalId, ':', 2)[1]"))
            .when(expr("userIdentity.accountId IS NOT NULL"), col("userIdentity.accountId"))
            .otherwise(col("userIdentity.type"))
        )

    @staticmethod
    def request_recipient_account_id():
        """extract requestParameters request assume role acount Id
        >>> CloudTrailFeatures.request_recipient_account_id()
        "assume role account Id"
        """
        return expr(
            """
        IF(eventName = 'AssumeRole' and requestParameters is not NULL and LENGTH(requestParameters.roleArn) >= 26,
           substr(requestParameters.roleArn, 14, 12),
           "")
        """
        )

    @staticmethod
    def request_roleArn() -> Column:
        """extract requestParameters request Role ARN field if eventName is AssumeRole
        >>> CloudTrailFeatures.request_roleArn()
        "request_aws_role"
        """
        return expr(
            """
        IF(eventName = 'AssumeRole' and requestParameters is not NULL and LENGTH(requestParameters.roleArn) >= 32,
           substr(requestParameters.roleArn,32,40),
           "")
        """
        )

    @staticmethod
    def _assumeRole_response_string(field_name: str):
        return expr(
            f"""
        IF(eventName = 'AssumeRole' and responseElements is NOT NULL,
           responseElements.{field_name},
           NULL)
        """
        )

    @staticmethod
    def assumeRole_response(field_name: str):
        """extract response field content if eventName is AssumeRole
        >>> CloudTrailFeatures.assumeRole_response("assumedRoleUser").assumedRoleId
        "assumeRoleId"
        param:
            field_name
        return:
            field value
        """
        return from_json(
            CloudTrailFeatures._assumeRole_response_string(field_name),
            schema=assumeRole_response[field_name],
        )

    @staticmethod
    def hostedZoneId():
        """
        "route53 hostedZoneId"
        """

        return expr(
            f"""
        IF(eventName = 'AssociateVPCWithHostedZone' and requestParameters is not NULL,
           requestParameters.hostedZoneId,"")
        """)

    @staticmethod
    def vPCRegion():
        """
        >>> CloudTrailFeatures.vPCRegion()
        "aws vPCRegion"
        """
        return expr(
            f"""
        IF(eventName = 'AssociateVPCWithHostedZone' and requestParameters is not NULL,
           requestParameters.vPC.vPCRegion,"")
        """)

    @staticmethod
    def changeInfo():
        """
        >>> CloudTrailFeatures.changeInfo()
        "aws changeInfo"
        """
        return expr(
            f"""
        IF(eventName = 'AssociateVPCWithHostedZone' and responseElements is not NULL,
           responseElements.changeInfo,"")
        """)

    @staticmethod
    def credentials():
        return from_json('responseElements.credentials', "map<string, string>")

    @staticmethod
    def add_all_features(df):
        return (
            df
            .withColumn("username", CloudTrailFeatures.username())
            .withColumn("request_recipient_account_id", CloudTrailFeatures.request_recipient_account_id())
            .withColumn("ar_resp_credentials", CloudTrailFeatures.assumeRole_response("credentials"))
            .withColumn("ar_resp_assumedRoleUser", CloudTrailFeatures.assumeRole_response("assumedRoleUser"))
            .withColumn("credentials", CloudTrailFeatures.credentials())
        )


CloudtrailSchema = StructType([
    StructField('date', DateType(), True),
    StructField('recipientAccountId', StringType(), True),
    StructField('awsRegion', StringType(), True),
    StructField('errorMessage', StringType(), True),
    StructField('eventSource', StringType(), True),
    StructField('eventName', StringType(), True),
    StructField('requestParameters', MapType(StringType(), StringType(), True), True),
    StructField('responseElements', MapType(StringType(), StringType(), True), True),
    StructField('sourceIPAddress', StringType(), True),
    StructField('userIdentity',
                StructType(
                    [StructField('accessKeyId', StringType(), True),
                     StructField('accountId', StringType(), True),
                     StructField('arn', StringType(), True),
                     StructField('invokedBy', StringType(), True),
                     StructField('principalId', StringType(), True),
                     StructField('sessionContext',
                                 StructType([
                                     StructField('attributes',
                                                 StructType([
                                                     StructField('creationDate', StringType(), True),
                                                     StructField('mfaAuthenticated', StringType(), True)
                                                 ]), True),
                                     StructField('sessionIssuer',
                                                 StructType([
                                                     StructField('accountId', StringType(), True),
                                                     StructField('arn', StringType(), True),
                                                     StructField('principalId', StringType(), True),
                                                     StructField('type', StringType(), True),
                                                     StructField('userName', StringType(), True)
                                                 ]), True)
                                 ]), True),
                     StructField('type', StringType(), True),
                     StructField('userName', StringType(), True),
                     StructField('webIdFederationData',
                                 StructType([
                                     StructField('federatedProvider', StringType(), True),
                                     StructField('attributes', MapType(StringType(), StringType(), True), True)
                                 ]), True)
                     ]), True),
    StructField('vpcEndpointId', StringType(), True),
    StructField('additionalEventData', StringType(), True),
    StructField('apiVersion', StringType(), True),
    StructField('errorCode', StringType(), True),
    StructField('eventID', StringType(), True),
    StructField('eventType', StringType(), True),
    StructField('eventTime', StringType(), True),
    StructField('eventVersion', StringType(), True),
    StructField('readOnly', BooleanType(), True),
    StructField('requestID', StringType(), True),
    StructField('resources',
                ArrayType(StructType([
                    StructField('ARN', StringType(), True),
                    StructField('accountId', StringType(), True),
                    StructField('type', StringType(), True)
                ]), True), True),
    StructField('sharedEventID', StringType(), True),
    StructField('serviceEventDetails', MapType(StringType(), StringType(), True), True),
    StructField('userAgent', StringType(), True),
    StructField('recordJson', StringType(), True),
    StructField('ingestFilename', StringType(), True),
    StructField('ingestLine', IntegerType(), True),
    StructField('ingestTime', TimestampType(), True),
    StructField('timestamp', TimestampType(), True)
])

assumeRole_response = {
    "credentials": StructType([
        StructField('accessKeyId', StringType()),
        StructField('expiration', StringType()),
        StructField('sessionToken', StringType())
    ]),
    "assumedRoleUser": StructType([
        StructField('assumedRoleId', StringType()),
        StructField('arn', StringType())
    ])
}
