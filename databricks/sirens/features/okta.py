from pyspark.sql.functions import expr, struct, from_json, to_json, col, to_timestamp, lit
from pyspark.sql.types import *


class OktaFeatures:
    @staticmethod
    def debugContext():
        """Converts debugContext.debugData into a map"""

        return struct(
            from_json(to_json('debugContext.debugData'), "map<string, string>").alias('debugData')
        )

    @staticmethod
    def published():
        """Converts published from string to timestamp"""

        return to_timestamp('published')

    @staticmethod
    def targetUser():
        """Extract the canonical username from an Okta record."""

        return expr("LOWER(FILTER(target, x -> x.type = 'User')[0].alternateId)")

    @staticmethod
    def targetUserGroup():
        """Extract the UserGroup displayName from an Okta record."""

        return expr("LOWER(FILTER(target, x -> x.type = 'UserGroup')[0].displayName)")

    @staticmethod
    def privilegeGranted():
        """What privileges were granted"""

        return col("debugContext.debugData.privilegeGranted")

    @staticmethod
    def targetApp():
        """Extract the name of the application being accessed."""

        return expr("""filter(target, x -> x.type='AppInstance')[0].alternateId""")

    @staticmethod
    def oktaURL():
        """This is in CJ Poller and requires some extra extraction. This is a placeholder for now"""
        return lit('')

    @staticmethod
    def requestApiTokenId():
        """Extract the requestApiTokenId from transaction.detail."""
        return expr("""FROM_JSON(transaction.detail, "MAP<STRING,STRING>").requestApiTokenId""")

    @staticmethod
    def add_all_features(df):
        return (
            df
            .withColumn('debugContext', OktaFeatures.debugContext())
            .withColumn('published', OktaFeatures.published())
            .select(
                '*',
                OktaFeatures.targetUser().alias('targetUser'),
                OktaFeatures.targetUserGroup().alias('targetUserGroup'),
                OktaFeatures.privilegeGranted().alias('privilegeGranted'),
                OktaFeatures.targetApp().alias('targetApp'),
                OktaFeatures.oktaURL().alias('oktaURL'),
                OktaFeatures.requestApiTokenId().alias('requestApiTokenId'),
            )
        )
