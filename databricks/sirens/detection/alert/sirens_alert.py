from typing import Union, Optional
from pyspark.sql.functions import lit, current_timestamp, struct, to_json, to_date, expr, col, when
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from databricks.sirens.detection.alert.alert import Alert

AlertSchema = StructType([
    # The version of this schema. Use Metadata.VERSION below.
    StructField("schemaVersion", IntegerType(), True),

    # Unique identifier for the detection that triggered this alert. E.g.
    # * the name of an alerting rule
    # * name of a correlation function
    # * name of a model
    # The name + ruleVersion should be traceable to a particular code commit that shows the alerting logic.
    StructField('name', StringType(), True),

    # Descriptive text provided by the alerting data source, can be different for every alert with this name.
    # (e.g. "Instance i-9999 looked up a domain associated with C&C"
    StructField('summary', StringType(), True),

    # Severity of the alert.  low, medium, high
    StructField('severity', StringType(), True),

    # Particular details to uniquely identify particular instances of the source.
    # This would typically be generated at alert time using a template that includes
    # fields from the data table.
    StructField('source', StringType(), True),

    # If there's a hierarchical naming scheme then use colon-delimited.
    # For example: "aws:123456789:us-west-1:sensor1" for a sensor deployed in a particular
    # regional workspace. With the "source" field and "sourceDetails" field we should be able to
    # know where to go to investigate a particular alert.
    StructField('sourceDetails', StringType(), True),

    # Use AlertEnum
    StructField('alertClass', StringType(), True),

    # Version of rule (or model) that triggered alert.
    # This could be an MLFLOW Version number for a registered ML model, a field in the rule config
    # that is updated whenever the rule is changed, or a version ID provided from a vendor.
    # Using the ruleVersion and name, you should be able to look up the exact configuration that led
    # to the alert, e.g. a particular GitHub commit where the version was incremented.
    StructField('ruleVersion', StringType(), True),

    # UTC Time the security event occurred, should come from the source data.
    StructField('eventTime', TimestampType(), True),

    # UTC Time the alert was generated, should come from the source data if the source data is an alert.
    ## Otherwise, this field will be filled in with current_timestamp() when the alert is generated.
    StructField('alertedTime', TimestampType(), True),

    # Raw timestamp from the data source, e.g. if time zone is unknown.  Leave out if no raw time
    StructField('rawTime', TimestampType(), True),

    # Date value, based on timeReceived as only timestamp we definitely have.
    # Used to partition the data.
    StructField('eventDate', DateType(), True),

    # The entity that is doing the attack, the subject.
    StructField('actor', StructType([

        # Use the EntityType enum
        StructField('type', StringType(), True),

        # Entity's domain of operation. For example "aws-prod" or "example.com"
        StructField('domain', StringType(), True),

        # A unique, static identifier for the entity, e.g. "user123@example.com"
        StructField('id', StringType(), True),

        # For RBAC, the role this entity was using.
        StructField('role', StringType(), True),

        # range [-1.0, 1.0]
        # The belief level that the actor entity has been compromised.
        # 1.0 means certainty that the actor IS compromised (the proposition is true)
        # -1.0 means certainty that the actor IS NOT compromised (the proposition is false)
        # 0.0 means that there is no information.
        StructField('beliefCompromised', FloatType(), True)

    ]), True),
    # The entity that is being attacked, the object.
    StructField('target', StructType([

        # Use the EntityType enum
        StructField('type', StringType(), True),

        # Entity's domain of operation. For example "aws-prod:us-west-2" or "example.com"
        StructField('domain', StringType(), True),

        # A unique, static identifier for the entity, e.g. "user123@example.com"
        StructField('id', StringType(), True),

        # range [-1.0, 1.0]
        # The belief level that the actor entity has been compromised.
        # (See actorEntity.beliefCompromised for explanation of values)
        StructField('beliefCompromised', FloatType(), True)

    ]), True),

    # List of attacks being done, subject === attack ==> object
    StructField('attacks', ArrayType(StructType([
        StructField('mitre', StructType([
            # Use MitreTaxonomyType below
            StructField('taxonomy', StringType(), True),

            # E.g. Use MitreEnterpriseTactic or MitreMobileTactic enum below
            StructField('tactic', StringType(), True),

            # E.g. Use Mitre's Technique ID, e.g. "T1098"
            StructField('techniqueId', StringType(), True),

            # E.g. use Mitre's Technique name, e.g. "Account Manipulation"
            StructField('technique', StringType(), True),

            # E.g. Use Mitre's Subtechnique ID, e.g. "T1098.004"
            StructField('subtechniqueId', StringType(), True),

            # E.g. Use Mitre's Subtechnique name, e.g. "SSH Authorized Keys"
            StructField('subtechnique', StringType(), True),

        ]), True),

        # Cyber Kill Chain
        StructField('killchain', StructType([

            # Use Kill chain Stage below
            StructField('stage', StringType(), True)
        ])),

        # Custom taxonomy
        StructField('other', StringType(), True)
    ])), True),

    # Data that could be matched to, or produce, threat intel
    StructField('observables', StructType([
        # Dotted decimal IPv4 or colon-delimited IPv6
        StructField('ipAddresses', ArrayType(StringType()), True),

        # FQDN
        StructField('domains', ArrayType(StringType()), True),

        # SHA-1, SHA-256, MD5, etc.
        StructField('fileHashes', ArrayType(StringType()), True),

        # File paths
        StructField('filePaths', ArrayType(StringType()), True),

        # URLs
        StructField('urls', ArrayType(StringType()), True),

        # Executable path of processes that are running.
        # (e.g. /bin/bash or /Applications/Google Chrome.app/Contents/Frameworks/Google Chrome Framework.framework/Versions/110.0.548)
        StructField('processes', ArrayType(StringType()), True)
    ]), True),

    # If this is a correlation event, list the uuid fields from the events that led to this event.
    StructField("sourceUuids", ArrayType(StringType()), True),

    # A risk metric.
    # If using quantified risk this would be the product of beliefCompromised * magnitude
    # (summing over both entities), where magnitude is the level of impact if the particular
    # entity is compromised, which will differ based on factors such as level of access,
    # proximity to crown jewels, etc.
    StructField("risk", FloatType(), True),

    # The scale on which risk is expressed. This could be an abstract scale such as 0-100 per entity.
    # To use quantified risk using the FAIR framework (https://fairinstitute.org), specify the currency
    # using ISO4217 codes (e.g. "USD").
    StructField("riskScale", StringType(), True),

    # Additional context, provided as a key/value map. Because this schema would be different for
    # each alert name, we don't provide a specific set of key fields.
    StructField('context', MapType(StringType(), StringType()), True),

    # Alert data as provided by the data source, as JSON.
    # The schema is typically characteristic of the alert source and type.
    StructField('rawJson', StringType(), True),

    ## Not in the configuration but calculated based on alert fields and other context.
    StructField('uuid', StringType(), True),

    # the filter logic (i.e. code in SELECT WHERE clause) used when the alert was generated
    StructField('filter', StringType(), False),

    # module name - usually the pipeline where the rule was run from
    StructField('moduleName', StringType(), False)
])


class SirensAlert(Alert):
    schema_field_set = {
        'schemaVersion',
        'name',
        'summary',
        'severity',
        'source',
        'sourceDetails',
        'alertClass',
        'ruleVersion',
        'eventTime',
        'alertedTime',
        'rawTime',
        'eventDate',
        'actor',
        'target',
        'attacks',
        'observables',
        'context',
        'sourceUuids',
        'risk',
        'riskScale',
        'uuid',
        'filter',
        'rawJson',
        'moduleName'
    }

    def __init__(self, module_name: str, detection_rules,
                 output_path: Optional[str] = None, output_table: Optional[str] = None):
        super().__init__(module_name, detection_rules, output_path, output_table)
        self.schema = AlertSchema

    def make_alert(self, detected_df: DataFrame, key: str, is_struct=True):
        row = self.rule_set[key].pyspark_fields

        if 'rawJson' in detected_df.schema.names:
          rawJson = col('rawJson') 
        elif is_struct:
          rawJson = to_json(struct(*detected_df.schema.names))
        else:
          rawJson = struct(*detected_df.schema.names).cast('string')
        
        uuid = row['uuid'] if 'uuid' in row else expr('uuid()')

        def get_row_val(key):
            row_val = row.get(key) if key in row else lit(None)
            return row_val if is_struct else row_val.cast(AlertSchema[key].dataType.simpleString())

        inner_fields = (
            lit(Metadata.VERSION).alias('schemaVersion'),
            get_row_val('name').alias('name'),
            get_row_val('summary').alias('summary'),
            get_row_val('severity').alias('severity'),
            get_row_val('source').alias('source'),
            get_row_val('sourceDetails').alias('sourceDetails'),
            get_row_val('alertClass').alias('alertClass'),
            get_row_val('ruleVersion').alias('ruleVersion'),
            get_row_val('eventTime').alias('eventTime'),
            get_row_val('alertedTime').alias('alertedTime'),
            get_row_val('rawTime').alias('rawTime'),
            to_date(get_row_val('eventTime')).alias('eventDate'),
            get_row_val('actor').alias('actor'),
            get_row_val('target').alias('target'),
            get_row_val('attacks').alias('attacks'),
            get_row_val('observables').alias('observables'),
            get_row_val('sourceUuids').alias('sourceUuids'),
            get_row_val('risk').alias('risk'),
            get_row_val('riskScale').alias('riskScale'),
            get_row_val('context').alias('context'),
            rawJson.alias('rawJson'),
            uuid.alias('uuid'),
            lit(self.rule_set[key].filter).alias('filter'),
            lit(self.module_name).alias('moduleName')
        )

        return struct(*inner_fields) if is_struct else detected_df.select(*inner_fields)


class Metadata:
    VERSION = 4  # Increment version whenever the schema is changed.
