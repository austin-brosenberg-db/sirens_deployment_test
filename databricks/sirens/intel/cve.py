from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType, MapType, BooleanType
from pyspark.sql.functions import *

# Define the schema for the CVE JSON data
cve_record_schema = StructType([
    StructField('containers', StructType([
        StructField('cna', StructType([
            StructField('affected', ArrayType(StructType([
                StructField('collectionURL', StringType(), True),
                StructField('collection_url', StringType(), True),
                StructField('cpe', ArrayType(StringType(), True), True),
                StructField('cpes', ArrayType(StringType(), True), True),
                StructField('defaultStatus', StringType(), True),
                StructField('modules', ArrayType(StringType(), True), True),
                StructField('packageName', StringType(), True),
                StructField('platforms', ArrayType(StringType(), True), True),
                StructField('product', StringType(), True),
                StructField('programFiles', ArrayType(StringType(), True), True),
                StructField('programRoutines', ArrayType(StructType([
                    StructField('name', StringType(), True)
                ]), True), True),
                StructField('repo', StringType(), True),
                StructField('vendor', StringType(), True),
                StructField('versions', ArrayType(StructType([
                    StructField('None', StringType(), True),
                    StructField('changes', ArrayType(StructType([
                        StructField('at', StringType(), True),
                        StructField('status', StringType(), True)
                    ]), True), True),
                    StructField('lessThan', StringType(), True),
                    StructField('lessThanOrEqual', StringType(), True),
                    StructField('status', StringType(), True),
                    StructField('version', StringType(), True),
                    StructField('versionType', StringType(), True)
                ]), True), True),
                StructField('x_redhatStatus', StringType(), True)
            ]), True), True),
            StructField('configurations', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('supportingMedia', ArrayType(StructType([
                    StructField('base64', BooleanType(), True),
                    StructField('type', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('credits', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('type', StringType(), True),
                StructField('user', StringType(), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('dateAssigned', StringType(), True),
            StructField('datePublic', StringType(), True),
            StructField('descriptions', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('supportingMedia', ArrayType(StructType([
                    StructField('base64', BooleanType(), True),
                    StructField('type', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('exploits', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('supportingMedia', ArrayType(StructType([
                    StructField('base64', BooleanType(), True),
                    StructField('type', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('impacts', ArrayType(StructType([
                StructField('capecId', StringType(), True),
                StructField('descriptions', ArrayType(StructType([
                    StructField('lang', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True)
            ]), True), True),
            StructField('metrics', ArrayType(StructType([
                StructField('cvssV2_0', StructType([
                    StructField('baseScore', DoubleType(), True),
                    StructField('baseSeverity', StringType(), True),
                    StructField('vectorString', StringType(), True),
                    StructField('version', StringType(), True)
                ]), True),
                StructField('cvssV3_0', StructType([
                    StructField('attackComplexity', StringType(), True),
                    StructField('attackVector', StringType(), True),
                    StructField('availabilityImpact', StringType(), True),
                    StructField('baseScore', DoubleType(), True),
                    StructField('baseSeverity', StringType(), True),
                    StructField('confidentialityImpact', StringType(), True),
                    StructField('exploitCodeMaturity', StringType(), True),
                    StructField('integrityImpact', StringType(), True),
                    StructField('privilegesRequired', StringType(), True),
                    StructField('remediationLevel', StringType(), True),
                    StructField('reportConfidence', StringType(), True),
                    StructField('scope', StringType(), True),
                    StructField('temporalScore', DoubleType(), True),
                    StructField('temporalSeverity', StringType(), True),
                    StructField('userInteraction', StringType(), True),
                    StructField('vectorString', StringType(), True),
                    StructField('version', StringType(), True)]), True),
                StructField('cvssV3_1', StructType([
                    StructField('attackComplexity', StringType(), True),
                    StructField('attackVector', StringType(), True),
                    StructField('availabilityImpact', StringType(), True),
                    StructField('availabilityRequirement', StringType(), True),
                    StructField('baseScore', DoubleType(), True),
                    StructField('baseSeverity', StringType(), True),
                    StructField('confidentialityImpact', StringType(), True),
                    StructField('confidentialityRequirement', StringType(), True),
                    StructField('environmentalScore', DoubleType(), True),
                    StructField('environmentalSeverity', StringType(), True),
                    StructField('exploitCodeMaturity', StringType(), True),
                    StructField('integrityImpact', StringType(), True),
                    StructField('integrityRequirement', StringType(), True),
                    StructField('modifiedAttackComplexity', StringType(), True),
                    StructField('modifiedAttackVector', StringType(), True),
                    StructField('modifiedAvailabilityImpact', StringType(), True),
                    StructField('modifiedConfidentialityImpact', StringType(), True),
                    StructField('modifiedIntegrityImpact', StringType(), True),
                    StructField('modifiedPrivilegesRequired', StringType(), True),
                    StructField('modifiedScope', StringType(), True),
                    StructField('modifiedUserInteraction', StringType(), True),
                    StructField('privilegesRequired', StringType(), True),
                    StructField('remediationLevel', StringType(), True),
                    StructField('reportConfidence', StringType(), True),
                    StructField('scope', StringType(), True),
                    StructField('temporalScore', DoubleType(), True),
                    StructField('temporalSeverity', StringType(), True),
                    StructField('userInteraction', StringType(), True),
                    StructField('vectorString', StringType(), True),
                    StructField('version', StringType(), True)
                ]), True),
                StructField('format', StringType(), True),
                StructField('other', StructType([
                    StructField('content', StructType([
                        StructField('attackComplexity', StringType(), True),
                        StructField('attackVector', StringType(), True),
                        StructField('availabilityImpact', StringType(), True),
                        StructField('baseScore', DoubleType(), True),
                        StructField('baseSeverity', StringType(), True),
                        StructField('confidentialityImpact', StringType(), True),
                        StructField('description', StructType([
                            StructField('description_data', ArrayType(StructType([
                                StructField('lang', StringType(), True),
                                StructField('value', StringType(), True)
                            ]), True), True)
                        ]), True),
                        StructField('integrityImpact', StringType(), True),
                        StructField('lang', StringType(), True),
                        StructField('namespace', StringType(), True),
                        StructField('other', StringType(), True),
                        StructField('privilegesRequired', StringType(), True),
                        StructField('scope', StringType(), True),
                        StructField('ssvc', StringType(), True),
                        StructField('text', StringType(), True),
                        StructField('url', StringType(), True),
                        StructField('userInteraction', StringType(), True),
                        StructField('value', StringType(), True),
                        StructField('vectorString', StringType(), True),
                        StructField('version', StringType(), True)
                    ]), True),
                    StructField('type', StringType(), True)
                ]), True),
                StructField('scenario', StringType(), True),
                StructField('scenarios', ArrayType(StructType([
                    StructField('lang', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True)
            ]), True), True),
            StructField('problemTypes', ArrayType(StructType([
                StructField('descriptions', ArrayType(StructType([
                    StructField('CWE-ID', StringType(), True),
                    StructField('cweId', StringType(), True),
                    StructField('description', StringType(), True),
                    StructField('lang', StringType(), True),
                    StructField('reference', StructType([
                        StructField('url', StringType(), True)]), True),
                    StructField('type', StringType(), True)]), True), True)
            ]), True), True),
            StructField('providerMetadata', StructType([
                StructField('dateUpdated', StringType(), True),
                StructField('orgId', StringType(), True),
                StructField('shortName', StringType(), True)
            ]), True),
            StructField('references', ArrayType(StructType([
                StructField('name', StringType(), True),
                StructField('refsource', StringType(), True),
                StructField('tags', ArrayType(StringType(), True), True),
                StructField('url', StringType(), True)]), True), True),
            StructField('rejectedReasons', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('supportingMedia', ArrayType(StructType([
                    StructField('base64', BooleanType(), True),
                    StructField('type', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('replacedBy', ArrayType(StringType(), True), True),
            StructField('solutions', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('supportingMedia', ArrayType(StructType([
                    StructField('base64', BooleanType(), True),
                    StructField('type', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('source', StructType([
                StructField('advisory', StringType(), True),
                StructField('defect', ArrayType(StringType(), True), True),
                StructField('defects', ArrayType(StringType(), True), True),
                StructField('discovery', StringType(), True),
                StructField('found_during', StringType(), True)
            ]), True),
            StructField('tags', ArrayType(StringType(), True), True),
            StructField('timeline', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('time', StringType(), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('title', StringType(), True),
            StructField('workarounds', ArrayType(StructType([
                StructField('lang', StringType(), True),
                StructField('supportingMedia', ArrayType(StructType([
                    StructField('base64', BooleanType(), True),
                    StructField('type', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('value', StringType(), True)
            ]), True), True),
            StructField('x_ConverterErrors', StructType([
                StructField('DATE_PUBLIC', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True),
                StructField('TITLE', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True),
                StructField('affects', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True),
                StructField('cvssV3_0', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True),
                StructField('cvssV3_1', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True),
                StructField('product_name', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True), StructField('version_name', StructType([
                    StructField('error', StringType(), True),
                    StructField('message', StringType(), True)
                ]), True)
            ]), True),
            StructField('x_generator', StringType(), True),
            StructField('x_legacyV4Record', StructType([
                StructField('CNA_private', StructType([
                    StructField('CVE_list', ArrayType(StringType(), True), True),
                    StructField('CVE_table_description', ArrayType(StringType(), True), True),
                    StructField('internal_comments', StringType(), True),
                    StructField('owner', StringType(), True),
                    StructField('publish', StructType([
                        StructField('month', StringType(), True),
                        StructField('year', StringType(), True),
                        StructField('ym', StringType(), True)]), True),
                    StructField('share_with_CVE', BooleanType(), True),
                    StructField('todo', ArrayType(StringType(), True), True)
                ]), True),
                StructField('CVE_data_meta', StructType([
                    StructField('AKA', StringType(), True),
                    StructField('ASSIGNER', StringType(), True),
                    StructField('DATA_ASSIGNED', StringType(), True),
                    StructField('DATE_ASSIGNED', StringType(), True),
                    StructField('DATE_ASSIGNEDE', StringType(), True),
                    StructField('DATE_PUBLIC', StringType(), True),
                    StructField('DATE_REQUESTED', StringType(), True),
                    StructField('ID', StringType(), True),
                    StructField('REQUESTER', StringType(), True),
                    StructField('STATE', StringType(), True),
                    StructField('STATE_DETAIL', StringType(), True),
                    StructField('TITLE', StringType(), True),
                    StructField('UPDATED', StringType(), True),
                    StructField('vendor_name', StringType(), True)
                ]), True),
                StructField('affects', StructType([
                    StructField('vendor', StructType([
                        StructField('vendor_data', ArrayType(StructType([
                            StructField('product', StructType([
                                StructField('product_data', ArrayType(StructType([
                                    StructField('product_name', StringType(), True),
                                    StructField('version', StructType([
                                        StructField('version_data', ArrayType(StructType([
                                            StructField('affected', StringType(), True),
                                            StructField('affected:', StringType(), True),
                                            StructField('configuration', StringType(), True),
                                            StructField('platform', StringType(), True),
                                            StructField('version_affected', StringType(), True),
                                            StructField('version_begin', StringType(), True),
                                            StructField('version_name', StringType(), True),
                                            StructField('version_number', StringType(), True),
                                            StructField('version_value', StringType(), True),
                                            StructField('versions_affected', StringType(), True)
                                        ]), True), True)
                                    ]), True)
                                ]), True), True)
                            ]), True),
                            StructField('vendor_name', StringType(), True)
                        ]), True), True)
                    ]), True)
                ]), True),
                StructField('configuration', StringType(), True),
                StructField('containers', StructType([
                    StructField('cna', StructType([
                        StructField('affected', ArrayType(StructType([
                            StructField('defaultStatus', StringType(), True),
                            StructField('product', StringType(), True),
                            StructField('vendor', StringType(), True),
                            StructField('versions', ArrayType(StructType([
                                StructField('lessThanOrEqual', StringType(), True),
                                StructField('status', StringType(), True),
                                StructField('version', StringType(), True),
                                StructField('versionType', StringType(), True)
                            ]), True), True)
                        ]), True), True),
                        StructField('descriptions', ArrayType(StructType([
                            StructField('lang', StringType(), True),
                            StructField('supportingMedia', ArrayType(StructType([
                                StructField('base64', BooleanType(), True),
                                StructField('type', StringType(), True),
                                StructField('value', StringType(), True)
                            ]), True), True),
                            StructField('value', StringType(), True)
                        ]), True), True),
                        StructField('impacts', ArrayType(StructType([
                            StructField('capecId', StringType(), True),
                            StructField('descriptions', ArrayType(StructType([
                                StructField('lang', StringType(), True),
                                StructField('value', StringType(), True)
                            ]), True), True)
                        ]), True), True),
                        StructField('metrics', ArrayType(StructType([
                            StructField('cvssV3_1', StructType([
                                StructField('attackComplexity', StringType(), True),
                                StructField('attackVector', StringType(), True),
                                StructField('availabilityImpact', StringType(), True),
                                StructField('baseScore', DoubleType(), True),
                                StructField('baseSeverity', StringType(), True),
                                StructField('confidentialityImpact', StringType(), True),
                                StructField('integrityImpact', StringType(), True),
                                StructField('privilegesRequired', StringType(), True),
                                StructField('scope', StringType(), True),
                                StructField('userInteraction', StringType(), True),
                                StructField('vectorString', StringType(), True),
                                StructField('version', StringType(), True)
                            ]), True),
                            StructField('format', StringType(), True),
                            StructField('scenarios', ArrayType(StructType([
                                StructField('lang', StringType(), True),
                                StructField('value', StringType(), True)
                            ]), True), True)
                        ]), True), True),
                        StructField('problemTypes', ArrayType(StructType([
                            StructField('descriptions', ArrayType(StructType([
                                StructField('cweId', StringType(), True),
                                StructField('description', StringType(), True),
                                StructField('lang', StringType(), True),
                                StructField('type', StringType(), True)
                            ]), True), True)
                        ]), True), True),
                        StructField('providerMetadata', StructType([
                            StructField('orgId', StringType(), True)
                        ]), True),
                        StructField('references', ArrayType(StructType([
                            StructField('url', StringType(), True)
                        ]), True), True),
                        StructField('source', StructType([
                            StructField('discovery', StringType(), True)
                        ]), True),
                        StructField('title', StringType(), True),
                        StructField('x_generator', StructType([
                            StructField('engine', StringType(), True)
                        ]), True)
                    ]), True)
                ]), True),
                StructField('credit', StringType(), True),
                StructField('cveMetadata', StructType([
                    StructField('assignerOrgId', StringType(), True),
                    StructField('cveId', StringType(), True),
                    StructField('requesterUserId', StringType(), True),
                    StructField('serial', StringType(), True),
                    StructField('state', StringType(), True)]), True),
                StructField('cve_id', StringType(), True),
                StructField('dataType', StringType(), True),
                StructField('dataVersion', StringType(), True),
                StructField('data_format', StringType(), True),
                StructField('data_type', StringType(), True),
                StructField('data_version', StringType(), True),
                StructField('description', StructType([
                    StructField('description_data', ArrayType(StructType([
                        StructField('lang', StringType(), True),
                        StructField('value', StringType(), True)
                    ]), True), True)
                ]), True),
                StructField('discoverer', StringType(), True),
                StructField('exploit', StringType(), True),
                StructField('generator', StringType(), True),
                StructField('impact', StringType(), True),
                StructField('problemtype', StructType([
                    StructField('problemtype_data', ArrayType(StructType([
                        StructField('description', ArrayType(StructType([
                            StructField('lang', StringType(), True),
                            StructField('value', StringType(), True)
                        ]), True), True)
                    ]), True), True)
                ]), True),
                StructField('references', StructType([
                    StructField('reference_data', ArrayType(StructType([
                        StructField('name', StringType(), True),
                        StructField('refsource', StringType(), True),
                        StructField('title', StringType(), True),
                        StructField('url', StringType(), True)
                    ]), True), True)
                ]), True),
                StructField('solution', StringType(), True),
                StructField('source', StructType([
                    StructField('advisory', StringType(), True),
                    StructField('defect', ArrayType(StringType(), True), True),
                    StructField('discovery', StringType(), True),
                    StructField('found_during', StringType(), True)
                ]), True),
                StructField('timeline', ArrayType(StructType([
                    StructField('lang', StringType(), True),
                    StructField('time', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('work_around', ArrayType(StructType([
                    StructField('lang', StringType(), True),
                    StructField('value', StringType(), True)
                ]), True), True),
                StructField('workaround',
                            StructType([
                                StructField('workaround_data', StructType([
                                    StructField('description', StructType([
                                        StructField('description_data', ArrayType(StructType([
                                            StructField('lang', StringType(), True),
                                            StructField('value', StringType(), True)
                                        ]), True), True)
                                    ]), True)
                                ]), True)
                            ]), True), StructField('x_advisoryEoL', BooleanType(), True),
                StructField('x_affectedList', ArrayType(StringType(), True), True),
                StructField('x_likelyAffectedList', ArrayType(StringType(), True), True)
            ]), True),
            StructField('x_redHatCweChain', StringType(), True)
        ]), True)
    ]), True),
    StructField('cveMetadata', StructType([
        StructField('assignerOrgId', StringType(), True),
        StructField('assignerShortName', StringType(), True),
        StructField('cveId', StringType(), True),
        StructField('datePublished', StringType(), True),
        StructField('dateRejected', StringType(), True),
        StructField('dateReserved', StringType(), True),
        StructField('dateUpdated', StringType(), True),
        StructField('requesterUserId', StringType(), True),
        StructField('serial', StringType(), True),
        StructField('state', StringType(), True)
    ]), True),
    StructField('dataType', StringType(), True),
    StructField('dataVersion', StringType(), True)
])
