from datetime import date
from dateutil.relativedelta import relativedelta
from databricks.sirens.maintenance.retention_policy import RetentionPolicy


def test_retension_policy_parsing(global_config_rec):
    config = global_config_rec
    retention_policy = RetentionPolicy.from_config(config, "zscalerdb", "network")
    cutoffDate = date.today() - relativedelta(weeks=2)
    assert retention_policy, "retention policy for zscalerdb.network should exist."
    assert retention_policy.cutoffDate == cutoffDate, "Retention policy shoud compute cutoff data correctly."
    # case 2: table policy does not exist, fall back to database policy
    retention_policy = RetentionPolicy.from_config(config, "zscalerdb", "network2")
    cutoffDate = date.today() - relativedelta(months=6)
    assert retention_policy, "retention policy for zscalerdb should exist."
    assert retention_policy.cutoffDate == cutoffDate, "Retention policy shoud compute cutoff data correctly."
    # case 3: db policy does not exist, fall back to global policy
    retention_policy = RetentionPolicy.from_config(config, "zscalerdb2", "network2")
    cutoffDate = date.today() - relativedelta(years=1)
    assert retention_policy, "global retention policy should exist."
    assert retention_policy.cutoffDate == cutoffDate, "Retention policy shoud compute cutoff data correctly."

def test_generate_commands(global_config_rec):
    config = global_config_rec
    retention_policy = RetentionPolicy.from_config(config, "zscalerdb", "network")
    assert retention_policy._generate_commands() == f"""DELETE FROM {retention_policy.database}.{retention_policy.tableName} WHERE {retention_policy.columnName} < DATE('{retention_policy.cutoffDate}')"""
