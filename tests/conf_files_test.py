from databricks.sirens.controller import Controller


def test_enrichment_conf_syntax(access_log_config):
    controller = Controller(access_log_config().global_config)
    result = controller.validate(conf_type='enrichments', search_path=['conf'])
    assert not result


def test_enrichment_logsource_syntax(access_log_config):
    controller = Controller(access_log_config().global_config)
    result = controller.validate(conf_type='enrichments', search_path=['log_sources'])
    assert not result

    # TODO add log sources for inputs in new PR.
