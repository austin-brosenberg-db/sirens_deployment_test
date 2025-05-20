from databricks.sirens import exceptions

def test_sirens_exception():
    result = exceptions.SirensException()

def test_sirens_user_exception():
    result = exceptions.SirensUserException()

def test_sirens_aggregation_exception():
    result = exceptions.SirensAggregationException()

def test_sirens_connector_exception():
    result = exceptions.SirensConnectorException()

def test_sirens_expectations_exception():
    result = exceptions.SirensExpectationsException()

def test_sirens_parsing_exception():
    result = exceptions.SirensParsingException()

def test_sirens_sql_exception():
    result = exceptions.SirensSQLException()

def test_sirens_normalize_exception():
    result = exceptions.SirensNormalizeException()

def test_sirens_config_exception():
    result = exceptions.SirensConfigException()

def test_sirens_global_config_exception():
    result = exceptions.SirensGlobalConfigException()

def test_sirens_detection_exception():
    result = exceptions.SirensDetectionException()

def test_sirens_action_exception():
    result = exceptions.SirensActionException()

def test_sirens_alertmanager_exception():
    result = exceptions.SirensAlertManagerException()












