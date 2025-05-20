from databricks.sirens import logging

def test_colours():
    result = logging.colours
    assert result.INFO == '\033[92m'

def test_logLevels():
    assert logging.logLevels.get('INFO') == 20

def test__read_log_level_pos():
    result = logging._read_log_level().upper()
    assert result == 'INFO' or result == 'WARN' or result == 'ERROR' or result == 'DEBUG' or result == 'CRITICAL'

def test_get_logger_pos():
    result = logging.get_logger(__name__)
