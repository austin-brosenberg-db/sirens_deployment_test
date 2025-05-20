import pytest

from databricks.sirens.exceptions import SirensUserException
from databricks.sirens.controller import Controller, NotebookLanguage, NotebookType, NotebookStage


def test__gen_ingest_notebook_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._write_notebook", return_value=True)
    inputs = {"fn": "input", "source": "aws", "sourcetype": "cloudtrail", "groupby": "stage"}
    controller_obj._gen_notebook(stage=NotebookStage.ingest, nb_lang=NotebookLanguage.python, nb_type=NotebookType.delta, inputs=inputs)


def test__gen_normalize_notebook_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._write_notebook", return_value=True)
    inputs = {"fn": "input", "source": "aws", "sourcetype": "cloudtrail", "groupby": "stage"}
    controller_obj._gen_notebook(stage=NotebookStage.normalize, nb_lang=NotebookLanguage.python, nb_type=NotebookType.delta, inputs=inputs)


def test__gen_parse_notebook_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._write_notebook", return_value=True)
    inputs = {"fn": "input", "source": "aws", "sourcetype": "cloudtrail", "groupby": "stage"}
    controller_obj._gen_notebook(stage=NotebookStage.normalize, nb_lang=NotebookLanguage.python, nb_type=NotebookType.delta, inputs=inputs)


def test__gen_standardize_notebook_neg(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._write_notebook", return_value=True)
    inputs = {"fn": "input", "source": "aws", "sourcetype": "cloudtrail", "groupby": "stage"}
    with pytest.raises(SirensUserException) as exc:
        controller_obj._gen_standardize_notebook(nb_lang=NotebookLanguage.python, nb_type=NotebookType.delta, inputs=inputs)
    assert exc.type is SirensUserException


def test__gen_standardize_notebook_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._write_notebook", return_value=True)
    inputs = {"fn": "input", "source": "aws", "sourcetype": "cloudtrail", "groupby": "stage"}
    controller_obj._gen_standardize_notebook(nb_lang=NotebookLanguage.python, nb_type=NotebookType.dlt, inputs=inputs)


@pytest.mark.skip(reason="code not implemented yet")
def test__gen_detection_notebook_pos():
    assert True


def test__get_template_pos(controller_obj):
    result = controller_obj._get_template(nb_lang=NotebookLanguage.python, nb_type=NotebookType.delta,
                                          stage=NotebookStage.ingest.value)
    assert result is not None


@pytest.mark.skip(reason="code not implemented yet")
def test__get_detection_template_pos():
    assert True


def test_search_file_path_neg(controller_obj):
    conf_type = 'enrichments'
    search_path = ["tests/nonexistent_dir/yaml_files"]
    result = controller_obj._search_file_path(conf_type, search_path)
    assert isinstance(result, list)
    assert len(result) == 0


def test__search_file_path_pos(controller_obj):
    conf_type = 'enrichments'
    search_path = ["tests/samples/yaml_files"]
    result = controller_obj._search_file_path(conf_type, search_path)
    assert isinstance(result, list)
    assert len(result) == 1


@pytest.mark.skip(reason="code not implemented yet")
def test__read_detection_yaml_pos():
    assert True


@pytest.mark.skip(reason="code not implemented yet")
def test__write_detection_notebook_pos():
    assert True


@pytest.mark.skip(reason="Airplane - need more info..")
def test__write_notebook_pos(mocker, controller_obj):
    # TODO figure how to patch deploy_to var. Then write temp dir, and delete.
    mocker.patch("databricks.sirens.controller.Controller.open", return_value=True)
    controller_obj._write_notebook()


@pytest.mark.skip(reason="Airplane - need more info..")
def test__write_dashboard(mocker, controller_obj):
    content = "write to file"
    file = "test_file"
    controller_obj._write_dashboard(content, file)


def test_generate_notebooks_missing_key_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._gen_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._gen_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._gen_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._gen_standardize_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._write_notebook")
    if controller_obj.config.has_option('global', 'input_config_dir'):
        controller_obj.config.remove_option('global', 'input_config_dir')
    controller_obj.generate_notebooks(stanzas="apache:access_combined")
    assert controller_obj.config_dir == 'log_sources'


def test_generate_notebooks_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Controller._gen_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._gen_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._gen_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._gen_standardize_notebook", return_value=0)
    mocker.patch("databricks.sirens.controller.Controller._write_notebook")
    controller_obj.generate_notebooks(stanzas="apache:access_combined")


def test_validate_neg(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Validator.validate_file", return_value=True)
    search_path = 'log_sources/'
    result = controller_obj.validate('inputs', [search_path])
    assert result == 1


def test_validate_pos(mocker, controller_obj):
    mocker.patch("databricks.sirens.controller.Validator.validate_file", return_value=False)
    search_path = 'log_sources/'
    result = controller_obj.validate('inputs', [search_path])
    assert result == 0


def test_deploy(mocker, controller_obj):
    mocker.patch("databricks.sirens.deployer.TF.apply", return_value=True)
    mocker.patch("databricks.sirens.deployer.Repos._get_input", return_value="aa")
    mocker.patch("databricks.sirens.deployer.Repos.get_details", return_value=None)
    result = controller_obj.deploy()
    assert result is True


def test_destroy(mocker, controller_obj):
    mocker.patch("databricks.sirens.deployer.TF.destroy", return_value=True)
    mocker.patch("databricks.sirens.deployer.Repos._get_input", return_value="aa")
    mocker.patch("databricks.sirens.deployer.Repos.get_details", return_value=None)
    result = controller_obj.destroy()
    assert result is True


def test_read_yaml_file():
    result = Controller._read_yaml_file("tests/data/yaml/rule_dict.yaml")
    assert isinstance(result, dict)


def test_read_yaml_file_non_existent():
    with pytest.raises(FileNotFoundError):
        result = Controller._read_yaml_file("tests/data/yaml/rule_dict_non_existent.yaml")
        assert result

def test__write_intel_notebook_collector(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config)._write_intel_notebook(nb_type='collector',
                                                             notebook='x',
                                                             template_content='x',
                                                             no_clobber=False)
    assert result
    
def test__write_intel_notebook_ibgest(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config)._write_intel_notebook(nb_type='ingest',
                                                             notebook='x',
                                                             template_content='x',
                                                             no_clobber=False)
    assert result

def test__write_intel_notebook_no_clobber(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config)._write_intel_notebook(nb_type='ingest',
                                                             notebook='x',
                                                             template_content='x',
                                                             no_clobber=True)
    assert result

def test__write_hunt_notebook_no_clobber(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config)._write_threat_hunt_notebook(hunt_name='hunt1',
                                                             notebook='x',
                                                             template_content='x',
                                                             no_clobber=True)
    assert result

@pytest.mark.skip("code refactor needed in new PR ISS-1314")
def test__make_intel_notebooks(global_config, mocker):
    notebook = {"type": "ingest", "notebook": "x", "filename": "x"}
    result = Controller(global_config)._make_intel_notebooks()
    assert True

def test__get_intel_configuration(global_config):
    result = Controller(global_config)._get_intel_configuration()
    assert isinstance(result, list)

def test__get_intel_feeds_from_global_config(global_config):
    result = Controller(global_config)._get_intel_feeds_from_global_config()
    assert isinstance(result, list)
    assert len(result) > 0
    
def test__generate_threat_intel(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config)._generate_threat_intel()
    assert result

def test_generate_threat_intel(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config).generate_threat_intel()
    assert result
    
def test_generate_threat_hunts(mocker, global_config):
    mocker.patch("databricks.sirens.controller.Controller._write_file")
    result = Controller(global_config).generate_threat_hunts()
    assert result

def test_gen(global_config):
    from databricks.sirens.deployer import Inventory, Jobs
    from databricks.sirens.exceptions import SirensConfigException

    master_inventory = Inventory.get_master_inventory(global_config)
    result = Jobs().create_intel_tf(master_inventory)
    assert isinstance(result, dict)

def test_build(global_config, mocker):
    mocker.patch("databricks.sirens.utils.base_utils.BaseUtils.dict_to_json_file")
    result = Controller(global_config).build()
    assert result
