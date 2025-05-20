"""Implements the classes used to represent any configuration files.

Returns: Configuration file object generally as python dict.

Authors:
    Derek King (4th July 2022)

Classes:
    GlobalConfig() - reader for sirens.config
    Actions() - reader for actions.yaml
    Alerts() - reader for alerts.yaml
    BaseConf: - read yamls in conf dir
    AggregateReader(BaseConf): - extends BaseConf for aggregations
    EnrichmentReader(BaseConf): - extends BaseConf for enrichments
    ThreatHuntReader(BaseConf): - extends BaseConf for threat hunts
    ThreatIntelReader(BaseConf): - extends BaseConf for threat intelligence
    CommonInformationModel() - reads cim_defs
    Detection() - detection file reader
"""
import json, os
from typing import Literal

from yaml._yaml import ConstructorError

from databricks.sirens import _version
from databricks.sirens.detection import *
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.internal.secrets import SecretsManager
from databricks.sirens.logging import get_logger
from databricks.sirens.utils.config_utils import config_utils
from databricks.sirens.utils.path_utils import PathUtils
from databricks.sirens.utils.schema_utils import DataFrameSchema

logger = get_logger(__name__)
ENGINE_VERSION = _version.__version__


class Alerts:
    """Class to process the alerts config file

    :raises SirensGlobalConfigException: _description_
    :raises SirensConfigException: _description_
    :return: None
    :rtype: None
    """

    @staticmethod
    def read() -> dict:
        """Read the global alerts config file

        :raises SirensGlobalConfigException: _description_
        :raises SirensConfigException: _description_
        :return: alerts config as dict
        :rtype: dict
        """
        file_dirs = PathUtils.get_relative_file_paths(["conf", "alerts"])
        for dir in file_dirs:
            try:
                file = "alerts" + ".yaml"
                config_file = Path(os.path.join(dir, file))
                if config_file.is_file():
                    with open(config_file, 'r') as f:
                        alert_config = yaml.safe_load(f)

                    return alert_config
            except Exception as exc:
                raise SirensGlobalConfigException(exc) from exc
        raise SirensGlobalConfigException('Global alerts.yaml not found.')


class BaseConf:
    """Class to read conf files at both system and content pack levels
       More Conf files can be added over time.
    """

    def __init__(self):
        gc = GlobalConfig.get()
        self.input_conf_dir = GlobalConfig.get_input_config_dir(gc)
        self.conf_dir = GlobalConfig.get_configs_default_dir(gc)
        self.running_notebook = BaseUtils.get_notebook_name()

    @staticmethod
    def list_directory(directory: List[str], with_paths: bool = True) -> List[str]:
        """
        List the contents of directories.

        This function takes a list of directory paths and returns the contents of the first existing directory found.
        The contents can be returned either as full paths or just the names of the items.

        :param directory: List of directory paths to check.
        :type directory: List[str]
        :param with_paths: If True, return full paths of the directory contents. If False, return only the names.
        :type with_paths: bool, optional
        :return: List of directory contents as full paths or names.
        :rtype: List[str]
        """
        for path in directory:
            p = Path(path)
            if p.exists() and with_paths:
                return [str(x) for x in p.iterdir()]
            if p.exists() and not with_paths:
                return [str(x.name) for x in p.iterdir()]

    @staticmethod
    def get_named_conf(running_config: list[dict], config_name: str) -> dict:
        """given a name param, return the config section for it

        :param running_config: config
        :type running_config: list
        :param config_name: name param to fetch
        :type config_name: str
        :return: sub dict with config_name
        :rtype: dict
        """
        for conf_item in running_config:
            if conf_item.get("name") == config_name:
                return conf_item

        logger.info(f"config name: {config_name} not found.")
        return {}

    def get_file_paths(self, file_type: str, level: Literal["system", "pack"],
                        source: str = None, sourcetype: str = None, source_dir: str = None,
                        sub_dir: str = None) -> Optional[list]:
        source_dir = source_dir if source_dir else self.conf_dir
        return self._get_file_paths(file_type, level, source, sourcetype, source_dir, sub_dir)

    @staticmethod
    def _get_file_paths(file_type: str, level: Literal["system", "pack"],
                        source: str = None, sourcetype: str = None, source_dir: str = None,
                        sub_dir: str = None) -> Optional[list]:
        """ create file paths for either the system directory, or the local content pack

        :param file_type: name of the config file (without .yaml extension)
        :type file_type: Literal[&quot;enrichments&quot;, &quot;aggregationss&quot;]
        :param level: system|pack
        :type level: Literal[&quot;system&quot;, &quot;pack&quot;]
        :param source: if local pack then source where were looking, defaults to None
        :type source: str, optional
        :param sourcetype: if local pack then sourcetype where were looking, defaults to None
        :type sourcetype: str, optional
        :param source_dir: global input_conf dir, defaults to None
        :type source_dir: str, optional
        :param sub_dir: sub directory (e.g conf/threat_hunting/sub_dir='hunt_exec_T1048'), defaults to None
        :type sub_dir: str, optional
        :return: _description_
        :rtype: list
        """

        file_dirs = []
        if level != "system" and level != "pack":
            logger.error(f"Invalid level: {level} passed")
            raise SirensConfigException(f"Invalid level: {level} passed")

        if 'system' in level:
            logger.debug(f"Getting {file_type} from system level")
            if not sub_dir:
                file_dirs = PathUtils.get_relative_file_paths([source_dir, file_type])
            else:
                file_dirs = PathUtils.get_relative_file_paths([source_dir, file_type, sub_dir])
        elif 'pack' in level and source and sourcetype:
            logger.debug(f'Getting {file_type} from app level for datasource: {source}, {sourcetype}')
            if not sub_dir:
                file_dirs = PathUtils.get_relative_file_paths([source_dir, source, sourcetype])
            else:
                file_dirs = PathUtils.get_relative_file_paths([source_dir, source, sourcetype, sub_dir])

        if not file_dirs:
            logger.warning("failed to create possible file paths for the requested config files")
            return None
        else:
            return file_dirs

    @staticmethod
    def read(paths: list, file_name: str) -> dict:
        """read the contents of config files

        :return: content of the config file as dict.
        :rtype: dict
        """

        for dir in paths:
            try:
                file_content = None
                file = file_name + ".yaml"

                config_file = Path(os.path.join(dir, file))
                if config_file.is_file():
                    with open(config_file, 'r') as f:
                        try:
                            file_content = yaml.load(f, Loader=yaml.SafeLoader)
                        except ConstructorError:
                            logger.error("yaml error: either the file os badly formatted, or maybe you configured a {"
                                         "{secret}} without enclosing in quotes?")
                        except yaml.YAMLError as exc:
                            raise SirensConfigException(exc) from exc
                    break

            except Exception as exc:
                raise SirensConfigException(exc) from exc

        if not file_content:
            logger.info(f"no content found for file: {config_file}")
        else:
            logger.debug(f"file_content: {file_content}")
            # check for secrets and replace them
            file_content = SecretsManager().substitute_secrets(file_content)

        return file_content


class ActionReader(BaseConf):
    def __init__(self):
        super().__init__()

    def read(self, action_module: str, source: str = None, sourcetype: str = None) -> List[Dict]:
        """read the action module that exists under the conf directory

        :param action_module: example: slack,webhook etc
        :type action_module: str
        :return: contents of the yaml file as python dict
        :rtype: dict
        """
        config, running_config = [], []
        # get system level actions
        paths = self._get_file_paths(file_type='actions', level='system',
                                     source_dir=self.conf_dir, sub_dir=action_module)

        config = super().read(paths=paths, file_name='action')
        if config:
            running_config = config

        # precedence order for a log_source level action if defined.
        # look for actions in log/sources/<s>/<st>/actions/<module>/action.yaml>
        if source and sourcetype:
            paths = self._get_file_paths(file_type='actions', level='pack',
                                         source=source, sourcetype=sourcetype,
                                         source_dir=self.input_conf_dir,
                                         sub_dir=os.path.join('actions', action_module))

            config = super().read(paths=paths, file_name='action')
            if config:
                running_config = config

        if not running_config:
            logger.info(f"no action for {action_module} found.")
            return []

        return running_config


class AggregateReader(BaseConf):
    """read aggregations.yaml config file
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def get_aggregate(running_config: list[dict], aggregate_name: str) -> dict:
        """
        Retrieve the aggregate configuration from the running configuration.

        :param running_config: The current running configuration.
        :type running_config: list of dict
        :param aggregate_name: The name of the aggregate to retrieve.
        :type aggregate_name: str
        :return: The configuration dictionary for the specified aggregate.
        :rtype: dict
        """
        return BaseConf.get_named_conf(running_config, aggregate_name)

    def read(self, source: str = None, sourcetype: str = None) -> List[Dict]:
        """
        Reads the configuration for aggregations from specified sources.

        This method first attempts to read system-wide aggregation configurations.
        If a specific source and sourcetype are provided, it will attempt to read
        more specific content pack definitions.

        :param source: The source of the configuration (optional).
        :type source: str, optional
        :param sourcetype: The type of the source (optional).
        :type sourcetype: str, optional
        :return: A list of dictionaries containing the configuration.
        :rtype: List[Dict]
        """

        # Look initially for system-wide aggregations.
        config, running_config = [], []

        paths = self._get_file_paths('aggregations', 'system',
                                     source_dir=self.conf_dir)
        config = super().read(paths, 'aggregations')
        if config:
            running_config = config

        # prefer more specific content pack definitions if available.
        if bool(source) and bool(sourcetype):
            paths = self._get_file_paths('aggregations', 'pack',
                                         source=source, sourcetype=sourcetype,
                                         source_dir=self.input_conf_dir)
            config = super().read(paths, 'aggregations')
            if config:
                running_config = config

        if not running_config:
            logger.info("no aggregate configuration found.")
            return []

        return running_config


class EnrichmentReader(BaseConf):
    """read enrichments.yaml config file
    """

    def __init__(self):
        super().__init__()

    def read(self, source: str = None, sourcetype: str = None) -> List[Dict]:
        """
        Reads configuration data from specified sources and returns a list of dictionaries.

        This method first reads the system-level configuration and then, if a more specific
        content pack configuration is available, it reads that and overrides the system-level
        configuration.

        Args:
            source (str, optional): The source name for the content pack configuration. Defaults to None.
            sourcetype (str, optional): The type of the source for the content pack configuration. Defaults to None.

        Returns:
            List[Dict]: A list of dictionaries containing the configuration data.
        """

        config, running_config = [], []

        paths = self._get_file_paths('enrichments', 'system', source_dir=self.conf_dir)
        config = super().read(paths, 'enrichments')
        if config:
            running_config = config
            [x.update({'defined_at': 'system'}) for x in config]

        # prefer more specific content pack config if available.
        if source and sourcetype:
            paths = self._get_file_paths('enrichments', 'pack',
                                         source=source, sourcetype=sourcetype,
                                         source_dir=self.input_conf_dir)
            config = super().read(paths, 'enrichments')
            if config:
                running_config = config
                [x.update({'defined_at': 'pack'}) for x in config]

        return running_config


class ThreatIntelReader(BaseConf):
    """read threat_intelligence config files"""

    def __init__(self):
        super().__init__()

    def read(self, feed: str = None) -> List:
        """
        Reads the threat intelligence configuration and returns the relevant configuration based on the provided feed.

        :param feed: The name of the feed to filter the configuration. If None, the entire configuration is returned.
        :type feed: str, optional
        :return: The configuration for the specified feed or the entire configuration if no feed is specified.
        :rtype: List
        :raises Exception: If an error occurs while reading the configuration.
        """
        config, running_config = [], []
        paths = super()._get_file_paths(file_type='threat_intelligence',
                                        source_dir=self.conf_dir, level='system')
        config = super().read(paths, 'intel')

        try:
            if config:
                if feed:
                    for x in config.get('feeds'):
                        if x.get('source_name') == feed:
                            running_config = x
                            return running_config
                    logger.warning(f'feed: {feed} not found in threat_intel config yaml')

                else:
                    running_config = config
        except Exception as exc:
            logger.error({exc})
            raise exc

        return running_config

    def read_named_file(self, file_name: str) -> Union[List[Dict], Dict]:
        """
        Reads a named file from the threat intelligence configuration directory.
        This method retrieves the file paths for the specified file type and reads the configuration
        from the given file name. If the configuration is not found, it returns an empty list.
        :param file_name: The name of the file to read.
        :type file_name: str
        :return: The configuration data read from the file, either as a list of dictionaries or a single dictionary.
        :rtype: Union[List[Dict], Dict]
        """
        paths = super()._get_file_paths(file_type='threat_intelligence',
                                        source_dir=self.conf_dir, level='system')
        config = super().read(paths, file_name)
        if not config:
            return []

        return config


class ThreatHuntReader(BaseConf):
    """read a threat_hunting config file
    """

    def __init__(self):
        super().__init__()

    def get_this_notebook_conf(self, running_config, notebook_name):
        """
        Retrieve the configuration for a specific notebook.

        This method extracts the configuration for a given notebook from the
        provided running configuration.

        :param running_config: The complete running configuration dictionary.
        :type running_config: dict
        :param notebook_name: The name of the notebook whose configuration is to be retrieved.
        :type notebook_name: str
        :return: The configuration for the specified notebook.
        :rtype: dict
        """
        config_part = running_config['notebooks']
        return self.get_named_conf(config_part, notebook_name)

    def read(self, hunt_name: str) -> dict:
        """
        Reads the configuration for a given hunt name.

        This method retrieves the file paths for the specified hunt name and reads the configuration
        from those paths. If a configuration is found, it is returned as a dictionary.

        :param hunt_name: The name of the hunt for which to read the configuration.
        :type hunt_name: str
        :return: The configuration for the specified hunt name.
        :rtype: dict
        """

        hunt_name = hunt_name.strip()
        config, running_config = [], []
        paths = self._get_file_paths(file_type="threat_hunting", level='system',
                                     source_dir=self.conf_dir, sub_dir=hunt_name)

        config = super().read(paths, 'hunt')

        if config:
            running_config = config

        return running_config

    def list_hunts(self) -> List[Dict]:
        """
        List all threat hunt configurations.

        This method retrieves all threat hunt configuration files from the specified
        directory, reads them, and returns a list of configurations.

        Returns:
            List[Dict]: A list of dictionaries, each representing a threat hunt configuration.
        """
        hunt_configs = []
        print(f'file_type="threat_hunting", level="system", source_dir={self.conf_dir}')
        paths = self._get_file_paths(file_type="threat_hunting", level='system', source_dir=self.conf_dir)
        hunts = self.list_directory(paths)
        if not hunts:
            logger.info(f"No threat hunts found under directory: {self.conf_dir}")

        for hunt in hunts:
            config = super().read([hunt], 'hunt')
            if config:
                hunt_configs.append(config)
        return hunt_configs


class PlaybookReader(BaseConf):
    """read a playbooks config file
    """

    def __init__(self):
        super().__init__()

    def read(self, playbook_name: str) -> dict:
        """
        Reads the configuration for a given playbook name.

        This method retrieves the file paths for playbooks at the system level
        from the specified configuration directory and sub-directory. It then
        reads the configuration using the superclass's read method and returns
        the running configuration if available.

        :param playbook_name: The name of the playbook to read the configuration for.
        :type playbook_name: str
        :return: The running configuration for the specified playbook.
        :rtype: dict
        """
        config, running_config = [], []
        paths = self._get_file_paths(file_type="playbooks", level='system',
                                     source_dir=self.conf_dir, sub_dir='response')
        config = super().read(paths, playbook_name)
        if config:
            running_config = config

        return running_config


class CommonInformationModel(BaseConf):
    """Common Information Model reader class"""

    def __init__(self):
        super().__init__()

    def read(self, model_file) -> StructType:
        """
        Reads a JSON configuration file and extracts schema keys.

        This function searches for a JSON file with the given model_file name in the
        "conf/cim_defs/default/" directory. If the file is found, it reads the
        file and extracts the field names and their data types into a list of dictionaries.

        :param model_file: The name of the model file (without the .json extension).
        :type model_file: str
        :return: A StructType of field name and datatype.
        :rtype: StructType
        :raises SirensGlobalConfigException: If there is an error reading the file or parsing the JSON.
        """
        file_dirs = self._get_file_paths(file_type="cim_defs", level='system', source_dir=self.conf_dir)

        for dir in file_dirs:
            try:
                file = model_file + ".json"
                config_file = Path(os.path.join(dir, file))
                if config_file.is_file():
                    # read JSON FILE
                    with open(config_file, 'r') as f:
                        json_object = json.load(f)
                    
                    # get schema as structtype
                    cim_schema = DataFrameSchema.as_struct([DataFrameSchema.translate_type(rec["name"], rec["datatype"]) for rec in json_object['fields']])

                    return cim_schema

            except Exception as exc:
                raise SirensGlobalConfigException(exc) from exc


class OpenCyberSecurityFramework(BaseConf):
    """Open Cyber Security Framework reader class"""
    
    def __init__(self):
        super().__init__()

    def read(self, model_file) -> StructType:
        """
        Reads a JSON model file and returns its schema as a StructType.
        :param model_file: The name of the model file (without extension) to read.
        :type model_file: str
        :return: The schema of the model file as a StructType.
        :rtype: StructType
        :raises SirensGlobalConfigException: If there is an error reading the model file.
        """
        file_dirs = self._get_file_paths(file_type="ocsf_defs", level='system', source_dir=self.conf_dir)

        for dir_path in file_dirs:
            try:
                file = model_file + ".json"
                config_file = Path(os.path.join(dir_path, file))
                if config_file.is_file():

                    # get schema as structtype
                    ocsf_schema = DataFrameSchema.as_struct(OpenCyberSecurityFramework.read_recursive(dir_path, model_file, [model_file]))

                    return ocsf_schema

            except Exception as exc:
                raise SirensGlobalConfigException(exc) from exc

    @staticmethod
    def read_recursive(dir_path, name: str, level: list) -> list:
        """
        Recursively reads JSON configuration files and constructs a list of model fields.
        This function reads a JSON file specified by `dir_path` and `name`, parses its content,
        and constructs a list of model fields. If a field's datatype is "model", it recursively
        reads the corresponding model file and appends its fields to the list.
        :param dir_path: The directory path where the JSON files are located.
        :type dir_path: str
        :param name: The name of the JSON file (without the .json extension).
        :type name: str
        :param level: A list representing the current level in the model hierarchy.
        :type level: list
        :return: A list of model fields.
        :rtype: list
        :raises Exception: If there is an error reading or parsing the JSON file.
        """
        try:
            with open(dir_path + f"/{name}.json", 'r') as f:
                model = json.load(f)

            model_fields = []

            for rec in model['fields']:

                if rec["datatype"] == "model":
                    new_fields = OpenCyberSecurityFramework.read_recursive(dir_path, rec["name"], level+[rec['name']])
                    if new_fields:
                        model_fields.append(DataFrameSchema.as_field(rec['name'], new_fields))
                else:
                    model_fields.append(DataFrameSchema.translate_type(rec["name"], rec["datatype"])) 

            return model_fields
        except Exception as exc: 
            print(exc)
            print("WARNING skipping model:", ".".join(level))
            pass

class Detection:
    """Detection Configuration reader class
    """

    def __init__(self, source, name, env=''):
        self.source = source
        self.env = env
        self.detection_prefix = None
        for path in PathUtils.get_relative_file_paths(["detection"]):
            if Path(path).is_dir():
                self.detection_prefix = path
                break

        if self.detection_prefix is None:
            raise SirensConfigException("detection directory not found")

    def list_pipelines(self):
        """list all detection pipelines

        :rtype: List<str>
        """
        pipeline_glob = f"{self.detection_prefix}/pipelines/{self.source}/**/*.yaml"
        return [
            os.path.basename(name).replace('.yaml', '')
            for name in glob.glob(pipeline_glob)
        ]

    def _load_pipeline_ruleset(self, pipeline: dict):
        """load detection pipline's ruleset

        :param pipeline: pipeline to load
        :type pipeline: dict
        :raises FileNotFoundError: If filesystem FileNotFound
        :raises SirensUserException: any other error
        :return: ruleset
        :rtype: DetectionRuleset
        """

        ruleset = DetectionRuleset()

        if self.source.lower() == 'correlation':
            alert_class = "CORRELATION"
            schema_fields = pipeline.get('schema_fields', self._load_pipeline_yaml('clustering').get('schema_fields'))
        else:
            alert_class = "ALERT"
            schema_fields = None

        for _rule in pipeline['rules']:
            if _rule['enabled'] is True:
                found = False
                for rule_dir in pipeline['rule_dir']:
                    rule_folder_prefix = f"{self.detection_prefix}/rules/{self.source}/{rule_dir}"
                    rule_folder_prefix = re.sub('\\w+/\\.\\.', '', rule_folder_prefix)
                    rule_path = f"{rule_folder_prefix}/{_rule['name']}.yaml"
                    if os.path.isfile(rule_path):
                        if found:
                            raise SirensDetectionException(f"Duplicate rule found in {pipeline['name']}: {_rule}")
                        ruleset += ruleset.load_yaml(rule_path, alert_class, _rule.get('schema_fields', schema_fields),
                                                     self.env)
                        found = True
                if not found:
                    raise SirensDetectionException(f"Rule: {_rule} not found in {pipeline['name']}")

        return ruleset

    def _load_yaml_file_recursively(self, dir: str, pipeline_name: str):
        pattern = os.path.join(dir, '**', f"{pipeline_name}.yaml")
        
        matches = glob.glob(pattern, recursive=True)
        
        if not matches:
            raise SirensDetectionException(f"Could not find '{pipeline_name}' under '{dir}'")
        
        yaml_path = matches[0]
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        return data

    def _get_detection_yaml(self, pipeline_name: str):
        return self._load_yaml_file_recursively(f"{self.detection_prefix}/pipelines/{self.source}", pipeline_name)

    def _get_transformation_yaml(self, pipeline_name: str, operation_dir: str="transformations"):
        return self._load_yaml_file_recursively(f"{self.detection_prefix}/{operation_dir}/{self.source}", pipeline_name)

    def _populate_transform_sql(self, pipeline: Dict, transform_sql: str):
        if transform_sql and pipeline.get("input_table", ""):
            pipeline["transformation_sql"] = transform_sql.replace("{{input_table}}", pipeline["input_table"]).strip()
        else:
            pipeline["transformation_sql"] = transform_sql.strip()
        return None

    def _load_pipeline_yaml(self, pipeline_name):
        """load detection pipline config by name

        :param pipeline_name: pipeline to load
        :type pipeline_name: str
        :raises FileNotFoundError: If filesystem FileNotFound
        :raises SirensUserException: any other error
        :return: pipeline and ruleset
        :rtype: dict
        """

        pipeline = self._get_detection_yaml(pipeline_name)
        pipeline = config_utils.sub_yaml_vars(pipeline, self.env)

        if self.source.lower() == 'correlation':
            if pipeline.get('dedup_on') is None:
                pipeline['dedup_on'] = ['uuid']
            if pipeline.get('schema_fields') is None:
                pipeline['schema_fields'] = SirensAlert.schema_field_set
                pipeline['schema_fields'].discard('rawJson')

        if pipeline.get('filter') is None:
            pipeline['filter'] = '1=1'

        rule_prefix = f"{self.detection_prefix}/rules/{self.source}"
        all_dirs = glob.glob(f"{rule_prefix}/*/", recursive=True)
        all_dirs = [x.split(f"{rule_prefix}/")[1] for x in all_dirs] + ['']

        if pipeline.get('rule_dir') is None:
            if self.source.lower() == 'correlation':
                pipeline['rule_dir'] = all_dirs
            else:
                pipeline['rule_dir'] = ['']
        elif pipeline['rule_dir'] == '*':
            pipeline['rule_dir'] = all_dirs
        elif isinstance(pipeline['rule_dir'], str):
            pipeline['rule_dir'] = [pipeline['rule_dir']]

        if 'interval' in pipeline:
            try:
                pipeline['interval'] = pipeline['interval'].lower()
                number, interval = pipeline['interval'].split(' ')

                if '.' in number:
                    raise SirensDetectionException(
                        f'Bad interval for pipeline: {pipeline["name"]}. Numerical value {number} appears to be a double.')

                number = int(number.strip())
                interval = interval.strip()

                expected_intervals = ['day', 'days', 'hour', 'hours', 'minute', 'minutes', 'second', 'seconds']
                if interval not in expected_intervals:
                    raise SirensDetectionException(
                        f'Bad interval for pipeline: {pipeline["name"]}. Expected {expected_intervals}, got {interval}.')
                if number > 60 or number <= 0:
                    SirensDetectionException(
                        f'Bad interval for pipeline: {pipeline["name"]}. Numerical value: {number} is not between 1 and 60')
            except:
                raise SirensDetectionException(
                    f'Bad interval for pipeline: {pipeline["name"]}. Input: {pipeline["interval"]}')
        
        if pipeline.get("transformation"):
            try:
                if "file" in pipeline["transformation"]:
                    transform_config = self._get_transformation_yaml(pipeline["transformation"]["file"])
                    self._populate_transform_sql(pipeline, transform_config["transformation"])
                elif "sql" in pipeline["transformation"]:
                    self._populate_transform_sql(pipeline, pipeline["transformation"]["sql"])
                else:
                    raise SirensDetectionException("Bad format for transformation. Expected file or sql.")
            except Exception as exp:
                logger.warning(exp)
        
        return pipeline

    def load_pipeline(self, pipeline_name: str):
        """load detection pipline config by name

        :param pipeline_name: pipeline to load
        :type pipeline_name: str
        :raises FileNotFoundError: If filesystem FileNotFound
        :raises SirensUserException: any other error
        :return: pipeline and ruleset
        :rtype: tuple
        """

        pipeline = self._load_pipeline_yaml(pipeline_name)
        ruleset = self._load_pipeline_ruleset(pipeline)

        return pipeline, ruleset
