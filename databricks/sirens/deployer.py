"""Code to generate terraform variable files, and execute terraform commands against a workspace

    Author: Derek King
    Date: 21st Feb 2023

    Classes:
        Notebooks()
        Jobs()
        Inventory()
        DBFS()

    Functions:
        create_notebook_tf()
        create_jobs_tf()
        get_master_inventory()

"""
import configparser
import json
import os
import re
import tzlocal
import yaml
import subprocess

from pathlib import Path
from typing import Dict, List, Literal, Tuple, Union
from pkg_resources import parse_version
from yaml.loader import SafeLoader

from python_terraform import IsNotFlagged, Terraform

from databricks.sirens._version import ROOT_DIR
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.internal import restclient
from databricks.sirens.exceptions import (SirensConfigException, SirensException)
from databricks.sirens.logging import colours, get_logger

from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.utils import get_bool_value
from databricks.sirens.utils.path_utils import PathUtils
from databricks.sirens.utils.schema_utils import Schemas
from databricks.sirens.modules import get_modules

logger = get_logger(__name__)


class TF:
    """execute terraform commands
    """

    @staticmethod
    def apply(tf_vars: dict = None) -> bool:
        """call terraform apply command using vars files built by 'sirens.py build'

        :return: true on success, error if not
        :rtype: bool
        """
        logger.debug(f"calling terraform apply; tf_vars: {tf_vars}")
        target_dir = os.path.join(ROOT_DIR, 'terraform')

        t = Terraform(working_dir=target_dir)
        t.init()

        skip_plan = os.getenv('SKIP_PLAN', False) in ['true', 't', 'True', 'yes', 'y', 'Yes']

        return_code, stdout, stderr = t.apply(
            capture_output='yes', var=tf_vars, skip_plan=skip_plan)

        logger.info("Terraform apply Return code")
        logger.info(return_code)

        if return_code == 0:
            logger.info("Terraform apply succeeded.")
        elif return_code == 1:
            logger.error("Terraform apply failed.")
            raise Exception(stderr)

        return True

    @staticmethod
    def destroy(tf_vars: dict = None) -> bool:
        """call terraform destroy command using vars files built by 'sirens build'

        :return: true on success, error if not
        :rtype: bool
        """
        logger.debug(f"calling terraform destroy; tf_vars: {tf_vars}")
        target_dir = os.path.join(ROOT_DIR, 'terraform')

        t = Terraform(working_dir=target_dir)
        return_code, stdout, stderr = t.destroy(
            capture_output='yes', vars=tf_vars,
            force=IsNotFlagged,
            auto_approve=True)
        return True

    @staticmethod
    def plan(tf_vars: dict = None) -> bool:
        """call terraform plan command using vars files built by 'sirens.py build'

        :return: true on success, error if not
        :rtype: bool
        """
        logger.debug(f"calling terraform plan; tf_vars: {tf_vars}")
        target_dir = os.path.join(ROOT_DIR, 'terraform')

        t = Terraform(working_dir=target_dir)
        t.init()
        return_code, stdout, stderr = t.plan(
            capture_output='yes', vars=tf_vars)

        return True


class Inventory:
    """Creates the dict from global config needed to generate terraform tf.vars files.

    :return: _description_
    :rtype: _type_
    """

    @staticmethod
    def _read_yaml_file(file: str) -> Dict:
        """read a yaml file

        :param file: relative path to file
        :type file: str
        :return: python dict of file
        :rtype: Dict
        """
        file_names = PathUtils.get_relative_file_paths([file])
        config = {}
        for file in file_names:
            if os.path.isfile(file):
                try:
                    with open(file) as f:
                        for data in yaml.load_all(f, Loader=SafeLoader):
                            config.update(data)
                        break
                except FileNotFoundError as exc:
                    logger.error(f"{exc}")
                    return {}
        if not config:
            raise FileNotFoundError(f"config file not found at any of: {file_names}")

        return config

    @staticmethod
    def _get_compute_attrs(config: configparser.ConfigParser, section: str) -> dict:
        """reads a named compute_attributes section

        :param config: configparser object
        :type config: configparser
        :param section: section name to read
        :type section: str
        :raises SirensConfigException:
        :return: dict of key/value from the compute_attributes:[name] section
        :rtype: dict
        """
        attrs = {}
        if config.has_option(section, "compute_attributes"):
            attrs_name = config.get(section, "compute_attributes")
            attr_section = f"compute_attributes:{attrs_name}"
            if not config.has_section(attr_section):
                raise SirensConfigException(f"Error: No Section: {attr_section}")

            options = config.options(attr_section)
            for option in options:
                attrs[option] = config.get(attr_section, option)
        return attrs

    @staticmethod
    def _get_spark_options(config: configparser.ConfigParser, section: str) -> dict:
        """reads a named spark_conf_options section

        :param config: configparser object
        :type config: configparser
        :param section: section name to read
        :type section: str
        :raises SirensConfigException:
        :return: dict of key/value from the spark_conf_options:[name] section
        :rtype: dict
        """
        opts = {}
        if config.has_option(section, "spark_conf_options"):
            attrs_name = config.get(section, "spark_conf_options")
            attr_section = f"spark_conf_options:{attrs_name}"
            if not config.has_section(attr_section):
                raise SirensConfigException(f"Error: No Section: {attr_section}")

            options = config.options(attr_section)
            for option in options:
                opts[option] = config.get(attr_section, option)
        return opts

    @staticmethod
    def _get_cloud_provider(config: configparser.ConfigParser, workspace: str):
        """reads a named workspace section

        :param config: configparser object
        :type config: configparser
        :raises SirensConfigException:
        :return: cloud_provider from the workspace:[name] section
        :rtype: dict
        """
        section_name = f"workspace:{workspace}"
        if not config.has_section(section_name):
            raise SirensConfigException("Error: no section: {section_name}")

        if not config.has_option(section_name, "cloud_service_provider"):
            raise SirensConfigException(f"required key cloud_service_provider missing from {section_name} ")

        csp = config.get(section_name, 'cloud_service_provider')
        if csp not in ["aws", "gcp", "azure"]:
            raise SirensConfigException("cloud_service_provider must be 'aws', 'azure' or 'gcp'")

        return config.get(section_name, 'cloud_service_provider')

    @staticmethod
    def _get_clusters(config: configparser.ConfigParser) -> List[dict]:
        """return list of clusters in global config file

        :param config: globalConfig Object
        :type config: object
        :return: cluster list w/keys
        :rtype: list[dict]
        """
        logger.debug("getting clusters from inventory list")
        clusters = []

        for section in config:
            if not section.startswith("cluster:") and not section.startswith("jobcluster:"):
                continue

            compute_attributes, spark_conf_options, autoscale = {}, {}, {}

            default = config.get(section, "default", fallback=False)
            cluster_name = config.get(section, "cluster_name", fallback=None)
            cluster_id = config.get(section, "cluster_id", fallback='')
            autoscale['min_workers'] = config.get(section, "min_workers", fallback=None)
            autoscale['max_workers'] = config.get(section, "max_workers", fallback=None)
            try:
                workspace = config.get(section, "workspace", fallback=None)
            except KeyError:
                raise SirensConfigException(f"required workspace key missing from {section}")

            # Get workspace cloud provider
            cloud_provider = Inventory._get_cloud_provider(config, workspace)

            # Get compute_attributes for this cluster
            compute_attributes = Inventory._get_compute_attrs(config, section)

            # get spark options for this cluster
            spark_conf_options = Inventory._get_spark_options(config, section)

            clusters.append({"name": section, "default": default,
                             "cluster_name": cluster_name, "cluster_id": cluster_id,
                             "cloud_provider": cloud_provider,
                             "workspace": workspace, "spark_options": spark_conf_options,
                             "compute_attributes": compute_attributes,
                             "autoscale": autoscale})

        return clusters

    @staticmethod
    def _get_dashboards(deploy_dir: str) -> list:
        """list dashboards found under the deploy_dir

        :param deploy_dir: deploy_dir from sirens.config
        :type deploy_dir: str
        :return: list of discovered json files
        :rtype: list
        """
        logger.debug(f"looking for dashboard files in: {deploy_dir}/dashboards")
        list_dir = os.path.join(deploy_dir, "dashboards")
        files = []
        for path in Path(list_dir).rglob('*.json'):
            files.append(os.path.abspath(str(path)))
        return files

    @staticmethod
    def _get_instance_profiles_by_rest(workspace_url: str, token: str) -> list:
        """aws specific rest api call to get configured instance_profiles from the given workspace

        :param workspace_url: url of workspace (https://workspace.cloud.databricks.com)
        :type workspace_url: str
        :param token: configured PAT token/SPrincipal
        :type token: str
        :return: list of profiles
        :rtype: list
        """
        logger.debug("getting instance_profiles using REST API")
        rest_client_obj = restclient.DatabricksAPI('databricks-sirens', token=token, workspace_url=workspace_url)
        data = rest_client_obj.instance_profiles_list()
        logger.debug(f"response: {data}")
        instance_profiles = []
        if data:
            for profile in data.get("instance_profiles"):
                instance_profiles.append(profile['instance_profile_arn'])
        return instance_profiles

    @staticmethod
    def _get_warehouses_by_rest(workspace_url: str, token: str) -> List[dict]:
        """attempt to list SQL warehouses using the REST API

        :param workspace_url: ex. https://workspace.cloud.databricks.com
        :type workspace_url: str
        :param token: PAT token
        :type token: str
        :return: json list of clusters
        :rtype: list[Dict]
        """
        logger.debug("getting sql warehouses using REST API")
        rest_client_obj = restclient.DatabricksAPI('databricks-sirens', token=token, workspace_url=workspace_url)
        data = rest_client_obj.warehouses_list()
        logger.debug(f"response: {data}")
        warehouses = []
        if data:
            for ws in data.get("warehouses"):
                warehouses.append({"name": ws['name'], "id": ws['id']})
        else:
            print(
                f"{colours.INFO}no SQL warehouses configured or accessible skipping dashboard installation{colours.ENDC}")
        return warehouses

    @staticmethod
    def _get_numeric_input(prompt: str, max_value: int) -> int:
        """get a number from the user

        :param prompt: prompt for the user
        :type prompt: str
        :return: number
        :rtype: int
        """
        number = 0
        while True:
            try:
                number = int(input(prompt))
                if not isinstance(number, int):
                    print("number required")
                    continue
                if number < 1 or number > max_value:
                    print("invalid option")
                    continue
            except ValueError:
                print("enter a number")
                continue
            break
        return number

    @staticmethod
    def _get_user_option(entries: list, prompt: str) -> Tuple[int, str]:
        """print list of entries with a number to choose from

        :param entries: list of entries to display
        :type entries: list
        :param prompt: prompt for the user input
        :type prompt: str
        :return: tuple of selection number and entry value
        :rtype: tuple[int, str]
        """
        opt = 1
        print(prompt)
        for entry in entries:
            print(f"{opt} - {entry}")
            opt += 1

        option = Inventory._get_numeric_input("Option: ", len(entries))
        return option, entries[option - 1]

    @staticmethod
    def _get_warehouse_user_option(warehouses: list[dict]) -> str:
        """ask user to choose the cluster to deploy dashboards to

        :param warehouses: list of configured warehouses on the workspace
        :type warehouses: list
        :return: SQL warehouse id
        :rtype: str
        """
        opt = 1
        print("\nPlease select SQL warehouse to deploy dashboards to:\n")
        for wh in warehouses:
            print(f"{opt}) {wh['name']} (id={wh['id']})")
            opt += 1

        option = Inventory._get_numeric_input("Option: ", len(warehouses))
        warehouse_id = warehouses[option - 1]['id']
        logger.debug(f"returning warehouse id: {warehouse_id}")
        return warehouse_id

    @staticmethod
    def _get_env_vars() -> Tuple[str, str]:
        """get DATABRICKS_HOST & DATABRICKS_TOKEN environment variables

        :raises SirensEnvironmentException: on missing envs
        :return: host, token
        :rtype: str
        """
        logger.debug("getting environment variables")
        host = os.getenv('DATABRICKS_HOST')
        token = os.getenv('DATABRICKS_TOKEN')

        if not host:
            print(
                "Please set the DATABRICKS_HOST env var \n ex. (export DATABRICKS_HOST=workspace.cloud.databricks.com)")
            exit()

        if not token:
            print("Please set the DATABRICKS_TOKEN env var \n ex. (export DATABRICKS_TOKEN=dapixxxxxxx)")
            exit()

        return host, token

    @staticmethod
    def _make_url(host: str) -> str:
        """prefix host with https://

        :param host: hostname (sfe.clolud.databricks.com)
        :type host: str
        :return: full url (https://xx.xx.databricks.com)
        :rtype: str
        """
        logger.debug(f"making url for: {host}")
        if not host.startswith("https://"):
            host = 'https://' + host
        return host

    @staticmethod
    def _is_delta_nb(notebook_type: Literal["delta", "dlt"]) -> bool:
        """create bool for structured stream

        :param notebook_type: delta, dlt
        :type notebook_type: Literal[&quot;delta&quot;, &quot;dlt&quot;]
        :return: false the DLT, true for delta
        :rtype: bool
        """
        # if delta or dlt in default notebook_type
        if notebook_type == "delta":
            isDeltaStreaming = True
        else:
            isDeltaStreaming = False

        return isDeltaStreaming

    @staticmethod
    def _get_notebook_paths(groupby: Literal["stage", "source"], deploy_dir: str, notebook_type: str,
                            source: str, sourcetype: str) -> list:
        """create a list of directories where notebooks can be listed from

        :param groupby: how notebooks are grouped in sirens.config
        :type groupby: Literal[&quot;stage&quot;, &quot;source&quot;]
        :param deploy_dir: configured deploy directory in sirens.config
        :type deploy_dir: str
        :param notebook_type: delta|dlt
        :type notebook_type: str
        :param source: data input source value
        :type source: str
        :param sourcetype: data input sourcetype
        :type sourcetype: str
        :return: list of directories to search
        :rtype: list
        """
        paths = []

        if groupby == 'stage':
            paths.append(os.path.join(deploy_dir, notebook_type, 'ingest/'))
            paths.append(os.path.join(deploy_dir, notebook_type, 'parse/'))
            paths.append(os.path.join(deploy_dir, notebook_type, 'normalize/'))
            paths.append(os.path.join(deploy_dir, notebook_type, 'aggregate/'))
            paths.append(os.path.join(deploy_dir, notebook_type, 'maintenance/'))
        else:
            paths.append(os.path.join(deploy_dir, notebook_type, source, sourcetype + '/'))

        return paths

    @staticmethod
    def _get_intel_notebook_paths(notebook_name: str, deploy_dir: str):
        return os.path.join(deploy_dir, "intel", "jobs", notebook_name)

    @staticmethod
    def _get_notebook_files(paths: list) -> list:
        """list notebooks on paths

        :param paths: list of directories to search
        :type paths: list
        :return: absolute path of each discovered file in each directory
        :rtype: list
        """
        files = []
        for path in paths:
            files = os.listdir(path)
            files = [os.path.join(path, file) for file in files if os.path.isfile(os.path.join(path, file))]

        return files

    @staticmethod
    def _get_notebook_dicts(groupby: Literal["source", "stage"], deploy_dir: str, notebook_type: str,
                            source: str, sourcetype: str) -> Tuple[List, List]:
        """get a dicts of ingest and maintenance notebooks for the data source input

        :param groupby: groupby key stored in sirens.config
        :type groupby: Literal[&quot;source&quot;, &quot;stage&quot;]
        :param deploy_dir: deploy key from sirens.config
        :type deploy_dir: str
        :param notebook_type: delta|dlt
        :type notebook_type: str
        :param source: datasource source
        :type source: str
        :param sourcetype: datasource sourcetype
        :type sourcetype: str
        :return: two dicts, one with ingest and one with discoveredmaintenance notebooks
        :rtype: Tuple[List, List]
        """

        ingest_notebooks, maintenance_notebooks = [], []

        # get directories
        nb_dirs = Inventory._get_notebook_paths(groupby, deploy_dir, notebook_type, source, sourcetype)

        # list files
        notebook_files = Inventory._get_notebook_files(nb_dirs)

        # add files to respective dicts
        for file in notebook_files:
            if "01-" in file:
                ingest_notebooks.append({"ingest": os.path.splitext(file)[0]})
            if "02-" in file:
                ingest_notebooks.append({"parse": os.path.splitext(file)[0]})
            if "03-" in file:
                ingest_notebooks.append({"normalize": os.path.splitext(file)[0]})
            if "07-" in file:
                ingest_notebooks.append({"aggregate": os.path.splitext(file)[0]})
            if "06-" in file:
                maintenance_notebooks.append({"maintenance": os.path.splitext(file)[0]})

        return ingest_notebooks, maintenance_notebooks

    @staticmethod
    def _get_timezone_id():
        """get timezone name
        """
        # now = datetime.datetime.now(tz=tz.tzlocal())
        # return now.tzname()
        # issue: #460.
        return tzlocal.get_localzone_name()

    @staticmethod
    def _get_global_inventory(config) -> Dict:

        global_inventory = {
            'deploy_dir': GlobalConfig.get_deploy_dir(config),
            'groupby': GlobalConfig.get_groupby(config),
            'target_database': GlobalConfig.get_target_database(config),
            'scratch_dir': GlobalConfig.get_global_scratch_dir(config),
            'notebook_language': GlobalConfig.get_notebook_language(config),
            'notebook_type': GlobalConfig.get_notebook_type(config),
            'sirens_lib': GlobalConfig.get_global_sirens_lib(config)
        }

        if global_inventory['sirens_lib'] == "latest":
            version_match = re.compile("databricks_sirens-(.*)-py3-none-any.whl")
            try:
                libs = [lib for lib in os.listdir("lib") if version_match.match(lib)]
            except FileNotFoundError as exc:
                logger.warning("lib directory does not exist. Assuming running from source?")
                global_inventory['whl_path'] = "not found"
            else:
                lib_files = sorted(libs, key=lambda x: parse_version(version_match.search(x).group(1)))
                if len(lib_files) == 0:
                    raise SirensConfigException("No wheel files located in local lib path")
                global_inventory['whl_path'] = os.path.join("lib", lib_files[-1])
                logger.info(f"Using {global_inventory['whl_path']}")
        else:
            global_inventory['whl_path'] = os.path.relpath(global_inventory['sirens_lib'], start=os.curdir)

        # if it appears were on deploying to AWS, and instance_profile is not configured, try getting it from rest
        # this is not available for GCP or Azure.
        global_inventory['instance_profile'] = None

        # Get Clusters Sections
        global_inventory['clusters'] = Inventory._get_clusters(config)

        # get local timezone
        global_inventory['timezone_id'] = Inventory._get_timezone_id()

        return global_inventory

    @staticmethod
    def get_master_inventory(config: configparser.ConfigParser) -> List[Dict]:
        """takes config object and generates the master inventory as a dict

        :param config: _description_
        :type config: _type_
        :raises Exception: _description_
        :return: _description_
        :rtype: dict
        """
        logger.debug("getting master inventory (deployable artifacts)")
        master_inventory = []
        cluster = None
        schedule = None

        # get global vars from config.
        global_inventory = Inventory._get_global_inventory(config)

        logger.debug("getting input: stanzas from sirens.config")
        # step through sirens.config [input:xx:xx] stanzas
        for section in config.sections():
            # Only interested in inputs from here out.
            if not section.startswith("input:"):
                continue

            options = config.options(section)
            if 'enabled' not in options:
                raise SirensConfigException(f'enabled key must exist. stanza: {section} is missing it')

            # skip disabled inputs
            enabled = config.get(section, "enabled").lower()
            if enabled != 'true' and enabled != '1':
                continue

            # is this a DLT or streaming input
            is_delta_streaming = Inventory._is_delta_nb(global_inventory['notebook_type'])

            # inputs section
            _, source, sourcetype = section.split(":")

            # Allow the over-rides on a per source basis if specified.
            for option in options:
                if option == "source":
                    source = config.get(section, option)
                elif option == "sourcetype":
                    sourcetype = config.get(section, option)
                elif option == "notebook_type":
                    input_notebook_type = config.get(section, option)
                    is_delta_streaming = input_notebook_type == "delta"

            # See if job cluster or interactive cluster has been defined.
            # This should prioritize a new job cluster over an interactive (already exists cluster).
            cluster = config.get(section, "jobcluster", fallback=None)
            if not cluster:
                cluster = config.get(section, "cluster", fallback=None)

            # schedule
            schedule = config.get(section, "schedule", fallback=None)

            ingest_notebooks, maintenance_notebooks = Inventory._get_notebook_dicts(global_inventory['groupby'],
                                                                                    global_inventory['deploy_dir'],
                                                                                    global_inventory['notebook_type'],
                                                                                    source, sourcetype)

            master_inventory.append({
                "task_type": "ingest",
                "description": "ETL task for datasource: " + source + ":" + sourcetype,
                "task_name": source + "_" + sourcetype,
                "datasource_name": source + "_" + sourcetype,
                "app_name": "sirens",
                "language": global_inventory['notebook_language'],
                "isDeltaStreaming": is_delta_streaming,
                "source": source,
                "sourcetype": sourcetype,
                "schedule": schedule,
                "timezone_id": global_inventory['timezone_id'],
                "scratch_dir": global_inventory['scratch_dir'],
                "target_database": global_inventory['target_database'],
                "whl_path": global_inventory['whl_path'],
                "instance_profile": global_inventory['instance_profile'],
                "cluster": cluster,
                "clusters": global_inventory['clusters'],
                "edition": "advanced",
                "channel": "current",
                "photon": "true",
                "continuous": "false",
                "notebook_paths": ingest_notebooks
            })

            # Create a maintenance task
            master_inventory.append({
                "task_type": "maintenance",
                "task_name": "Maintenance",
                "description": "Executes maintenance tasks for datasource: " + source + ":" + sourcetype,
                "language": global_inventory['notebook_language'],
                "source": source,
                "sourcetype": sourcetype,
                "isDeltaStreaming": "true",
                "app_name": "sirens",
                "cluster": cluster,
                "clusters": global_inventory['clusters'],
                "schedule": schedule,
                "timezone_id": global_inventory['timezone_id'],
                "target_database": global_inventory['target_database'],
                "notebook_paths": maintenance_notebooks,
                "whl_path": global_inventory['whl_path']
            })

        for section in config.sections():
            if not section.startswith("alerts:"):
                continue

            options = config.options(section)
            if 'enabled' not in options:
                raise SirensConfigException(f'enabled key must exist. stanza: {section} is missing it')

            # skip disabled inputs
            enabled = get_bool_value(config.get(section, "enabled"))
            if not enabled:
                continue

            # is this a DLT or streaming input
            is_delta_streaming = Inventory._is_delta_nb(global_inventory['notebook_type'])

            # TODO: add support for DLTs
            if not is_delta_streaming:
                logger.warning("Alerts are only supported for delta streaming inputs. Skipping...")
                continue

            cluster = config.get(section, "jobcluster", fallback=None)
            if not cluster:
                cluster = config.get(section, "cluster", fallback=None)

            # schedule
            schedule = config.get(section, "schedule", fallback=None)

            database = config.get('default', "target_database", fallback=None)
            input_table = section.split(":")[1]
            datasource_name = f"{input_table}_alerts"

            alert_notebook_path = os.path.join(global_inventory['deploy_dir'], 'alerts',
                                               global_inventory['notebook_type'],
                                               f"{input_table}_table")
            alerts_notebooks = [{"name": f"{input_table}_table", "path": alert_notebook_path}]

            master_inventory.append({
                "task_type": "alerts",
                "description": f"Alerts monitoring pipeline for table {database}.{input_table}",
                "datasource_name": datasource_name,
                "task_name": f"ALERTS: {input_table}",
                "app_name": "sirens alerts",
                "language": global_inventory['notebook_language'],
                "isDeltaStreaming": is_delta_streaming,
                "schedule": schedule,
                "timezone_id": global_inventory['timezone_id'],
                "scratch_dir": global_inventory['scratch_dir'],
                "target_database": global_inventory['target_database'],
                "whl_path": global_inventory['whl_path'],
                "instance_profile": global_inventory['instance_profile'],
                "cluster": cluster,
                "clusters": global_inventory['clusters'],
                "edition": "advanced",
                "channel": "current",
                "photon": "true",
                "continuous": "false",
                "notebook_paths": alerts_notebooks,
                "input_table": input_table
            })

        for section in config.sections():
            if not section.startswith("detections:"):
                continue

            options = config.options(section)
            if 'enabled' not in options:
                raise SirensConfigException(f'enabled key must exist. stanza: {section} is missing it')

            # skip disabled inputs
            enabled = get_bool_value(config.get(section, "enabled"))
            if not enabled:
                continue

            # is this a DLT or streaming input
            notebook_type = global_inventory['notebook_type']
            is_delta_streaming = Inventory._is_delta_nb(notebook_type)

            # TODO: add support for DLTs
            if not is_delta_streaming:
                logger.warning("Alerts are only supported for delta streaming inputs. Skipping...")
                continue

            cluster = config.get(section, "jobcluster", fallback=None)
            if not cluster:
                cluster = config.get(section, "cluster", fallback=None)

            # schedule
            schedule = config.get(section, "schedule", fallback=None)

            input_table = section.split(":")[1]
            name = section.split(":")[2]
            datasource_name = f"{input_table}_{name}_detections"

            detection_notebook_path = os.path.join(global_inventory['deploy_dir'], 'detection', global_inventory['notebook_type'],
                                                 input_table, f'{name}_detections')
            detection_notebooks = [{"name": f"{input_table}_{name}", "path": detection_notebook_path}]

            master_inventory.append({
                "task_type": "detections",
                "description": f"{name} Detections pipeline for table {database}.{input_table}",
                "datasource_name": datasource_name,
                "task_name": f"DETECTIONS: {input_table}, {name}",
                "app_name": "sirens detections",
                "language": global_inventory['notebook_language'],
                "isDeltaStreaming": is_delta_streaming,
                "schedule": schedule,
                "timezone_id": global_inventory['timezone_id'],
                "scratch_dir": global_inventory['scratch_dir'],
                "target_database": global_inventory['target_database'],
                "whl_path": global_inventory['whl_path'],
                "instance_profile": global_inventory['instance_profile'],
                "cluster": cluster,
                "clusters": global_inventory['clusters'],
                "edition": "advanced",
                "channel": "current",
                "photon": "true",
                "continuous": "false",
                "notebook_paths": detection_notebooks,
                "input_table": input_table
            })

        logger.debug("getting threat_hunting: stanzas from sirens.config")
        # look through sirens.config at 'threat_hunt': section
        hunting_notebooks = []
        for section in config.sections():

            # Only interested in inputs from here out.
            if not section.startswith("threat_hunt:"):
                continue

            options = config.options(section)
            if 'enabled' not in options:
                raise SirensConfigException(f'enabled key must exist. stanza: {section} is missing it')

            # skip disabled inputs
            enabled = get_bool_value(config.get(section, "enabled"))
            if not enabled:
                continue

            is_delta_streaming = True
            _, hunt_name = section.split(":")

            # See if job cluster or interactive cluster has been defined.
            # This should prioritize a new job cluster over an interactive (already exists cluster).
            cluster = config.get(section, "jobcluster", fallback=None)
            if not cluster:
                cluster = config.get(section, "cluster", fallback=None)

            # schedule
            schedule = config.get(section, "schedule", fallback=None)


            conf_dir = GlobalConfig.get_configs_default_dir(config)
            hunt_conf_path = os.path.join(conf_dir, 'threat_hunting', hunt_name, 'hunt.yaml')

            hunt_config = Inventory._read_yaml_file(str(hunt_conf_path))
            hunt_name = hunt_config.get('hunt').get('name')
            norm_hunt_name = re.sub(r'[\.|\s]', '_', hunt_name)

            attacks = hunt_config.get("hunt").get("attacks", [])
            mitre_tactic, mitre_technique, kill_chain_stage = None, None, None
            for attack in attacks:
                for k in attack.keys():
                    if k == "mitre":
                        mitre_tactic = attack.get(k).get("tactic")
                        mitre_technique = attack.get(k).get("technique_id")
                    if k == "kill_chain":
                        kill_chain_stage = attack.get(k).get("stage")

            for nb_type in ["notebooks", "playbooks"]:
                notebooks = hunt_config.get(nb_type)
                if notebooks:
                    for notebook in notebooks:
                        notebook_name = notebook.get("name")

                        file_path = os.path.join(global_inventory['deploy_dir'], 'threat_hunting', hunt_name,
                                                 notebook_name)
                        if not os.path.exists(file_path + ".py"):
                            logger.error(f"{file_path} not created....")
                            raise SirensConfigException(f"{file_path} not created. Try generate_threat_hunts command?")

                        hunting_notebooks.append({"name": notebook.get("name"),
                                                  "path": file_path,
                                                  "task_params": {"input": str(notebook.get("input")),
                                                                  "output": str(notebook.get("output"))},
                                                  })
            master_inventory.append({
                "task_type": "threat_hunt",
                "task_name": norm_hunt_name,
                "datasource_name": norm_hunt_name,
                "description": "Threat Hunting execution for: " + norm_hunt_name,
                "source": "threat_hunt",
                "sourcetype": norm_hunt_name,
                "app_name": "sirens",
                "mitre_tactic": mitre_tactic,
                "mitre_technique": mitre_technique,
                "kill_chain": kill_chain_stage,
                "language": global_inventory['notebook_language'],
                "isDeltaStreaming": is_delta_streaming,
                "schedule": schedule,
                "timezone_id": global_inventory['timezone_id'],
                "scratch_dir": global_inventory['scratch_dir'],
                "target_database": global_inventory['target_database'],
                "whl_path": global_inventory['whl_path'],
                "instance_profile": global_inventory['instance_profile'],
                "cluster": cluster,
                "clusters": global_inventory['clusters'],
                "continuous": "false",
                "notebook_paths": hunting_notebooks
            })

            logger.debug(f"hunting_notebooks: {hunting_notebooks}")

        # Get Threat Intelligence Inventory.
        intel_jobs = Inventory._get_intelligence_inventory(config, global_inventory)
        if intel_jobs:
            [master_inventory.append(intel_job) for intel_job in intel_jobs]
        else:
            logger.info("Threat Intel Collection not configured.")

        logger.debug(f"master_inventory\n{json.dumps(master_inventory, indent=4)}")
        return master_inventory

    @staticmethod
    def _get_intelligence_inventory(config, global_inventory: dict) -> List[Dict]:
        intel_dict = {}
        intel_jobs = []

        conf_dir = GlobalConfig.get_configs_default_dir(config)
        intel_conf_path = os.path.join(conf_dir, 'threat_intelligence', 'intel.yaml')
        intel_config = Inventory._read_yaml_file(str(intel_conf_path))

        for section in config.sections():

            # Only interested in threat_intel stanzas from here out.
            if not section.startswith("threat_intel:"):
                continue

            options = config.options(section)
            if 'enabled' not in options:
                raise SirensConfigException(f'enabled key must exist. stanza: {section} is missing it')

            # skip disabled inputs
            enabled = config.get(section, "enabled").lower()
            if enabled != 'true' and enabled != '1':
                continue

            feed_name = section[13:]

            # grab the relevant config for this feed.
            feed_config = None
            for feed in intel_config.get('feeds'):
                if feed['source_name'] == feed_name:
                    feed_config = feed
                    break

            if not feed_config:
                logger.warning("Cannot find config for {feed_name} - ignoring.")
                continue

            # See if job cluster or interactive cluster has been defined.
            # This should prioritize a new job cluster over an interactive (already exists cluster).
            cluster = config.get(section, "jobcluster", fallback=None)
            if not cluster:
                cluster = config.get(section, "cluster", fallback=None)

            # schedule
            schedule = config.get(section, "schedule", fallback=None)
            if not schedule:
                logger.warning("No schedule set for {feed_name} - ignoring")
                continue

            # get ingestor_notebook
            ingestor_notebook = feed_config.get("ingest_notebook", None)
            if ingestor_notebook:
                #ingestor_notebook = os.path.join(GlobalOpts._get_deploy_dir(config), 'intel', 'dlt', ingestor_notebook)
                ingestor_notebook = os.path.join(GlobalConfig.get_deploy_dir(config), 'intel', 'jobs',
                                                 ingestor_notebook)

            option_args = {}
            option_args['source_name'] = feed_config.get('source_name')
            if feed_config.get('collector').get('options'):
                option_args.update(feed_config.get('collector').get('options'))

            # Get description for workflow
            if 'simple_collector' in feed_config.get('collector').get('name'):
                description = "Download threat intelligence feeds using generic collector"
            else:
                description = "Download threat intelligence feed: " + feed_config.get('source_name')

            intel_dict = {
                "task_type": "threat_intel",
                "task_name": 'intel_collector',
                "datasource_name": feed_config.get('source_name'),
                "description": description,
                "source": "intel",
                "sourcetype": feed_config.get('collector').get('name'),
                "app_name": "sirens",
                "language": global_inventory['notebook_language'],
                "isDeltaStreaming": True,
                "schedule": schedule,
                "timezone_id": global_inventory['timezone_id'],
                "scratch_dir": global_inventory['scratch_dir'],
                "target_database": global_inventory['target_database'],
                "whl_path": global_inventory['whl_path'],
                "instance_profile": global_inventory['instance_profile'],
                "cluster": cluster,
                "clusters": global_inventory['clusters'],
                "edition": "advanced",
                "channel": "current",
                "photon": "true",
                "continuous": "false",
                "notebook_paths": [
                    {'name': Inventory._get_intel_notebook_paths(feed_config.get('collector').get('name'),
                                                                 global_inventory['deploy_dir']),
                     'options': option_args,
                     'context': feed_config.get('context'),
                     'type': 'collector'}
                ]
            }
            if ingestor_notebook:
                intel_dict["notebook_paths"].append(
                    {'name': Inventory._get_intel_notebook_paths(feed_config.get('ingest_notebook'),
                                                                 global_inventory['deploy_dir']),
                     'type': 'ingest'})

            intel_jobs.append(intel_dict)

        return intel_jobs


class Jobs:
    """creates the json needed to push to terraform for the jobs resource
    """

    def __init__(self):
        pass

    @staticmethod
    def _get_nb_target_paths(notebook_paths: list) -> Tuple[str, str, str, str]:
        """make target nb filename

        :param notebook_paths: task['notebook_paths']
        :type notebook_paths: list
        :param target_dir: from global config
        :type target_dir: str
        :return: ingest, parse, normalize notebook names w/ target directory.
        :rtype: _type_
        """
        logger.debug("making notebook target paths")
        ingest_nb, parse_nb, normalize_nb, aggregate_nb = None, None, None, None
        # TODO: recheck logic here.
        for notebook in notebook_paths:
            if notebook.get("ingest"):
                ingest_nb = notebook['ingest']
            if notebook.get("parse"):
                parse_nb = notebook['parse']
            if notebook.get("normalize"):
                normalize_nb = notebook['normalize']
            if notebook.get("aggregate"):
                aggregate_nb = notebook['aggregate']

        return ingest_nb, parse_nb, normalize_nb, aggregate_nb

    @staticmethod
    def _get_storage_location(scratch_dir) -> str:
        """create the location where DLT runs will store info

        :param scratch_dir: scratch_dir configured in sirens.config
        :type scratch_dir: str
        :return: scratch_dir + "/dlt_runs"
        :rtype: str
        """
        if not scratch_dir.startswith('/'):
            scratch_dir = '/' + scratch_dir

        return scratch_dir + "/dlt_runs"

    @staticmethod
    def _get_cluster_key(task: Dict) -> str:
        """generate a cluster key from the input: stanza, or add from any default clusters. Prioritizes job
           clusters over interactive in the case of defaults. If no default configured returns last resport hardcoded keys

        :param task: current datasource input
        :type task: Dict
        :raises Exception: _description_
        :return: job_cluster
        :rtype: str
        """
        # If cluster not specified in input: then use the default.
        # prioritize any job clusters over interactive already in existence ones.
        try:
            if task['cluster'] is None:
                found = False
                # look for the default job cluster.
                for cluster in task['clusters']:
                    if not cluster['name'].startswith("jobcluster:"):
                        continue

                    if cluster['default'] and cluster['cluster_name']:
                        job_cluster_key = cluster['cluster_name']
                        found = True
                        break

                    elif cluster['default'] and not cluster['cluster_name']:
                        job_cluster_key = task['task_name']
                        found = True
                        break

                if not found:
                    # look at interactive clusters for a default
                    for cluster in task['clusters']:
                        if cluster['default'] and cluster['name'].startswith("cluster:"):
                            job_cluster_key = cluster['name']

            else:
                job_cluster_key = task['cluster']
        except Exception as exc:
            raise Exception({exc})

        return job_cluster_key

    @staticmethod
    def _get_default_cluster(type: Literal['job', 'interactive'], clusters: list) -> dict:
        """gets the first default cluster (type dependant) in sirens.config

        :param type: job or interactive
        :type type: Literal[&#39;job&#39;, &#39;interactive&#39;]
        :param clusters: list of clusters from master inventory
        :type clusters: list
        :raises SirensException: any error
        :return: dictionary for the default cluster
        :rtype: dict
        """
        default_cluster = {}
        if type != "job" and type != "interactive":
            raise SirensException("incorrect cluster type. (job or interactive)")

        for cluster in clusters:
            if type == "interactive" and cluster['name'].lower().startswith("cluster:"):
                if "true" in cluster['default'].lower():
                    default_cluster = cluster
                    break
            if type == "job" and cluster['name'].lower().startswith("jobcluster:"):
                if "true" in cluster['default'].lower():
                    default_cluster = cluster
                    break

        return default_cluster

    @staticmethod
    def _make_ingest_tasks_json(task, cluster_attributes) -> Tuple[list, dict]:
        ingest_nb, parse_nb, normalize_nb, aggregate_nb = Jobs._get_nb_target_paths(task['notebook_paths'])
        nbs = [{"task_key": "ingest", "notebook_path": ingest_nb},
               {"task_key": "parse", "notebook_path": parse_nb},
               {"task_key": "normalize", "notebook_path": normalize_nb},
               ]
        task_var = []
        [task_var.append({"task_key": nb.get("task_key"),
                          "job_cluster_key": task['job_cluster_key'],
                          "existing_cluster_id": cluster_attributes['cluster_id'],
                          "notebook_path": nb.get("notebook_path"),
                          "task_params": {"source": task["source"],
                                          "sourcetype": task["sourcetype"],
                                          "database": task['target_database']}
                          }) for nb in nbs]

        # add depends key **only if running on a schedule
        if task['schedule'] != "continuous":

            for itm, nb in enumerate(task_var):
                if nb.get("task_key") == "parse":
                    task_var[itm].update({"depends": ["ingest"]})
                if nb.get("task_key") == "normalize":
                    task_var[itm].update({"depends": ["parse"]})

        if aggregate_nb:
            task_var.append({"task_key": "aggregate",
                             "depends": ["normalize"],
                             "job_cluster_key": task['job_cluster_key'],
                             "existing_cluster_id": cluster_attributes['cluster_id'],
                             "notebook_path": aggregate_nb,
                             "task_params": {"source": task["source"],
                                             "sourcetype": task["sourcetype"],
                                             "database": task['target_database']}
                             })
        tags = {
            "app": task['app_name'],
            "source": task["source"],
            "sourcetype": task["sourcetype"],
            "product": "delta",
            "type": task['task_type'],
        }

        return task_var, tags

    @staticmethod
    def _make_detections_tasks_json(task, cluster_attributes) -> Tuple[list, dict]:
        notebook_path = task['notebook_paths'][0].get("path")
        nbs = [{"task_key": "detection", "notebook_path": notebook_path},
               ]
        task_var = []
        [task_var.append({"task_key": nb.get("task_key"),
                          "job_cluster_key": task['job_cluster_key'],
                          "existing_cluster_id": cluster_attributes['cluster_id'],
                          "notebook_path": nb.get("notebook_path"),
                          "task_params": {
                              "input_table": task["input_table"],
                              "database": task['target_database']}
                          }) for nb in nbs]

        tags = {
            "app": task['app_name'],
            "product": "delta",
            "type": task['task_type'],
        }

        return task_var, tags

    @staticmethod
    def _make_alerts_tasks_json(task, cluster_attributes) -> Tuple[list, dict]:
        alert_notebook_path = task['notebook_paths'][0].get("path")
        nbs = [{"task_key": "alert", "notebook_path": alert_notebook_path},
               ]
        task_var = []
        [task_var.append({"task_key": nb.get("task_key"),
                          "job_cluster_key": task['job_cluster_key'],
                          "existing_cluster_id": cluster_attributes['cluster_id'],
                          "notebook_path": nb.get("notebook_path"),
                          "task_params": {
                                          "input_table": task["input_table"],
                                          "database": task['target_database']}
                          }) for nb in nbs]


        tags = {
            "app": task['app_name'],
            "product": "delta",
            "type": task['task_type'],
        }

        return task_var, tags

    @staticmethod
    def _make_hunt_tasks_json(task, cluster_attributes) -> Tuple[list, dict]:
        last_notebook = None
        task_var = []
        for nb in task['notebook_paths']:
            task_dict = {
                "task_key": nb.get("name"),
                "job_cluster_key": task['job_cluster_key'],
                "existing_cluster_id": cluster_attributes['cluster_id'],
                "notebook_path": nb.get("path"),
                "task_params": nb.get("task_params")
            }
            if last_notebook:
                task_dict['depends'] = [last_notebook]

            tags = {"app": task['app_name'],
                    "type": task['task_type'],
                    "mitre_tactic": task.get('mitre_tactic'),
                    "mitre_technique": task.get('mitre_technique'),
                    "kill_chain": task.get('kill_chain')
                    }
            task_var.append(task_dict)
            last_notebook = nb.get("name")

        return task_var, tags

    def _make_maintenance_task_json(self, jobs: list[dict]) -> dict:
        """Create the maintenance tfvars json structure from master_inventory

        :param jobs: master_inventory
        :type jobs: list
        :return: maintenance_tasks json
        :rtype: dict
        """
        aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
        data = {}
        default_cluster = {}
        data['maintenance_jobs'] = []
        task_defs = []
        task_level_data = {}
        count = 1

        for task in jobs:
            if task['task_name'] != "Maintenance":
                continue

            # get default cluster
            if not default_cluster:
                default_cluster = self._get_default_cluster("job", task['clusters'])
                if default_cluster:
                    # update the target cluster with the default.
                    task['cluster'] = default_cluster['name']
                else:
                    # if no default configured, resort to a basic cluster without attributes/spark_conf etc.
                    # success may vary here if a cluster does not have access to tables.
                    task['cluster'] = None

                # get any associated cluster attributes.
                cluster_attributes = self._get_cluster_opts(task)
                if not cluster_attributes['cluster_id']:
                    uses_existing_cluster = False
                else:
                    uses_existing_cluster = True

                # get cluster attributes if a specific cluster is defined in the input.
                aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
                if task['cluster']:
                    aws_attributes, azure_attributes, gcp_attributes = self._set_csp_attrs(
                        cluster_attributes['cloud_provider'],
                        cluster_attributes['compute_attributes'])

                # add cluster options to the cluster dictionary.
                cluster_options = {"cluster_key": "maintenance", "runtime_engine": cluster_attributes['runtime_engine']}
                cluster_options.update({"autoscale": cluster_attributes['autoscale']})

            # if first grab the task level data to apply all tasks to.
            if count == 1:
                task_level_data = {
                    "task_name": 'Maintenance',
                    "datasource_name": 'Maintenance',
                    "description": task["description"],
                    "language": task['language'],
                    "isDeltaStreaming": task['isDeltaStreaming'],
                    "aws_attributes": aws_attributes,
                    "azure_attributes": azure_attributes,
                    "gcp_attributes": gcp_attributes,
                    "whl_path": task['whl_path'],
                    "spark_conf": cluster_attributes['spark_options'],
                    "uses_existing_cluster": uses_existing_cluster,
                    "tags": {task['app_name']: "sirens"},
                    "cluster": cluster_options,
                    "schedule": {"quartz_cron_expression": "0 0 13 * * ?", "timezone_id": task['timezone_id']},
                }

            # accumulate tasks into a single list of dicts.
            notebooks = [*task['notebook_paths'][0].values()]

            for notebook_path in notebooks:
                task_defs.append({"task_key": task['source'] + '_' + task['sourcetype'] + '_maintenance',
                                  "job_cluster_key": "maintenance",
                                  "notebook_path": str(notebook_path),
                                  "task_params": {"source": task["source"],
                                                  "sourcetype": task["sourcetype"],
                                                  "database": task['target_database']}
                                  }),
            count += 1

        # create the return data
        data['maintenance_jobs'].append(task_level_data)
        data['maintenance_jobs'][0].update({"tasks": task_defs})

        return data

    def _make_jobs_json(self, jobs: list[dict]) -> dict:
        """Generate JSON for jobs

        :param jobs: master_inventory
        :type jobs: list
        :return: json object for terraform
        :rtype: dict
        """
        logger.debug("creating jobs dict")
        data = {"jobs": []}

        # Work through each datasource generating dict for terraform.
        for task in jobs:
            # Skip the tasks (dealt with later)
            if (task['task_name'] == "Setup" or task['task_name'] == "Maintenance" or
                task['task_type'] == 'threat_intel'):
                continue

            # skip DLT jobs
            if task['isDeltaStreaming'] is False:
                continue

            # get cluster attributes. (spark_opts/autoscaling/csp attributes etc)
            cluster_attributes = self._get_cluster_opts(task)

            if not cluster_attributes['cluster_id']:
                uses_existing_cluster = False
            else:
                uses_existing_cluster = True

            # get cluster attributes if a specific cluster is defined in the input.
            aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
            if task['cluster']:
                aws_attributes, azure_attributes, gcp_attributes = self._set_csp_attrs(
                    cluster_attributes['cloud_provider'],
                    cluster_attributes['compute_attributes'])

            # get cluster_key
            task['job_cluster_key'] = self._get_cluster_key(task)

            # add cluster options to the cluster dictionary.
            cluster_options = {"cluster_key": task["job_cluster_key"],
                               "runtime_engine": cluster_attributes['runtime_engine']}
            cluster_options.update({"autoscale": cluster_attributes['autoscale']})

            # create jobs for ingest pipelines
            if task['task_type'] == 'ingest':
                # Create target notebook paths
                task_var, tags = Jobs._make_ingest_tasks_json(task, cluster_attributes)

            # create jobs for threat_hunting notebooks.
            if task['task_type'] == 'threat_hunt':
                task_var, tags = Jobs._make_hunt_tasks_json(task, cluster_attributes)

            if task['task_type'] == 'alerts':
                task_var, tags = Jobs._make_alerts_tasks_json(task, cluster_attributes)

            if task['task_type'] == 'detections':
                task_var, tags = Jobs._make_detections_tasks_json(task, cluster_attributes)

            job_dict = {
                "task_name": task['task_name'],
                "description": task['description'],
                "datasource_name": task['datasource_name'],
                "language": task['language'],
                "isDeltaStreaming": task['isDeltaStreaming'],
                "aws_attributes": aws_attributes,
                "azure_attributes": azure_attributes,
                "gcp_attributes": gcp_attributes,
                "spark_conf": cluster_attributes['spark_options'],
                "uses_existing_cluster": uses_existing_cluster,
                "tags": tags,
                "whl_path": task['whl_path'],
                "cluster": cluster_options,
                "tasks": task_var
            }

            if task['schedule'] != 'continuous':
                job_dict['schedule'] = {"quartz_cron_expression": task['schedule'], "timezone_id": task['timezone_id']}

            data['jobs'].append(job_dict)

        return data

    @staticmethod
    def _make_intel_tasks_json(task, cluster_attributes) -> Tuple[list, dict]:
        task_var = []
        simple_collectors = []

        # get collection jobs
        collection_jobs = task.get('notebook_paths')

        # create the task dict & append notebooks to it.
        for x in collection_jobs:

            # Store as dependencies for simple_ingest notebook.
            if 'simple_collector' in x.get('name'):
                simple_collectors.append(x.get('name'))

            if 'simple_ingest' in x.get('name') and x.get('type') == 'ingest':
                simple_ingest_notebook = x.get('name')

            # Set the task name depending on the type: key.
            task_key = re.sub('[^0-9a-zA-Z]+', '_', task['datasource_name'])
            if 'ingest' in x.get('type'):
                task_key = re.sub('[^0-9a-zA-Z]+', '_', task['datasource_name']) + '_ingest'

            # override the job name for the normalization notebooks.
            if task['datasource_name'] == 'intelligence' and 'collector' in x.get("type"):
                task_key = 'intelligence_staging'
            if task['datasource_name'] == 'intelligence' and 'ingest' in x.get("type"):
                task_key = 'intelligence_enrichment'

            # if simple connector being used we only add simple_ingest once & last in the order.
            task_dict = {
                "task_key": task_key,
                "job_cluster_key": task['job_cluster_key'],
                "existing_cluster_id": cluster_attributes['cluster_id'],
                "notebook_path": x.get("name"),
            }

            if x.get("options") or x.get("context"):
                task_dict["task_params"] = {}

            if x.get("options"):
                task_dict["task_params"]["options"] = str(x.get("options"))

            if x.get("context"):
                context = x.get("context")
                tags = context.get("tags")
                if not tags:
                    del context["tags"]

                task_dict["task_params"]["context"] = str(x.get("context"))

            if 'ingest' in x.get('type') and 'intelligence' not in task['datasource_name']:
                task_dict["depends"] = [re.sub('[^0-9a-zA-Z]+', '_', task['datasource_name'])]

            if 'ingest' in x.get('type') and 'intelligence' in task['datasource_name']:
                task_dict["depends"] = ['intelligence_staging']

            if 'ingest' in x.get('type') and 'simple_ingest' in x.get('name'):
                added_ingestor = True

            task_var.append(task_dict)

        # create job tags
        tags = {
            "app": task['app_name'],
            "datasource_name": task["sourcetype"],
            "product": "delta",
            "type": task['task_type'],
        }

        return task_var, tags

    def _make_intel_json(self, master_inventory: list[dict]):
        inventory = list([x for x in master_inventory if x.get('task_type') == 'threat_intel'])
        data = {'intel_jobs': []}
        tasks = []

        # sort the inventory
        grouped_inventory = BaseUtils.group_items(inventory, ["sourcetype", "schedule", "cluster"])
        for _, group in grouped_inventory:
            tasks.append(list(group))

        d = {}
        last_source_type = None
        for jobs in tasks:
            for task in jobs:
                source_type = task.get('sourcetype')

                # get cluster attributes. (spark_opts/autoscaling/csp attributes etc)
                cluster_attributes = self._get_cluster_opts(task)

                if not cluster_attributes.get('cluster_id'):
                    uses_existing_cluster = False
                else:
                    uses_existing_cluster = True

                # get cluster attributes if a specific cluster is defined in the input.
                aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
                if task['cluster']:
                    aws_attributes, azure_attributes, gcp_attributes = self._set_csp_attrs(
                        cluster_attributes['cloud_provider'],
                        cluster_attributes['compute_attributes'])

                # get cluster_key
                task['job_cluster_key'] = self._get_cluster_key(task)

                # add cluster options to the cluster dictionary.
                cluster_options = {"cluster_key": task["job_cluster_key"],
                                   "runtime_engine": cluster_attributes['runtime_engine']}
                cluster_options.update({"autoscale": cluster_attributes['autoscale']})

                # gen dict for the notebooks that run as part of this job
                task_var, tags = self._make_intel_tasks_json(task, cluster_attributes)

                if source_type != last_source_type:
                    # if new sourcetype detected - append the current built up content
                    # to the jobs.
                    if d:
                        data['intel_jobs'].append(d)
                        d = {}

                    # set datasource name to simple if multiple tasks will be set against a job.
                    if len(jobs) > 1:
                        ds_name = source_type
                    else:
                        ds_name = task['datasource_name']

                    # setup & override task name where req'd
                    task_name = 'intel_' + source_type
                    if 'intel_staging' in source_type:
                        task_name = 'intelligence_normalization'

                    # setup & override job description where req'd
                    task_description = task["description"]
                    if 'intelligence' in task["description"]:
                        task_description = 'Collate all threat feeds into staging table, apply enrichments & normalize to single intel table'

                    # start a new dictionary.
                    d = {
                        "task_name": task_name,
                        "datasource_name": ds_name,
                        "description": task_description,
                        "language": task['language'],
                        "isDeltaStreaming": task['isDeltaStreaming'],
                        "aws_attributes": aws_attributes,
                        "azure_attributes": azure_attributes,
                        "gcp_attributes": gcp_attributes,
                        "spark_conf": cluster_attributes['spark_options'],
                        "uses_existing_cluster": uses_existing_cluster,
                        "tags": tags,
                        "whl_path": task['whl_path'],
                        "cluster": cluster_options,
                        "tasks": task_var,
                        "schedule": {"quartz_cron_expression": task['schedule'], "timezone_id": task['timezone_id']},
                    }
                if source_type == last_source_type:
                    # add a new set of notebook information
                    [d['tasks'].append(x) for x in task_var]

                # store last sourcetype
                last_source_type = source_type

        # catch any last write at the end of loop
        if d:
            data['intel_jobs'].append(d)

        count = 0
        for jobs in data['intel_jobs']:
            for task in jobs.get("tasks"):
                if 'simple_ingest' in task.get('notebook_path'):
                    count = count + 1
        # print(f'{count} tasks have simple_ingest')

        found = 1
        depends = []
        for _, jobs in enumerate(data['intel_jobs']):
            if 'intel_simple_collector' not in jobs.get('task_name'):
                continue

            for itm, task in enumerate(jobs.get("tasks")):
                if 'simple_ingest' in task.get('notebook_path'):
                    if found < count:
                        depends = depends + task.get("depends")
                        found += 1
                        del jobs.get("tasks")[itm]
                        continue

                    if found == count:
                        current_depends = task.get("depends")[0]
                        new_depends = [current_depends] + depends

                        jobs.get("tasks")[itm].update({'depends': new_depends})
                        jobs.get("tasks")[itm].update({'task_key': 'Simple_Intel_Ingest'})
                        break

        return data

    @staticmethod
    def _make_intel_tasks_json(task, cluster_attributes) -> Tuple[list, dict]:
        task_var = []
        simple_collectors = []

        # get collection jobs
        collection_jobs = task.get('notebook_paths')

        # create the task dict & append notebooks to it.
        for x in collection_jobs:

            # Store as dependencies for simple_ingest notebook.
            if 'simple_collector' in x.get('name'):
                simple_collectors.append(x.get('name'))

            if 'simple_ingest' in x.get('name') and x.get('type') == 'ingest':
                simple_ingest_notebook = x.get('name')

            # Set the task name depending on the type: key.
            task_key = re.sub('[^0-9a-zA-Z]+', '_', task['datasource_name'])
            if 'ingest' in x.get('type'):
                task_key = re.sub('[^0-9a-zA-Z]+', '_', task['datasource_name']) + '_ingest'

            # override the job name for the normalization notebooks.
            if task['datasource_name'] == 'intelligence' and 'collector' in x.get("type"):
                task_key = 'intelligence_staging'
            if task['datasource_name'] == 'intelligence' and 'ingest' in x.get("type"):
                task_key = 'intelligence_enrichment'

            # if simple connector being used we only add simple_ingest once & last in the order.
            task_dict = {
                "task_key": task_key,
                "job_cluster_key": task['job_cluster_key'],
                "existing_cluster_id": cluster_attributes['cluster_id'],
                "notebook_path": x.get("name"),
            }

            if x.get("options") or x.get("context"):
                task_dict["task_params"] = {}

            if x.get("options"):
                task_dict["task_params"]["options"] = str(x.get("options"))

            if x.get("context"):
                context = x.get("context")
                tags = context.get("tags")
                if not tags:
                    del context["tags"]

                task_dict["task_params"]["context"] = str(x.get("context"))

            if 'ingest' in x.get('type') and 'intelligence' not in task['datasource_name']:
                task_dict["depends"] = [re.sub('[^0-9a-zA-Z]+', '_', task['datasource_name'])]

            if 'ingest' in x.get('type') and 'intelligence' in task['datasource_name']:
                task_dict["depends"] = ['intelligence_staging']

            if 'ingest' in x.get('type') and 'simple_ingest' in x.get('name'):
                added_ingestor = True

            task_var.append(task_dict)

        # create job tags
        tags = {
            "app": task['app_name'],
            "datasource_name": task["sourcetype"],
            "product": "delta",
            "type": task['task_type'],
        }

        return task_var, tags

    def _make_intel_json(self, master_inventory: list[dict]):
        inventory = []
        data = {}
        data['intel_jobs'] = []
        tasks = []

        [inventory.append(x) for x in master_inventory if x.get('task_type') == 'threat_intel']
        # sort the inventory
        grouped_inventory = BaseUtils.group_items(inventory, ["sourcetype", "schedule", "cluster"])
        for _, group in grouped_inventory:
            tasks.append(list(group))

        d = {}
        last_sourcetype = None
        for jobs in tasks:
            for task in jobs:
                sourcetype = task.get('sourcetype')

                # get cluster attributes. (spark_opts/autoscaling/csp attributes etc)
                cluster_attributes = self._get_cluster_opts(task)

                if not cluster_attributes.get('cluster_id'):
                    uses_existing_cluster = False
                else:
                    uses_existing_cluster = True

                # get cluster attributes if a specific cluster is defined in the input.
                aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
                if task['cluster']:
                    aws_attributes, azure_attributes, gcp_attributes = self._set_csp_attrs(
                        cluster_attributes['cloud_provider'],
                        cluster_attributes['compute_attributes'])

                # get cluster_key
                task['job_cluster_key'] = self._get_cluster_key(task)

                # add cluster options to the cluster dictionary.
                cluster_options = {"cluster_key": task["job_cluster_key"],
                                   "runtime_engine": cluster_attributes['runtime_engine']}
                cluster_options.update({"autoscale": cluster_attributes['autoscale']})

                # gen dict for the notebooks that run as part of this job
                task_var, tags = self._make_intel_tasks_json(task, cluster_attributes)

                if sourcetype != last_sourcetype:
                    # if new sourcetype detected - append the current built up content
                    # to the jobs.
                    if d:
                        data['intel_jobs'].append(d)
                        d = {}

                    # set datasource name to simple if multiple tasks will be set against a job.
                    if len(jobs) > 1:
                        ds_name = sourcetype
                    else:
                        ds_name = task['datasource_name']

                    # setup & override task name where req'd
                    task_name = 'intel_' + sourcetype
                    if 'intel_staging' in sourcetype:
                        task_name = 'intelligence_normalization'

                    # setup & override job description where req'd
                    task_description = task["description"]
                    if 'intelligence' in task["description"]:
                        task_description = 'Collate all threat feeds into staging table, apply enrichments & normalize to single intel table'

                    # start a new dictionary.
                    d = {
                        "task_name": task_name,
                        "datasource_name": ds_name,
                        "description": task_description,
                        "language": task['language'],
                        "isDeltaStreaming": task['isDeltaStreaming'],
                        "aws_attributes": aws_attributes,
                        "azure_attributes": azure_attributes,
                        "gcp_attributes": gcp_attributes,
                        "spark_conf": cluster_attributes['spark_options'],
                        "uses_existing_cluster": uses_existing_cluster,
                        "tags": tags,
                        "whl_path": task['whl_path'],
                        "cluster": cluster_options,
                        "tasks": task_var,
                        "schedule": {"quartz_cron_expression": task['schedule'], "timezone_id": task['timezone_id']},
                    }
                if sourcetype == last_sourcetype:
                    # add a new set of notebook information
                    [d['tasks'].append(x) for x in task_var]

                # store last sourcetype
                last_sourcetype = sourcetype

        # catch any last write at the end of loop
        if d:
            data['intel_jobs'].append(d)

        count = 0
        for jobs in data['intel_jobs']:
            for task in jobs.get("tasks"):
                if 'simple_ingest' in task.get('notebook_path'):
                    count = count + 1
        # print(f'{count} tasks have simple_ingest')

        found = 1
        depends = []
        for _, jobs in enumerate(data['intel_jobs']):
            if 'intel_simple_collector' not in jobs.get('task_name'):
                continue

            for itm, task in enumerate(jobs.get("tasks")):
                if 'simple_ingest' in task.get('notebook_path'):
                    if found < count:
                        depends = depends + task.get("depends")
                        found += 1
                        del jobs.get("tasks")[itm]
                        continue

                    if found == count:
                        current_depends = task.get("depends")[0]
                        new_depends = [current_depends] + depends

                        jobs.get("tasks")[itm].update({'depends': new_depends})
                        jobs.get("tasks")[itm].update({'task_key': 'Simple_Intel_Ingest'})
                        break

        return data

    @staticmethod
    def _get_cluster_opts(task: dict) -> Dict:
        """get attributes req'd for creating/running a cluster for this job

        :param task: current datasource input
        :type task: list[dict]
        :raises SirensConfigException: for errors
        :return: dict of compute_attributes
        :rtype: Dict
        """
        compute_attributes, spark_options, autoscale, cluster_options = {}, {}, {}, {}
        cloud_provider, cluster_id = None, None

        target_cluster = task.get('cluster')
        # if no cluster specified, get the default cluster, and see its existing or not.
        if not target_cluster:
            # find interactive clusters first.
            try:
                for cluster in task.get('clusters'):
                    if cluster['name'].startswith("cluster:"):
                        default = cluster.get("default")
                        if default:
                            cluster_id = cluster.get("cluster_id")
                            break

                # override or set with jobcluster as the default if it exists.
                for cluster in task.get('clusters'):
                    if cluster['name'].startswith("jobcluster:"):
                        if cluster.get("default"):
                            cluster_id = cluster.get("cluster_id")
                            break

                cluster_options = {"cloud_provider": None, "compute_attributes": {},
                                   "spark_options": {"spark.databricks.isv.product": "databricks-sirens"},
                                   "cluster_id": cluster_id,
                                   "autoscale": {"min_workers": "1", "max_workers": "5"},
                                   "runtime_engine": "PHOTON"}
            except Exception as exc:
                raise SirensConfigException(f"unable to read cluster information: {exc}\n\n{task}")

            return cluster_options

        # if a specific cluster has been defined in the input, get its options.
        cluster_def_found = False
        for cluster in task['clusters']:
            # found the target cluster in clusters list.
            if target_cluster in cluster['name']:
                spark_options = cluster.get('spark_options', dict())
                spark_options["spark.databricks.isv.product"] = "databricks-sirens"
                compute_attributes = cluster.get('compute_attributes', dict())
                cloud_provider = cluster.get('cloud_provider', None)
                cluster_id = cluster.get("cluster_id", None)
                autoscale = cluster.get('autoscale', dict())
                runtime = "PHOTON"
                cluster_def_found = True
                break

        if not cluster_def_found:
            logger.error(f"cluster definition stanza: {target_cluster} not found for task: {task['task_name']}")
            raise SirensConfigException(
                f"cluster definition stanza: {target_cluster} not found for task: {task['task_name']}")
        else:
            cluster_options = {"cloud_provider": cloud_provider, "compute_attributes": compute_attributes,
                               "spark_options": spark_options, "cluster_id": cluster_id,
                               "autoscale": autoscale, "runtime_engine": runtime}

        return cluster_options

    @staticmethod
    def _set_csp_attrs(cloud_provider: Literal["aws", "azure", "gcp"],
                       compute_attributes: dict) -> Tuple[dict, dict, dict]:
        """create cloud service provider dicts like instance_profile, availability etc

        :param cloud_provider: one of aws, gcp, azure
        :type cloud_provider: Literal[&quot;aws&quot;, &quot;azure&quot;, &quot;gcp&quot;]
        :param compute_attributes: attributes from sirens.config
        :type compute_attributes: dict
        :return: dicts for each provider - 2 empty, 1 with data for the cloud_provider given
        :rtype: tuple[dict, dict, dict]
        """
        aws_attrs, azure_attrs, gcp_attrs = {}, {}, {}

        if cloud_provider == "aws":
            aws_attrs = compute_attributes
        elif cloud_provider == "azure":
            azure_attrs = compute_attributes
        elif cloud_provider == "gcp":
            gcp_attrs = compute_attributes

        return aws_attrs, azure_attrs, gcp_attrs

    def _make_dlt_intel_jobs_json(self, jobs: list) -> Dict:
        data = {"dlt_intel_jobs": []}
        intel_ingest_notebooks = list(set([x.get('ingestor_notebook')
                                           for x in jobs if
                                           'threat_intel' in x['task_type'] and 'ingestor_notebook' in x.keys()]))

        # append enrichment & ingest notebooks
        config = GlobalConfig.read()
        deploy_to = GlobalConfig.get_deploy_dir(config)
        ingest_nb = os.path.join(deploy_to, "intel", "dlt", "intel_ingest")
        enrich_nb = os.path.join(deploy_to, "intel", "dlt", "intel_enrichment")
        intel_ingest_notebooks.extend([ingest_nb, enrich_nb])

        first_intel_index = next((index for (index, x) in enumerate(jobs) if x['task_type'] == "threat_intel"), False)
        if not isinstance(first_intel_index, int):
            logger.debug("master inventory does not include threat_intel jobs - please check")
            raise SirensConfigException("master inventory does not include threat_intel jobs - please check")

        # Grab the first record to use for the dlt job spec.
        task = jobs[first_intel_index]

        module = get_modules()

        target_database = Schemas.get_schema(module=module.THREAT_INTEL).name

        cluster_attributes = self._get_cluster_opts(task)

        # get cluster attributes if a specific cluster is defined in the input.
        aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
        if task['cluster']:
            aws_attributes, azure_attributes, gcp_attributes = self._set_csp_attrs(cluster_attributes['cloud_provider'],
                                                                                   cluster_attributes[
                                                                                       'compute_attributes'])
        # add cluster options to the cluster dictionary.
        cluster_options = {"label": "default", "custom_tags": {"cluster_type": "sirens"}}
        cluster_options.update({"autoscale": cluster_attributes['autoscale']})
        cluster_options['autoscale'].update({"mode": "ENHANCED"})

        data["dlt_intel_jobs"].append({
            "task_name": "intel_normalizer",
            "storage_location": self._get_storage_location(task['scratch_dir']),
            "datasource_name": "intel_normalizer",
            "target_database": target_database,
            "edition": task['edition'],
            "channel": task['channel'],
            "photon": task['photon'],
            "continuous": task['continuous'],
            "isDeltaStreaming": False,
            "aws_attributes": aws_attributes,
            "azure_attributes": azure_attributes,
            "gcp_attributes": gcp_attributes,
            "whl_path": task['whl_path'],
            "spark_conf": cluster_attributes['spark_options'],
            "tags": {
                "app": task['app_name'],
                "source": task["source"],
                "sourcetype": "intel_normalizer",
                "product": "dlt",
                "type": "threat_intel"
            },
            "notebook_libraries": intel_ingest_notebooks,
            "configuration": {
                "input": "'\\[{'database': " + target_database + \
                         ", 'source': " + task["source"] + \
                         ",'sourcetype': " + "intel_normalizer" + "}\\]'",
                "pipeline_refresh": True
            },
            "schedule": {"quartz_cron_expression": task['schedule'], "timezone_id": task['timezone_id']},
            "cluster": [cluster_options]
        })

        return data

    def _make_dlt_jobs_json(self, jobs: list[dict]) -> Dict:
        """create tfvars json for dlt jobs

        :param jobs: master_inventory
        :type jobs: list
        :return: json to be written to tfvars.json file
        :rtype: Dict
        """
        logger.debug("creating dlt jobs dict")
        data = {"dlt_jobs": []}
        for task in jobs:

            # Skip the setup task (dealt with later)
            if task['task_name'] == "Setup" or task['task_name'] == "Maintenance":
                continue

            # skip Delta Streaming jobs
            if task['isDeltaStreaming'] is True:
                continue

            # print(json.dumps(task, indent=4))
            cluster_attributes = self._get_cluster_opts(task)

            # get cluster attributes if a specific cluster is defined in the input.
            aws_attributes, azure_attributes, gcp_attributes = {}, {}, {}
            if task['cluster']:
                aws_attributes, azure_attributes, gcp_attributes = self._set_csp_attrs(
                    cluster_attributes['cloud_provider'],
                    cluster_attributes['compute_attributes'])
            # add cluster options to the cluster dictionary.
            cluster_options = {"label": "default", "custom_tags": {"cluster_type": "sirens"}}
            cluster_options.update({"autoscale": cluster_attributes['autoscale']})
            cluster_options['autoscale'].update({"mode": "ENHANCED"})

            data["dlt_jobs"].append({
                "task_name": task['task_name'],
                "storage_location": self._get_storage_location(task['scratch_dir']),
                "datasource_name": task['datasource_name'],
                "target_database": task['target_database'],
                "edition": task['edition'],
                "channel": task['channel'],
                "photon": task['photon'],
                "continuous": task['continuous'],
                "isDeltaStreaming": task['isDeltaStreaming'],
                "aws_attributes": aws_attributes,
                "azure_attributes": azure_attributes,
                "gcp_attributes": gcp_attributes,
                "whl_path": task['whl_path'],
                "spark_conf": cluster_attributes['spark_options'],
                "tags": {
                    "app": task['app_name'],
                    "source": task["source"],
                    "sourcetype": task["sourcetype"],
                    "product": "dlt",
                    "type": "ingest"
                },
                "notebook_libraries": [os.path.splitext(i[k])[0] for i in task['notebook_paths'] for k in i],
                "configuration": {
                    "input": "'\\[{'database': " + task['target_database'] + \
                             ", 'source': " + task["source"] + \
                             ",'sourcetype': " + task["sourcetype"] + "}\\]'",
                    "pipeline_refresh": True
                },
                "schedule": {"quartz_cron_expression": task['schedule'], "timezone_id": task['timezone_id']},
                "cluster": [cluster_options]
            })

        return data

    def create_job_tf(self, master_inventory: list[dict]) -> dict:
        """Generate the jobs dict

        :param master_inventory: dict of global config
        :type master_inventory: dict
        :return: JSON for jobs.tfvars.json
        :rtype: dict
        """
        logger.debug("creating tfvars for jobs")
        jobs_data = self._make_jobs_json(master_inventory)
        return jobs_data

    def create_intel_tf(self, master_inventory: list[dict]) -> dict:
        logger.debug("creating intel.tfvars.json")
        # print(json.dumps(master_inventory, indent=2))
        intel_data = self._make_intel_json(master_inventory)
        return intel_data

    def create_dlt_intel_tf(self, master_inventory: list[dict]) -> dict:
        logger.debug("creating dlt_intel_jobstfvars.json")
        dlt_intel_jobs = self._make_dlt_intel_jobs_json(master_inventory)
        return dlt_intel_jobs

    def create_dlt_job_tf(self, master_inventory: list[dict]) -> dict:
        """create json for dlt jobs to be written to a tfvars.json file

        :param master_inventory: dict of inventory
        :type master_inventory: dict
        :return: dlt vars for terraform
        :rtype: dict
        """
        logger.debug("creating dlt tfvars")
        dlt_job_data = self._make_dlt_jobs_json(master_inventory)
        return dlt_job_data

    def create_maintenance_tasks_tf(self, master_inventory: list[dict]) -> dict:
        """create json for maintenance jobs to be written to a tfvars.json file

        :param master_inventory: dict of inventory
        :type master_inventory: dict
        :return: maintenance vars for terraform
        :rtype: dict
        """
        logger.debug("creating maintenance notebooks tfvars")
        maintenance_tasks_data = self._make_maintenance_task_json(master_inventory)
        return maintenance_tasks_data


class Repos:

    def __init__(self, config: configparser.ConfigParser):
        self._config = config
        # self.databricks_host = self._get_input("DATABRICKS_HOST", required = True)
        # self.databricks_token = self._get_input("DATABRICKS_TOKEN", required = True)
        self.git_url = self._get_input("git_url", required=True)
        self.git_username = self._get_input("git_username", required=True)
        self.git_pat_token = self._get_input("git_pat_token", required=True)
        self.git_provider = self._get_input("git_provider", fallback='gitHub')
        self.git_repo_name = self._get_input("git_repo_name", fallback=os.path.basename(self.git_url))

        read_profile_name = get_bool_value(self._get_input("read_profile_name", fallback='True'))
        self.profile_name = ''
        if read_profile_name:
            self.profile_name = self._get_input("profile_name", fallback='')
        self.git_branch = Repos._get_current_git_branch()

    def _from_conf(self, arg):
        return self._config.get('deploy', arg, fallback=None)

    @staticmethod
    def _from_env(arg):
        if arg == 'DATABRICKS_HOST' or arg == 'DATABRICKS_TOKEN':
            prefix = ''
        else:
            prefix = 'TF_VAR_'
        return os.getenv(prefix + arg) or None

    @staticmethod
    def _from_user(arg, required: bool = False):
        user_input = None
        while not user_input:
            user_input = input(f"{arg} not found in sirens.config or ENV_VAR: Please input manually or <CR> for None: ")
            if not required or (required and user_input):
                break
            print(f'required argument: {arg}')
        return user_input

    def _get_input(self, arg, required: bool = False, fallback: str = None):
        return self._from_conf(arg) or self._from_env(arg) or fallback or self._from_user(arg, required)

    def as_dict(self):
        args = {}
        [args.update({k: v}) for k, v in self.__dict__.items() if not k.startswith('_')]
        print(args)
        return args

    @staticmethod
    def _get_current_git_branch() -> Union[str, None]:
        branch = subprocess.run(['git rev-parse --abbrev-ref HEAD'], capture_output=True, shell=True, text=True)
        return branch.stdout.strip() or None

    def _get_source_code_vars(self) -> dict:

        #TODO #1316 - Add user input request when details are missing

        return self.as_dict()
        # s_code_details = {
        #     "git_url": self.config.get('deploy', 'git_url'),
        #     "git_username": self.config.get('deploy', 'git_username', fallback=''),
        #     "git_pat_token": os.getenv('TF_VAR_git_pat_token', ''),
        #     "git_provider": self.config.get('deploy', 'git_provider', fallback='github'),
        #     "git_repo_name": self.config.get('deploy', 'git_repo_name',
        #                                      fallback=os.path.basename(self.config.get('deploy', 'git_url'))),
        #     "profile_name": self.config.get('deploy', 'profile_name', fallback=''),
        #     "git_branch": Repos._get_current_git_branch()
        # }
        # return s_code_details

    def get_details(self) -> dict:
        s_code_details = self._get_source_code_vars()
        for entry in s_code_details.keys():
            if s_code_details[entry] is None:
                s_code_details[entry] = input(f"{entry}: ")

        return s_code_details


class Dashboards:
    def __init__(self, config: configparser.ConfigParser):
        self.config = config
        pass

    def create_dashboards_tf(self) -> dict:
        dashboards_dict = {
            "sql_endpoint_id": GlobalConfig.get_warehouse_id(self.config),
            "target_database": GlobalConfig.get_target_database(self.config)
        }

        return dashboards_dict
