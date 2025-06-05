import os
import sys

import inquirer
import configparser
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config as DBSDKConfig
from databricks.sdk.errors.platform import PermissionDenied, ResourceAlreadyExists
from databricks.sdk.service.sql import State
from databricks.sdk.service.workspace import ImportFormat

from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.logging import get_logger

logger = get_logger(__name__)

class DeploymentSetup:

    def __init__(self):
        self._init_db_workspace_client()
        self._check_databricks_connection(self.dbHost)

    def _init_db_workspace_client(self):
        questions = [
            inquirer.Text("dbHost", "Enter the hostname of your Databricks workspace"),
            # using bitly because original link was too long and messed up the console
            # TODO: find better solution
            inquirer.Password("dbPat", message="Enter an existing Databricks Personal Access Token (PAT) (https://bit.ly/4j42fyd)")
        ]
        db_conn_answers = inquirer.prompt(questions)

        self.dbHost = db_conn_answers['dbHost']
        dbPat = db_conn_answers['dbPat']

        import os
        os.environ['DATABRICKS_HOST'] = self.dbHost
        os.environ['DATABRICKS_TOKEN'] = dbPat

        # TODO make timeout configurable
        dbSDKConfig = DBSDKConfig(host=self.dbHost, token=dbPat, http_timeout_seconds=5, retry_timeout_seconds=15)
        self.w = WorkspaceClient(config=dbSDKConfig)

        if (os.path.exists('sirens.config')):
            self.config = GlobalConfig._read_config(['sirens.config'])
        else:
            self.config = GlobalConfig._read_config(['sirens.config.default'])

    def generateSirensConfig(self):
        config = self.config

        logger.info("[SQL Config]")
        # Target database
        target_database = inquirer.text("Enter the name of your target database in Databricks (catalog.schema format)",
                                        default=self._get_config(section='default', option='target_database'))

        # SQL Warehouse
        questions = [
            inquirer.List("sql_wh_id_or_list",
                          "Do you want to choose a SQL Warehouse from a list of active warehouses or type in the SQL Warehouse ID manually?",
                          choices=["active list", "manual id"])
        ]
        choice = inquirer.prompt(questions)["sql_wh_id_or_list"]
        if (choice == "active list"):
            sql_warehouse_id = self._get_sql_warehouse_from_list()
        else:
            sql_warehouse_id = inquirer.text("Enter SQL Warehouse ID (e.g. 8ba4ed1f4014912d)",
                                             default=self._get_config(section='deploy', option='sql_warehouse_id'))

        # Git config
        logger.info("[Git Config]")
        questions = [
            inquirer.Text("git_url", "Enter the URL for your Git repo",
                          default=self._get_config(section='deploy', option='git_url')),
            inquirer.Text("git_repo_name", "Enter the name for your Git repo (it will be used in Databricks)",
                          default=self._get_config(section='deploy', option='git_repo_name')),
            inquirer.Text("git_username", "Enter the username for your Git repo",
                          default=self._get_config(section='deploy', option='git_username')),
            # using bitly because original link was too long and messed up the console
            # TODO: find better solution
            inquirer.Password("git_pat_token", message="Enter the Git PAT token (http://bit.ly/4kKQjmQ)",
                              default=self._get_config(section='deploy', option='git_pat_token'))
        ]
        github_answers = inquirer.prompt(questions)
        self.github_repo = github_answers["git_repo_name"]
        #TODO: test Github conn

        # Workspace config
        logger.info("[Workspace Config]")
        default_workspace_name = self._get_default_workspace_name()
        workspace_name = inquirer.text("Enter a name for your workspace", default=default_workspace_name)
        workspace_section = f"workspace:{workspace_name}"
        cloud_service_provider = inquirer.list_input("Enter the cloud service provider for your workspace",
            choices=["aws", "azure", "gcp"],
            default=self._get_config(section=workspace_section, option="cloud_service_provider"))

        # Jobcluster config
        logger.info("[Jobcluster Config]")
        jobcluster_name = inquirer.text("Enter name for your job cluster")
        jobcluster_section = f"jobcluster:{jobcluster_name}"
        questions = [
            inquirer.Text("min_workers",
                          "Enter the minimum number of workers on the cluster", validate=self._validate_num,
                          default=self._get_config(jobcluster_section, "min_workers")),
            inquirer.Text("max_workers",
                          "Enter the maximum number of workers on the cluster", validate=self._validate_num,
                          default=self._get_config(jobcluster_section, "max_workers"))
        ]
        jobcluster_answers = inquirer.prompt(questions)

        # Input sources config
        logger.info("[Input sources config]")
        input_sections = list(filter(lambda s: s.startswith('input:'), config.sections()))
        input_names = sorted(list(map(lambda s: s.replace('input:', ''),  input_sections)))

        prev_selected_inputs = []
        for input_section in input_sections:
            if self.config.get(input_section, "enabled") == "true":
                prev_selected_inputs.append(input_section)
        prev_input_names = sorted(list(map(lambda s: s.replace('input:', ''), prev_selected_inputs)))

        selected_inputs = inquirer.checkbox("Select input sources you want to enable (use space bar)",
                                            choices=input_names, default=prev_input_names)

        # Generate sirens.config
        config.set(section='default', option='target_database', value=target_database)

        config.set(section='deploy', option='read_profile_name', value='False')
        config.set(section='deploy', option='sql_warehouse_id', value=sql_warehouse_id)
        config.set(section='deploy', option='git_url', value=github_answers["git_url"])
        config.set(section='deploy', option='git_repo_name', value=github_answers["git_repo_name"])
        config.set(section='deploy', option='git_username', value=github_answers["git_username"])
        config.set(section='deploy', option='git_pat_token', value=github_answers["git_pat_token"])

        self._add_config_section(workspace_section)
        config.set(workspace_section, option="hostname", value=self.dbHost.replace('https://',''))
        config.set(workspace_section, option="cloud_service_provider", value=cloud_service_provider)

        self._add_config_section(jobcluster_section)
        config.set(jobcluster_section, option='workspace', value=workspace_name)
        config.set(jobcluster_section, option='min_workers', value=jobcluster_answers['min_workers'])
        config.set(jobcluster_section, option='max_workers', value=jobcluster_answers['max_workers'])
        config.set(jobcluster_section, option='default', value='true') #TODO -- is this one needed?

        for input in selected_inputs:
            config.set(f"input:{input}", option='enabled', value='true')

            #TODO: customize path, etc in inputs.yaml

        with open('sirens.config', 'w') as f:
            config.write(f)

        logger.info("Wrote config to sirens.config")

        return config

    def save_deploy_files_to_repo(self):
        # commit and push generated notebooks to your Git repo
        os.system("git add deploy -f")
        os.system("git commit -m 'adding generated notebooks'") #TODO: better commit msg
        github_branch = os.popen("git branch --show-current").read().strip()
        os.system(f"git push --set-upstream origin {github_branch}")

    def upload_wheel(self):
        logger.info("Uploading wheel file to Databricks...")

        wheel_name = os.listdir('dist/')[0]

        self.github_repo = self.config.get(section='deploy', option='git_repo_name')
        lib_dir = f"/Workspace/Repos/{self.w.current_user.me().user_name}/{self.github_repo}/lib"
        self.w.workspace.mkdirs(lib_dir)

        remote_path = f"{lib_dir}/{wheel_name}"
        with open(f"dist/{wheel_name}", "rb") as wheel:
            try:
                self.w.workspace.upload(remote_path, wheel, format=ImportFormat.RAW)
                logger.info("Upload successful.")
            except ResourceAlreadyExists:
                resp = inquirer.list_input("Wheel file already exists. Overwrite?", choices=["yes", "no"])
                if (resp == "yes"):
                    wheel.seek(0)
                    self.w.workspace.upload(remote_path, wheel, format=ImportFormat.RAW, overwrite=True)
                    logger.info("Upload successful.")

    def _check_databricks_connection(self, dbHost):
        try:
            logger.info("Testing Databricks connection...")
            self.w.current_user.me()
        except TimeoutError:
            logger.error(f"Could not reach Databricks host: {dbHost}")
            sys.exit(1)
        except PermissionDenied as e:
            logger.error(f"Auth error when connecting to Databricks: {e}")
            sys.exit(1)

        logger.info("Databricks connection successful.")

    def _get_sql_warehouse_from_list(self):
        warehouses = filter(lambda wh: wh.state == State.RUNNING, self.w.warehouses.list())
        warehouse_dict = dict(map(lambda wh: (wh.name, wh.id), warehouses))
        warehouse_names = warehouse_dict.keys()

        questions = [ inquirer.List("sql_warehouse", "Select your SQL Warehouse", choices=warehouse_names) ]
        answers = inquirer.prompt(questions)

        sql_warehouse_id = warehouse_dict[answers['sql_warehouse']]
        return sql_warehouse_id

    def _validate_num(self, answers, current):
        return current.isnumeric()

    def _get_default_workspace_name(self):
        import re
        default_workspace_name = re.sub(r'https://([^.]+)\..+$', r'\1', self.dbHost)
        return default_workspace_name

    def _add_config_section(self, section):
        try:
            self.config.add_section(section)
        except configparser.DuplicateSectionError:
            # must have been added before, ignore
            pass

    def _get_config(self, section, option):
        try:
            val = self.config.get(section, option)
            return val
        except (configparser.NoOptionError, configparser.NoSectionError):
            # no previous val was set, ignore
            return ""
