"""User facing API for sirens.py - Implements actions
       available from the command line.

Authors:
    Derek King 11th August 2022
    Julian Shalaby (inc. generate_detections() code) - Dec 2022.

Classes:
    Controller()

Functions:
    generate_notebooks()
    generate_detections()
    deploy_notebooks()
    deploy_detections()
    validate()

"""
import configparser
import logging
import os
import csv
import shutil
import sys
import traceback
from collections import namedtuple
from configparser import NoOptionError
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Dict, List, Optional, Any
import glob
import yaml

from jinja2 import Environment, ChoiceLoader, FileSystemLoader, PackageLoader, StrictUndefined, \
    TemplateNotFound, Template
from yaml.loader import SafeLoader

import deployment_setup
from databricks.sirens import config_reader, deployer, validators
from databricks.sirens._version import ROOT_DIR
from databricks.sirens.datasource import DataSource
from databricks.sirens.exceptions import SirensUserException, SirensConfigException
from databricks.sirens.global_config import GlobalConfig
from databricks.sirens.logging import colours, get_logger
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.utils.path_utils import PathUtils

logger = get_logger(__name__)

stage_nums = {'ingest': '01', 'parse': '02', 'normalize': '03', 'standardize': '04',
              'detection': '05', 'maintenance': '06', 'aggregate': '07'}
extensions = {'python': '.py', 'sql': '.sql'}

NotebookData = namedtuple("NotebookData", field_names=['NotebookTask', 'FileName', 'FileContents'])


class NotebookLanguage(Enum):
    python = 'python'
    sql = 'sql'

    def file_extension(self):
        return extensions[self.value]


class NotebookType(Enum):
    dlt = 'dlt'
    delta = 'delta'


class NotebookStage(Enum):
    ingest = 'ingest'
    parse = 'parse'
    normalize = 'normalize'
    standardize = 'standardize'
    detection = 'detection'
    maintenance = 'maintenance'
    aggregate = 'aggregate'
    alerts = 'alerts'


    def stage_num(self):
        return stage_nums[self.value]


class NotebookOrder(Enum):
    stage = 'stage'
    source = 'source'


def print_error_message(message: str):
    print(f"{colours.ERROR}ERROR: {message}{colours.ENDC}")


def print_warning_message(message: str):
    print(f"{colours.WARN}WARNING: {message}'{colours.ENDC}")


def print_info_message(message: str):
    print(f"{colours.INFO}INFO: {message}{colours.ENDC}")


class Controller:
    """API for controlling the behaviour of the user interface
    """

    # TODO #1314 - Remove duplicated code as new PR - this needs a tidy up
    def __init__(self, config: configparser.ConfigParser) -> None:
        """init for controller

        :param config: the global config file as a dict
        :type config: dict
        """
        self.config = config
        self.template_dir = GlobalConfig.get_template_dir(self.config)
        self.deploy_dir = GlobalConfig.get_deploy_dir(self.config)
        self.hunt_notebooks_dir = GlobalConfig.get_hunt_notebooks_dir(self.config)
        self.playbooks_dir = GlobalConfig.get_playbooks_dir(self.config)
        self.config_dir = 'log_sources'
        self.intel_coll_dir = GlobalConfig.get_intel_collection_nb_dir(self.config)
        self.intel_ingest_dir = GlobalConfig.get_intel_ingest_nb_dir(self.config)
        self.intel_normalize_dir = GlobalConfig.get_intel_normalize_nb_dir(self.config)
        loaders = [
            FileSystemLoader([self.template_dir, self.hunt_notebooks_dir, self.playbooks_dir])]

        try:
            # if package is not installed this will fail
            loaders.append(PackageLoader("databricks.sirens"))
        except Exception as exc:
            # ignore errors, probably means we're running from source
            pass

        self.j2obj = Environment(loader=ChoiceLoader(loaders))

    def _get_template(self, nb_lang: NotebookLanguage, nb_type: NotebookType,
                      stage: str) -> Template:
        """Get the jinja template object

        :param nb_lang: notebook language
        :type nb_lang: NotebookLanguage
        :param nb_type: dlt, or delta
        :type nb_type: NotebookType
        :param stage: ingest, parse, normalize, standardize, aggregate
        :type stage: NotebookStage
        :raises FileNotFoundError: _description_
        :raises Exception: _description_
        :return: jinja object
        :rtype: Template
        """
        try:
            filename = f"{nb_lang.value}_{nb_type.value}_{stage}{nb_lang.file_extension()}.jinja"
            template_file = os.path.join(self.template_dir, filename)
            return self.j2obj.get_template(filename)
        except (FileNotFoundError, Exception) as exc:
            raise SirensUserException(f"_getTemplate Error: {exc}") from exc

    @staticmethod
    def _write_file(file_name: str, mode: str, content: Any) -> bool:
        try:
            with open(file_name, mode) as outfp:
                outfp.write(content)
        except Exception as exc:
            logger.error(exc)

        return True

    def _write_notebook(self, inputs: Optional[Dict], content: str, stage: NotebookStage,
                        nb_lang: NotebookLanguage, order: NotebookOrder = NotebookOrder.stage,
                        no_clobber: bool = False):
        """write out the notebook to the ``deploy`` directory

        :param inputs: input stanza as dict (optional)
        :type inputs: dict
        :param content: template content
        :type content: str
        :param stage: ingest, parse, normalize, standardize, aggregate
        :type stage: NotebookStage
        :param nb_lang: python or sql
        :type nb_lang: NotebookLanguage
        :param order: write notebooks by stage order, or source, sourcetype
        :type order: NotebookOrder
        :raises Exception: on write error
        """
        try:
            nb_type = NotebookType[GlobalConfig.get_notebook_type(self.config)]

            if order == NotebookOrder.stage:
                deploy_to = os.path.join(self.deploy_dir, nb_type.value, stage.value)
            else:
                deploy_to = os.path.join(self.deploy_dir, nb_type.value, inputs.get("source"),
                                         inputs.get("sourcetype"))

            if stage == NotebookStage.standardize:
                # force if dlt aggregation notebook required.
                filename = f"{stage.stage_num()}-{stage.value}{nb_lang.file_extension()}"
            else:
                filename = f"{stage.stage_num()}-{inputs.get('source')}-{inputs.get('sourcetype')}-{stage.value}{nb_lang.file_extension()}"

            os.makedirs(deploy_to, exist_ok=True)
            outfile = os.path.join(deploy_to, filename)

            # skip existing files if --no-clobber set.
            if no_clobber and os.path.exists(outfile):
                logger.debug(f"skipping file: {outfile} --no-clobber is set and file exists.")
                return

            logger.debug(f"writing nb: {outfile}")
            Controller._write_file(outfile, 'w', content)

        except Exception as exc:
            logger.exception(exc)
            raise SirensUserException(f"_write_notebook: {exc}") from exc

    def _write_threat_hunt_notebook(self, hunt_name, notebook, template_content, no_clobber):
        try:
            deploy_to = GlobalConfig.get_deploy_dir(self.config)
            outfile = os.path.join(deploy_to, "threat_hunting", hunt_name, notebook)

            if no_clobber and os.path.exists(outfile):
                logger.debug(f"skipping file: {outfile} --no-clobber set and file exists already")
                return

            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            Controller._write_file(outfile, 'w', template_content)

        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        return True

    def _write_intel_notebook(self, nb_type: str, notebook: str,
                              template_content: Template, no_clobber: bool = False):
        try:
            deploy_to = GlobalConfig.get_deploy_dir(self.config)
            #if 'collector' in nb_type:
            #    outfile = os.path.join(deploy_to, "intel", "jobs", notebook)
            #if 'ingest' in nb_type:
            #    outfile = os.path.join(deploy_to, "intel", "dlt", notebook)
            outfile = os.path.join(deploy_to, "intel", "jobs", notebook)

            if no_clobber and os.path.exists(outfile):
                logger.debug(f"skipping file: {outfile} --no-clobber set and file exists already")
                return True

            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            Controller._write_file(outfile, 'w', template_content)

        except Exception as exc:
            #raise SirensUserException(f"{exc}") from exc
            logger.error(f"error writing notebook: {notebook}: {exc}")

        return True

    def _gen_notebook(self, stage: NotebookStage, nb_lang: NotebookLanguage,
                      nb_type: NotebookType, inputs: Dict, no_clobber: bool = False):
        """write the ingest notebook

        :param stage: stage of ingest (ingest, parse, normalize, aggregate)
        :type stage: str
        :param nb_lang: language to write
        :type nb_lang: str
        :param nb_type: product set: dlt, or delta
        :type nb_type: str
        :param inputs: keys from input stanza
        :type inputs: dict
        :raises Exception: on unknown failure
        """
        try:
            template = self._get_template(nb_lang, nb_type, stage.value)
            tblName = DataSource.get_table_name(inputs.get("source"), inputs.get("sourcetype"))
            groupby = inputs.get("groupby")
            sirens_lib = GlobalConfig.get_global_sirens_lib(self.config)
            template_content = template.render(input_source=inputs, table_name=tblName,
                                               sirens_lib=sirens_lib)
            self._write_notebook(inputs, template_content, stage, nb_lang, groupby, no_clobber)
        except Exception as exc:
            traceback.print_tb(exc.__traceback__)
            raise SirensUserException(f"{stage}: {exc}") from exc

    def _gen_standardize_notebook(self, nb_lang: NotebookLanguage,
                                  nb_type: NotebookType, inputs: List,
                                  no_clobber: bool = False):
        """write the standardize notebook

        :param nb_lang: language to write
        :type nb_lang: str
        :param nb_type: product set: dlt, or delta
        :type nb_type: str
        :param inputs: keys from input stanza
        :type inputs: dict
        :raises Exception: on unknown failure
        """
        try:
            stage = NotebookStage.standardize
            template = self._get_template(nb_lang, nb_type, stage.value)
            sirens_lib = GlobalConfig.get_global_sirens_lib(self.config)
            template_content = template.render(input_source=inputs, sirens_lib=sirens_lib)
            self._write_notebook(None, template_content, stage, nb_lang, no_clobber=no_clobber)
        except Exception as exc:
            raise SirensUserException(f"_genStandardizeNb: {exc}") from exc

    def _gen_detection_notebook(self, nb_lang: NotebookLanguage, nb_type: NotebookType,
                                inputs: Dict, pipeline_name, suffix='detection',
                                no_clobber: bool = False):
        try:
            stage = NotebookStage.detection
            template = self._get_template(nb_lang, nb_type, suffix)
            sirens_lib = GlobalConfig.get_global_sirens_lib(self.config)
            template_content = template.render(sirens_lib=sirens_lib, **inputs)

            output_filename = pipeline_name
            if pipeline_name.endswith('.internal'):
                output_filename = pipeline_name[0:-1 * len('.internal')]
            if output_filename.endswith('_detections') is False and output_filename not in [
                'clustering', 'alerts_writer', 'global_installations']:
                output_filename += '_detections'

            detection_file = f'{stage.value}/{nb_type.value}/'
            if "source" in inputs:
                detection_file += f'{inputs["source"]}/'

            detection_file += f"{output_filename}.py"

            self._write_detection_notebook(template_content, detection_file, no_clobber)
        except Exception as exc:
            raise SirensUserException(f"_genDetectionNotebook: {exc}") from exc

    def _gen_alerts_notebook(self, nb_lang: NotebookLanguage, nb_type: NotebookType,
                                inputs: Dict,
                                suffix='alerts',
                                no_clobber: bool = False):
        try:
            stage = NotebookStage.alerts
            template = self._get_template(nb_lang, nb_type, suffix)
            sirens_lib = GlobalConfig.get_global_sirens_lib(self.config)
            template_content = template.render(sirens_lib=sirens_lib, **inputs)

            alerts_file = f"{stage.value}/{nb_type.value}/{inputs['input_table']}_table.py"

            self._write_alerts_notebook(template_content, alerts_file, no_clobber)
        except Exception as exc:
            raise SirensUserException(f"_genDetectionNotebook: {exc}") from exc

    @staticmethod
    def _search_file_path(conf_type: str, search_path: List[str]) -> List:
        """search for yaml files recursively from search_path

        :param search_path: directory path
        :type search_path: str
        :return: list of yaml files under the search_path provided
        :rtype: List
        """
        files_to_validate = []
        for d in search_path:
            for path in Path(d).rglob(f'{conf_type}.yaml'):
                files_to_validate.append(path)
        return files_to_validate

    # Detection based functions
    def _get_detection_template(self, file: str) -> Template:
        """load detection notebook jinja template

        :param file: file to load
        :type file: str
        :raises FileNotFoundError: If filesystem FileNotFound
        :raises SirensUserException: any other error
        :return: jinja object
        :rtype: Template
        """
        try:
            template_file = os.path.join(self.template_dir, file)
            return self.j2obj.get_template(template_file)
        except (FileNotFoundError, Exception) as exc:
            raise SirensUserException(f"_get_detection_template Error: {exc}") from exc

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

    def _write_detection_notebook(self, template_content: str, detection_file,
                                  no_clobber: bool = False):
        """write detection notebook to disk

        :param template_content: content object
        :type template_content: str
        :param detection_file: file name
        :type detection_file: _type_
        :raises SirensUserException: on any error
        """
        try:
            deploy_to = GlobalConfig.get_detection_deploy_dir(self.config)
            outfile = os.path.join(deploy_to, detection_file)
            if no_clobber and os.path.exists(outfile):
                logger.debug(f"skipping file: {outfile} --no-clobber is set and file exists")
                return

            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            with open(outfile, 'w') as outfp:
                outfp.write(template_content)
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

    def _write_alerts_notebook(self, template_content: str, alerts_file,
                                  no_clobber: bool = False):
        """write alerts notebook to disk

        :param template_content: content object
        :type template_content: str
        :param alerts_file: file name
        :raises SirensUserException: on any error
        """
        try:
            deploy_to = GlobalConfig.get_deploy_dir(self.config)
            outfile = os.path.join(deploy_to, alerts_file)
            if no_clobber and os.path.exists(outfile):
                logger.debug(f"skipping file: {outfile} --no-clobber is set and file exists")
                return

            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            with open(outfile, 'w') as outfp:
                outfp.write(template_content)
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

    @staticmethod
    def _defines_aggregates(input_config_dir: str, source: str, sourcetype: str) -> bool:
        """return True if the inputs.yaml config defines aggregates for the pipeline

        :param input_config_dir: directory for inputs.yaml files - from global config
        :type input_config_dir: str
        :param source: source
        :type source: str
        :param sourcetype: sourcetype
        :type sourcetype: _sre
        :raises SirensConfigException: on error
        :return: _description_
        :rtype: bool
        """

        file_names = PathUtils.get_relative_file_paths(
            [f"{input_config_dir}/{source}/{sourcetype}/inputs.yaml"])
        default = {}
        for file in file_names:
            try:
                with open(file) as f:
                    for data in yaml.load_all(f, Loader=SafeLoader):
                        default.update(data)
            except FileNotFoundError as exc:
                pass

        if not default:
            logger.error("unable to locate config file")
            raise SirensConfigException(
                f"Unable to locate config file: {source}/{sourcetype}/inputs.yaml")

        if default.get("transforms").get("gold"):
            if default.get("transforms").get("gold").get("aggregate"):
                return True

        return False

    def _make_hunt_notebooks(self, hunt_name, notebook, nb_conf, j2obj: Environment):
        filename = notebook + ".py"

        logger.debug(f"notebook: {notebook}, hunt: {hunt_name}")

        # get actual notebook from notebook directory
        try:
            template = j2obj.get_template(filename)
        except TemplateNotFound:
            raise SirensConfigException(
                f"Notebook: {filename} not found. ensure one of {self.hunt_notebooks_dir}, {self.playbooks_dir}")

        template_content = template.render()
        return [hunt_name, filename, template_content]

    @staticmethod
    def _make_intel_notebooks(notebook: dict, j2obj: Environment) -> NotebookData:
        nb_type = notebook.get('type')
        notebook = notebook.get('notebook')
        filename = notebook + ".py"

        try:
            template = j2obj.get_template(filename)
        except TemplateNotFound as exc:
            logger.error(f"Notebook: {filename} not found. Ensure default directory is correct in sirens.config")
            raise exc
        except Exception as exc:
            logger.error(f"error getting template: {exc}")
            raise exc
        try:
            template_content = template.render()
        except Exception as exc:
            logger.error(f"error rendering template: {exc}")
            raise exc

        return NotebookData(nb_type, filename, template_content)

    def _get_hunts_from_global_config(self, hunts: Optional[list]) -> List:
        """return a list of enabled threat_hunt stanzas in sirens.config

        :return: hunt_name(s)
        :rtype: List
        """
        hunts_in_config = []
        for section in self.config.sections():
            if 'threat_hunt:' not in section:
                continue

            # If a list of stanzas specified - process only those inputs.
            if hunts is not None:
                if section[12:] not in hunts:
                    continue
            else:
                try:
                    if self.config.get(section, 'enabled').lower() == 'true':
                        hunts_in_config.append(section[12:])
                except NoOptionError:
                    logger.warning(f'No option: "enabled" in section: {section} - ignoring.')
                    continue

        return hunts_in_config

    def _get_intel_feeds_from_global_config(self, collections: Optional[List] = None) -> List[Dict]:
        """return a list of enabled threat_intel: collections from sirens.config

        :param collections: _description_
        :type collections: Optional[List]
        :return: _description_
        :rtype: List
        """
        feeds = []
        for section in self.config.sections():
            if 'threat_intel:' not in section:
                continue

            # If a list of stanzas specified - process only those inputs.
            if collections is not None:
                if section[13:] not in collections:
                    continue
            else:
                try:
                    if self.config.get(section, 'enabled').lower() == 'true':
                        feeds.append({'source_name': section[13:], 'schedule': self.config.get(section, 'schedule')})
                except NoOptionError:
                    logger.warning(f'No option: "enabled" in section: {section} - ignoring.')
                    continue

        return feeds

    def _get_intel_configuration(self, collections: Optional[List] = None):
        threat_conf, configured_feeds = [], []
        # get the intel feeds from sirens.config and intel yaml conf
        try:
            feeds = self._get_intel_feeds_from_global_config(collections)
            configured_feeds.extend([config_reader.ThreatIntelReader().read(x['source_name']) for x in feeds])

            # add only records found in yaml file (as .read may return empty list if not found)
            threat_conf.extend(x for x in configured_feeds if x)
        except Exception as exc:
            logger.error(f'error reading config: {exc}')
            return False

        return threat_conf

    def _generate_threat_intel(self, collections: Optional[List] = None, no_clobber: bool = False) -> bool:
        notebooks, notebook_files = [], []

        loaders = [FileSystemLoader([self.intel_coll_dir, self.intel_ingest_dir, self.intel_normalize_dir])]
        try:
            # if package is not installed this will fail
            loaders.append(PackageLoader("databricks.sirens"))
        except Exception:
            # ignore errors, probably means we're running from source
            pass
        j2obj = Environment(loader=ChoiceLoader(loaders), undefined=StrictUndefined)

        # read threat configuration from sirens.conf & merge with intel.yaml
        threat_conf = self._get_intel_configuration(collections)

        # Generate a list of notebooks needed for both collection and subsequent ingest jobs.
        try:
            for itm in threat_conf:
                notebooks.append({'type': 'collector', 'notebook': itm.get('collector').get('name')})
                # add ingest notebook if defined
                if itm.get('ingest_notebook'):
                    notebooks.append({'type': 'ingest', 'notebook': itm.get('ingest_notebook')})

        except Exception as exc:
            logger.error(f'failed to create unique list of intel notebooks: {exc}')

        # append global notebooks required for all normalizing the individual feeds.
        global_notebooks = [{'type': 'ingest', 'notebook': 'intel_enrichment'},
                            {'type': 'ingest', 'notebook': 'intel_staging'},
                            {'type': 'ingest', 'notebook': 'dns_query_cti_preprocessing'},
                            {'type': 'ingest', 'notebook': 'vpcflow_cti_preprocessing'},
                            ]
        notebooks.extend(global_notebooks)

        # remove duplicates
        notebooks = list({frozenset(item.items()): item for item in notebooks}.values())

        for nb in notebooks:
            try:
                r_val = self._make_intel_notebooks(nb, j2obj)
            except Exception as exc:
                raise Exception(f"issue making notebook: {exc}")

            if isinstance(r_val, NotebookData):
                notebook_files.append(r_val)
            else:
                logger.error(f"received invalid object - should be NotebookData class: {r_val}")

        # write notebooks into deploy directory.
        for itm in notebook_files:
            self._write_intel_notebook(itm.NotebookTask, itm.FileName, itm.FileContents, no_clobber)

        return True

    def _generate_threat_hunts(self, hunts, no_clobber) -> bool:
        loaders = [FileSystemLoader([self.hunt_notebooks_dir, self.playbooks_dir])]
        try:
            # if package is not installed this will fail
            loaders.append(PackageLoader("databricks.sirens"))
        except Exception:
            # ignore errors, probably means we're running from source
            pass
        j2obj = Environment(loader=ChoiceLoader(loaders), undefined=StrictUndefined)

        configured_hunts = self._get_hunts_from_global_config(hunts=hunts)
        for hunt_name in configured_hunts:
            # read the conf/threat_hunting hunt.yaml file
            hunt_config = config_reader.ThreatHuntReader().read(hunt_name)
            notebooks = []
            notebook_files = []

            # get a list of notebooks & playbooks defined in the yaml config.
            try:
                notebooks = [notebook["name"] for notebook in hunt_config.get("notebooks")]
            except TypeError:
                pass
            try:
                [notebooks.append(x) for x in hunt_config.get("playbooks")]
            except TypeError:
                pass

            # pass any config from the yaml file into the each hunt notebook if possible.
            notebook_files.extend(
                [self._make_hunt_notebooks(hunt_name, notebook, hunt_config.get("notebooks"), j2obj)
                 for notebook in notebooks])
            # write notebooks
            [self._write_threat_hunt_notebook(itm[0], itm[1], itm[2], no_clobber) for itm in
             notebook_files]

        return True

    def generate_threat_hunts(self, hunts: Optional[List[str]] = None, no_clobber: bool = False):
        """Add threat hunt notebooks to the deploy directory

        :param no_clobber:
        :param hunts: list of specific hunt names to include
        :type hunts: Optional[List[str]]
        :return: success or failure
        :rtype: bool
        """
        return self._generate_threat_hunts(hunts=hunts, no_clobber=no_clobber)

    def generate_threat_intel(self, collections: Optional[List[str]] = None, no_clobber: bool = False) -> bool:
        """add threat intelligence collection notebooks to the deploy directory.

        :param collections: list of intel collections, defaults to None
        :type collections: Optional[List[str]], optional
        :param no_clobber: if to NOT overwrite existing deploy directory entires, defaults to False
        :type no_clobber: bool, optional
        :return: success or failure
        :rtype: bool
        """
        return self._generate_threat_intel(collections=collections, no_clobber=no_clobber)

    def generate_notebooks(self, stanzas: Optional[List[str]] = None,
                           no_clobber: bool = False) -> bool:
        """Generate Notebooks for each enabled [input:<source>:<sourcetype>] stanza

        :raises ValueError: for key issues
        :return: 0 on OK
        :rtype: bool
        """
        input_list = []
        try:
            self.config_dir = self.config['global']['input_config_dir']
        except KeyError:
            print_warning_message("Missing key in global 'input_config_dir' - defaulting to 'log_sources'")
            self.config_dir = 'log_sources'
        try:
            self.deploy_dir = self.config['deploy']['deploy_dir']
        except KeyError:
            print_warning_message(f"Missing key in global 'deploy_dir' - defaulting to 'deploy'")
            self.deploy_dir = 'deploy'
        try:
            self.notebook_language = NotebookLanguage[self.config['default']['notebook_language']]
        except KeyError:
            print_warning_message(f"Missing key in default 'notebook_language' - defaulting to 'python'")
            self.notebook_language = NotebookLanguage.python
        try:
            self.notebook_type = NotebookType[self.config['default']['notebook_type']]
        except KeyError:
            print_warning_message(f"Missing key in default 'notebook_type' - defaulting to 'delta'")
            self.notebook_type = NotebookType.delta
        try:
            if self.notebook_type == NotebookType.dlt:
                self.sirens_lib = self.config['default']['sirens_lib']
        except KeyError:
            print_warning_message(f"Missing key in default 'sirens_lib' - please correct")
        except ValueError as exc:
            print(f"{colours.WARN}{exc}")
            self.notebook_type = 'delta'
        try:
            self.template_dir = self.config['global']['template_dir']
        except KeyError:
            print_warning_message(f"missing key 'template_dir' = defaulting to 'templates'")
            self.template_dir = 'templates'
        try:
            self.deploy_groupby = self.config['deploy']['groupby']
        except KeyError:
            self.deploy_groupby = 'source'

        for section in self.config.sections():
            if 'input:' not in section:
                continue

            # If a list of stanzas specified - process only those inputs.
            if stanzas is not None:
                if section[6:] not in stanzas:
                    continue

            # reset variables that may be overridden on a per stanza basis.
            self.notebook_type = NotebookType[self.config['default']['notebook_type']]
            self.notebook_language = NotebookLanguage[self.config['default']['notebook_language']]
            self.deploy_groupby = self.config['deploy']['groupby']

            # get this stanzas key/value pairs
            self.section_vals = dict(self.config[section])

            if 'source' not in self.section_vals.keys() or 'sourcetype' not in self.section_vals.keys():
                print(
                    f"Stanza {section} is invalid. source and sourcetype should be set - ignoring..")
                continue
            else:
                source = self.section_vals.get("source")
                if source == "correlation":
                    continue
                source_type = self.section_vals.get("sourcetype")
                fn = section
                group_by = self.section_vals.get("groupby", self.deploy_groupby)
                inputs = {"fn": fn, "source": source, "sourcetype": source_type,
                          "groupby": group_by}

                aggregates_reqd = Controller._defines_aggregates(self.config_dir, source,
                                                                 source_type)

            if self.section_vals.get("enabled").lower() == 'true' or self.section_vals.get(
                "enabled") is True:
                # Allow per stanza notebook language over-ride from default
                notebook_language = NotebookLanguage[
                    self.section_vals.get("notebook_language", self.notebook_language.value)]
                # Allow per stanza notebook type over-ride from default
                notebook_type = NotebookType[
                    self.section_vals.get("notebook_type", self.notebook_type.value)]
                if notebook_type == NotebookType.dlt:
                    # save up and create the single standardize notebook for all feeds as a last
                    # operation.
                    input_list.append(inputs)

                # if groupby key specified, check is either stage or source
                if group_by != 'stage' and group_by != 'source':
                    print_warning_message("groupby must be either stage or source - defaulting to 'source'")
                    inputs['groupby'] = 'source'

                try:
                    print_info_message(f"Generating Notebooks for {section}")
                    self._gen_notebook(NotebookStage.ingest, notebook_language,
                                       notebook_type, inputs, no_clobber)
                    self._gen_notebook(NotebookStage.parse, notebook_language,
                                       notebook_type, inputs, no_clobber)
                    self._gen_notebook(NotebookStage.normalize, notebook_language,
                                       notebook_type, inputs, no_clobber)
                    if self.notebook_type == NotebookType.delta:
                        self._gen_notebook(NotebookStage.maintenance, notebook_language,
                                           NotebookType.delta, inputs, no_clobber)
                    if aggregates_reqd:
                        self._gen_notebook(NotebookStage.aggregate, notebook_language,
                                           notebook_type, inputs, no_clobber)
                except (SirensUserException, Exception) as exc:
                    print_error_message(f"Stanza {section}: Failed Notebook Creation: {exc}")

        # write out single aggregation notebook for dlt deployments.
        if input_list:
            try:
                self._gen_standardize_notebook(self.notebook_language, NotebookType.dlt, input_list,
                                               no_clobber)
            except (SirensUserException, Exception) as exc:
                traceback.print_tb(exc.__traceback__)
                print_error_message(f"Failed dlt Standardize Notebook Creation: {exc}")

        return True


    def auto_deploy(self, gen_config_only, gen_and_deploy_only):
        setup = deployment_setup.DeploymentSetup()

        if not gen_and_deploy_only:
            self.config = setup.generateSirensConfig()

        self._validate_enabled_inputs()

        #TODO: validate detections

        if gen_config_only:
            return True

        self.generate_notebooks()
        self.generate_detections()
        self.generate_threat_hunts()
        self.generate_threat_intel()
        self.generate_alerts()
        self.generate_cim_tables()

        setup.save_deploy_files_to_repo()

        self.build()
        self.deploy()

        setup.upload_wheel()

        return True


    def _validate_enabled_inputs(self):
        enabled_inputs = []
        for input in filter(lambda s: s.startswith('input:'), self.config.sections()):
            if self.config.get(section=input, option='enabled') == 'true':
                parts = input.split(":")
                if len(parts) != 3:
                    logger.warning(f"Invalid input format: {input}. Skipping.")
                    continue
                _, group, source = parts
                input_config_path = f"log_sources/{group}/{source}"
                enabled_inputs.append(input_config_path)

        if self.validate('inputs', enabled_inputs):
            logger.error("Inputs did not pass validation!")
            sys.exit(1)

    def validate(self, conf_type, search_path: List[str]) -> bool:
        """Validate the inputs.yaml files discovered under search_path

        :param conf_type:
        :param search_path: may discover a single file or multiple in glob'd search
        :type search_path: str
        :return: bool
        :rtype: bool
        """

        if isinstance(search_path, str):
            search_path = [search_path]

        has_errors = False
        print_info_message(f"Validating {conf_type} files discovered under {search_path}")
        files_to_validate = self._search_file_path(conf_type, search_path)
        if len(files_to_validate) == 0:
            print_warning_message(f"no files discovered for arg: {search_path}")
            return True

        print_info_message(
            f"Validating {len(files_to_validate)} files of type {conf_type} discovered under {search_path}")
        for file in files_to_validate:
            print_info_message(f"checking {file}....")
            validator_obj = Validator(file)
            failed_validation = None
            if 'inputs' in PurePosixPath(file).stem:
                failed_validation = validator_obj.validate_file(validators.inputs)
            if 'enrichments' in PurePosixPath(file).stem:
                failed_validation = validator_obj.validate_file(validators.enrichments)
            if failed_validation:
                print_warning_message(f"File {file} failed validation - will not be used")
                has_errors = True
            else:
                print_info_message(f"appears to be OK....")

            # if 'inputs' not in PurePosixPath(file).stem:
            #    print_warning_message(f"Validation for none inputs.yaml ({file}) not implemented yet.")

        return has_errors

    def generate_detections(self, stanzas: Optional[List[str]] = None,
                            no_clobber: bool = False) -> bool:
        """Generate Detections for each enabled [input:<source>:<sourcetype>] stanza

        :raises ValueError: for key issues
        :return: 0 on OK
        :rtype: bool
        """
        try:
            self.deploy_dir = self.config['deploy']['deploy_dir']
        except KeyError:
            print_warning_message(f"Missing key in global 'deploy_dir' - defaulting to 'deploy'")
            self.deploy_dir = 'deploy'
        try:
            self.template_dir = self.config['global']['template_dir']
        except KeyError:
            print_warning_message(f"missing key 'template_dir' = defaulting to 'templates'")
            self.template_dir = 'templates'

        nb_lang = NotebookLanguage[self.config['default'].get('notebook_language', 'python')]
        nb_type = NotebookType[self.config['default'].get('notebook_type', 'dlt')]
        pipelines = ''

        for section in self.config.sections():
            #for section in self.config.get('sections', []):
            # get this stanzas key/value pairs
            section_vals = dict(self.config[section])

            if 'detections:' not in section:
                continue

            if section_vals.get('enabled').lower() != 'true':
                continue

            source_table = section.split(':')[1]
            name = section.split(':')[2]

            detection_obj = config_reader.Detection(source_table, name)

            pipeline_config = detection_obj._load_pipeline_yaml(name)
            if pipeline_config.get("enabled", True) is False:
                continue

            preprocessing_prefix = f'/detection/notebooks/{nb_type.value}/{source_table}/'

            try:
                inputs = {
                    "pipeline_name": name,
                    "source": source_table,
                }

                print_info_message(f"Generating Detection Notebooks for {section}:{name}")

                self._gen_detection_notebook(nb_lang, nb_type, inputs, name,
                                             no_clobber=no_clobber)

                pipelines += f"""'sirens.{pipeline_config["name"]}', \n  """

                if pipeline_config.get('depends_on'):
                    for dependency in pipeline_config['depends_on']:
                        preprocessing_source = preprocessing_prefix + dependency + '.py'
                        dest = GlobalConfig.get_detection_deploy_dir(
                            self.config) + f"/detection/{nb_type}/{source_table}/{dependency}.py"
                        for file in PathUtils.get_relative_file_paths([preprocessing_source]):
                            try:
                                config_file = Path(file)
                                if config_file.is_file():
                                    shutil.copyfile(file, dest)
                            except Exception as exc:
                                logging.exception(exc)

            except (SirensUserException, Exception) as exc:
                logging.exception(exc)
                print_error_message(f"Stanza {section}: Failed Notebook Creation: {exc}")

            if nb_type == NotebookType.dlt:
                self._gen_detection_notebook(nb_lang, nb_type,
                                             {"source": source_table},
                                             'global_installations', 'global_installations',
                                             no_clobber)
        if nb_type == NotebookType.dlt:
            pipelines = pipelines[:-5]
            self._gen_detection_notebook(nb_lang, nb_type, {"pipelines": pipelines},
                                         'alerts_writer', 'alerts_writer', no_clobber)

        return True

    def generate_alerts(self, stanzas: Optional[List[str]] = None,
                            no_clobber: bool = False) -> bool:
        """Generate Alerts for each enabled [alerts:<input_table>]

        :raises ValueError: for key issues
        :return: 0 on OK
        :rtype: bool
        """
        try:
            self.deploy_dir = self.config['deploy']['deploy_dir']
        except KeyError:
            print_warning_message(f"Missing key in global 'deploy_dir' - defaulting to 'deploy'")
            self.deploy_dir = 'deploy'
        try:
            self.template_dir = self.config['global']['template_dir']
        except KeyError:
            print_warning_message(f"missing key 'template_dir' = defaulting to 'templates'")
            self.template_dir = 'templates'

        nb_lang = NotebookLanguage[self.config['default'].get('notebook_language', 'python')]
        nb_type = NotebookType[self.config['default'].get('notebook_type', 'dlt')]

        for section in self.config.sections():
            section_vals = dict(self.config[section])

            section_split = section.split(':')
            if section_split[0] != 'alerts':
                continue

            if section_vals.get('enabled').lower() != 'true':
                continue

            input_table = section_split[1]
            section_vals['database'] = self.config['default']['target_database']
            section_vals['input_table'] = input_table

            self._gen_alerts_notebook(nb_lang, nb_type, section_vals)

        return True

    @staticmethod
    def build() -> bool:
        """Build terraform variable files

        :return: bool
        :rtype: bool
        """
        try:
            target_dir = os.path.join(ROOT_DIR, 'terraform')
            config = config_reader.GlobalConfig.read()
            # Get a dict from global config file
            master_inventory = deployer.Inventory.get_master_inventory(config)
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        try:
            # Generate jobs tfvars.json
            print_info_message(f"Generating jobs var file")
            jobs_data = deployer.Jobs().create_job_tf(master_inventory)
            BaseUtils.dict_to_json_file(jobs_data,
                                        os.path.join(target_dir, 'jobs.auto.tfvars.json'))
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        try:
            print_info_message(f"Generating threat_intel var file")
            intel_data = deployer.Jobs().create_intel_tf(master_inventory)
            BaseUtils.dict_to_json_file(intel_data, os.path.join(target_dir, 'intel.auto.tfvars.json'))

            # DK - removed - in favour of structured streaming ingest/normalization. sep-24.
            #print_info_message("Creating DLT ingest Jobs::::")
            #dlt_intel = deployer.Jobs().create_dlt_intel_tf(master_inventory)
            #BaseUtils.dict_to_json_file(dlt_intel, os.path.join(target_dir, 'dlt_intel.auto.tfvars.json'))
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        try:
            # Generate DLT jobs tfvars.json
            print_info_message(f"Generating DLT jobs var file")
            dlt_jobs_data = deployer.Jobs().create_dlt_job_tf(master_inventory)
            BaseUtils.dict_to_json_file(dlt_jobs_data,
                                        os.path.join(target_dir, 'dlt_jobs.auto.tfvars.json'))
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        try:
            # Generate maintenance_tasks tfvars.json
            print_info_message(f"Generating notebook maintenance tasks")
            if GlobalConfig.get_notebook_language(config) == "delta":
                maintenance_tasks_data = deployer.Jobs().create_maintenance_tasks_tf(
                    master_inventory)
            else:
                maintenance_tasks_data = {"maintenance_jobs": []}
            BaseUtils.dict_to_json_file(maintenance_tasks_data,
                                        os.path.join(target_dir, 'maintenance_jobs.auto.tfvars.json'))
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        try:
            # Generate sql_dashboards tfvars.json
            print_info_message(f"Generating sql dashboards tasks")
            sql_dashboards_data = deployer.Dashboards(config).create_dashboards_tf()
            BaseUtils.dict_to_json_file(sql_dashboards_data,
                                        os.path.join(target_dir, 'sirens_dashboards.auto.tfvars.json'))
        except Exception as exc:
            raise SirensUserException(f"{exc}") from exc

        return True

    @staticmethod
    def plan():
        source_code_details = deployer.Repos(config_reader.GlobalConfig.get()).get_details()
        t = deployer.TF.plan(tf_vars=source_code_details)
        return True

    @staticmethod
    def deploy():
        source_code_details = deployer.Repos(config_reader.GlobalConfig.get()).get_details()
        t = deployer.TF.apply(tf_vars=source_code_details)
        return True

    @staticmethod
    def destroy():
        source_code_details = deployer.Repos(config_reader.GlobalConfig.read()).get_details()
        t = deployer.TF.destroy(tf_vars=source_code_details)
        return True
    
    def generate_cim_tables(self, path: Optional[str] = None) -> bool:
        path = path if path else "docs/source/cim_tables/"
        
        for file in glob.glob(os.path.join(path, "**.csv")):
            with open(file) as csvfile:
                try:
                    tablename = file.replace(".csv", "")
                    reader = csv.DictReader(csvfile)
                    fields: list[str] = []
                    for row in reader:
                        fields.append(row)

                    data = {"name": tablename, "fields": fields}
                    self._generate_cim_table(data)
                except Exception:
                    print(f"Error parsing {file}")
                    continue
    
    def _generate_cim_table(self, data):
        template = self.j2obj.get_template("cim_table.sql.jynja")
        deploy = self.deploy_dir + "/sql/cim_tables/"
        if not os.path.exists(deploy):
            os.makedirs(deploy)
        with open(os.path.join(deploy, f"{data['name']}.sql"), "w") as f:
            f.write(template.render(data))
        
class Validator:
    def __init__(self, file):
        self.file = file
    def validate_file(self, validator):
        return validator.start(self, self.file)
