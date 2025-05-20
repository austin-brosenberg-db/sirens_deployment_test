import re
import os

import yaml
import glob
from pathlib import Path
from typing import Union
from databricks.sirens.exceptions import SirensConfigException, SirensParsingException
from databricks.sirens.utils.path_utils import PathUtils


class config_utils:
    @staticmethod
    def _replace_var(_var, configs):
        pattern = re.compile(r'\$\{([0-9a-zA-Z_.]+?)\}')
        matches = pattern.findall(_var)

        for match in matches:
            temp = configs
            for token in match.split('.'):
                try:
                    temp = temp[token]
                except:
                    raise Exception(f"{match} not found in config")
            if _var != "${" + match + "}":
                _var = _var.replace("${" + match + "}", str(temp))
            else:
                _var = temp

        return _var

    @staticmethod
    def _check_env_var_dups(var, seen, formatted_var=None):
        for key in var.keys():
            if formatted_var is None:
                formatted_var = key
            else:
                formatted_var += f".{key}"

            if isinstance(var[key], str) or var[key] is None or isinstance(var[key], int) or isinstance(var[key],
                                                                                                        float):
                if formatted_var in seen:
                    raise Exception(f"{formatted_var} entered twice in environment configs.")
                seen.add(formatted_var)
            else:
                config_utils._check_env_var_dups(var[key], seen, formatted_var)

    @staticmethod
    def merge_dicts(d1, d2):
        for key, value in d2.items():
            if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
                config_utils.merge_dicts(d1[key], value)
            else:
                d1[key] = value
        return d1

    @staticmethod
    def _get_env_vars(env=''):
        env_vars_prefix = None
        for path in PathUtils.get_relative_file_paths(["env_vars"]):
            if Path(path).is_dir():
                env_vars_prefix = path
                break

        if env_vars_prefix is None or Path(f"{env_vars_prefix}/{env}").is_dir() is False:
            raise SirensConfigException("env_vars directory not found")

        seen = set()
        env_vars = {}
        for file in glob.glob(f"{env_vars_prefix}/{env}/*.yaml"):
            try:
                with open(file, "r") as config:
                    file_vars = yaml.safe_load(config)
            except yaml.YAMLError as exc:
                raise SirensConfigException(f"Failed to load env_vars file: {file}", exc) from exc
            config_utils._check_env_var_dups(file_vars, seen)
            config_utils.merge_dicts(env_vars, file_vars)

        return env_vars

    @staticmethod
    def sub_yaml_vars(yaml_vars, env='', env_vars=None):
        if env_vars is None:
            env_vars = config_utils._get_env_vars(env)

        if isinstance(yaml_vars, list):
            for element in yaml_vars:
                config_utils.sub_yaml_vars(element, env, env_vars)
        elif isinstance(yaml_vars, str):
            yaml_vars = config_utils._replace_var(yaml_vars, env_vars)
        elif isinstance(yaml_vars, dict):
            for key in yaml_vars.keys():
                if isinstance(yaml_vars[key], str):
                    yaml_vars[key] = config_utils._replace_var(yaml_vars[key], env_vars)
                elif yaml_vars[key] is None or isinstance(yaml_vars[key], int) or isinstance(yaml_vars[key], float):
                    continue
                else:
                    config_utils.sub_yaml_vars(yaml_vars[key], env, env_vars)

        return yaml_vars
    
    @staticmethod
    def is_file_accesible(file_path: str) -> bool:
        p = Path(file_path)
        if p.is_file():
            return True
        else:
            return False

    @staticmethod
    def get_path_to_file(file: str, absolute_only: bool = False) -> Union[str, bool]:
        paths = PathUtils.get_relative_file_paths([file])
        file_found = False
        for p in paths:
            if config_utils.is_file_accesible(p):
                if absolute_only and not os.path.isabs(str(p)):
                    continue
                
                file_path = str(p)
                file_found = True
                return file_path

        return False
