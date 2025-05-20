import os
import sys
from typing import List

from databricks.sirens._version import ROOT_DIR
from databricks.sirens.exceptions import SirensParsingException


class PathUtils:
    custom_dirs = []

    @staticmethod
    def get_relative_file_paths(params: List) -> List:
        """Return a list of possible relative file paths for input file.

        If the first component of params is in ["conf", "log_source"], it will insert "custom" and "default"
        before the config file and generate paths to try in order. If none of the paths exist, it will use params as is.

        :param params: List of file path segments to be joined (e.g., ["conf", "alert"])
        :type params: List
        :return: List of paths to try for file opening.
        :rtype: List
        """
        if not isinstance(params, list):
            raise SirensParsingException("Please pass a list")
        
        def generate_paths(input_file_path):
            paths = [
                f"{sys.prefix}/{input_file_path}",
                f"{ROOT_DIR}/{input_file_path}",
                f"../../../../../../../../{input_file_path}",
                f"../../../../../../../{input_file_path}",
                f"../../../../../../{input_file_path}",
                f"../../../../../{input_file_path}",
                f"../../../../{input_file_path}",
                f"../../../{input_file_path}",
                f"../../{input_file_path}",
                f"../{input_file_path}",
                f"./{input_file_path}",
                f"/{input_file_path}"
            ]
            if "SIRENS_CONFIG" in os.environ:
                paths.append(os.path.join(os.environ["SIRENS_CONFIG"], input_file_path))
            return paths

        components = []
        for param in params:
            components.extend(param.strip('/').split('/'))

        paths = []

        if not components:
            return paths

        # Check if the last component is a config file (contains a dot)
        file_name = components[-1]
        is_config_file = '.' in file_name and not file_name.startswith('.')
        # Check if the path starts with 'conf' or 'log_sources'
        if components[0] in PathUtils.custom_dirs and is_config_file:
            # Insert 'custom' and 'default' one level above the config file
            insertion_point = len(components) - 1  # One level above file name
            for insert_dir in ['custom', 'default']:
                modified_components = (
                    components[:insertion_point] +
                    [insert_dir] +
                    components[insertion_point:]
                )
                input_file_path = "/".join(modified_components)
                paths += generate_paths(input_file_path)

        # Always add the original path as the last resort
        input_file_path = "/".join(components)
        paths += generate_paths(input_file_path)

        return paths
