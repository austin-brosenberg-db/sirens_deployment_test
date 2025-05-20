from genericpath import isfile
from setuptools import find_packages, setup
import glob
import os
import re

def _get_version():
    VERSIONFILE = "databricks/sirens/_version.py"
    version = "unknown"
    try:
        version_file = open(VERSIONFILE, "rt").read()
    except EnvironmentError:
        pass
    else:
        VSRE = r"^__version__ = ['\"]([^'\"]*)['\"]"
        mo = re.search(VSRE, version_file, re.M)
        if mo:
            version = mo.group(1)
        else:
            print("unable to find version in {VERSIONFILE}")
            raise RuntimeError("if {VERSIONFILE}.py exists, it is required to be well-formed")

    return version


__version__ = _get_version()

def _get_required_modules():
    modules = []
    with open("requirements.txt", "r") as file:
        for mod in file:
            mod = mod.strip()
            if mod != "" and mod[0] != "#":
                modules.append(mod)
    return(modules)

def _get_data_files():
    file_list = []
    files = [file for file in file_list if isfile(file)]

    data_files = {}
    for f in files:
        dir_name = os.path.dirname(f)
        if dir_name not in data_files:
            data_files.setdefault(dir_name, []).append(f)
            # data_files[dir_name].append(f)
        else:
            data_files[dir_name].append(f)

    return [(k, v) for k, v in data_files.items()]

setup(
    name="databricks sirens",
    description="Security Analytics Engine for the Delta lake",
    author="Derek King - Databricks",
    author_email="cybersecurity@databricks.com",
    version=__version__,
    url="https://databricks.com",
    package_dir={
        "databricks": "databricks",
        "databricks.sirens.templates": "templates",
        "terraform": "terraform"
    },
    package_data={
        "databricks.sirens.templates": ["*", "*/**"],
        "terraform": ["*.tf", ".terraform.lock.hcl", "modules/**/*.tf"]
    },
    data_files=_get_data_files(),
    setup_requires=["wheel"],
    install_requires=_get_required_modules(),
    entry_points={'console_scripts': ['sirens = databricks.sirens:sirens_main']}
)
