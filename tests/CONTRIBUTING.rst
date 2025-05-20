Contributing to Sirens
======================

We value all contributions to Sirens. We use GitHub issues to track community reported issues and GitHub pull requests
for accepting changes.

Repository Structure
====================
The repository structure is as follows: 

+ databricks.sirens.config_reader - Reads a log source config object (inputs.yaml)
+ src/connectors - Connectors used for ingesting data
+ src/parsers - Parsing modules for specific log sources collected with a collector
+ databricks.sirens.normalize - Reads log source config object, and transforms a bronze frame to CIM
+ databricks.sirens.utils - Utility functions
+ databricks.sirens.sql - SQL based queries
+ tests/ - pytest unit tests
+ templates/ - Jinja templates used to generate Notebooks

+ log_sources/ - Configuration files for each log source to be ingested and transformed
+ deploy/ - location of generated notebooks, to be hooked up to Databricks workflows or Delta Live Tables



Documentation
=============

The documentation has been produced using Sphinx.

You can locally host the docs by running the reload.py script in the docs/source/ directory.

Style
=====

Tools we use to ensure code formatting:

+ flake8