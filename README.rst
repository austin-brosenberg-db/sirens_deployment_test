.. image:: https://github.com/lipyeowlim/public/raw/main/img/logo/databricks_cyber_logo_v1.png
   :alt: Banner Logo
   :width: 600px

.. image:: https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge
   :target: https://docs.databricks.com/release-notes/runtime/10.4ml.html
.. image:: https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge
   :target: https://cloud.google.com/databricks
.. image:: https://img.shields.io/badge/POC-10_days-green?style=for-the-badge
   :target: https://databricks.com/try-databricks

Welcome to Project Sirens
=========================

Sirens is a coherent suite of Databricks assets (notebooks log_sources.) to
accelerate building your Databricks Lakehouse for Security Operations. The
project includes assets that cover the following capabilities: 

+ pipelines (streaming, batch, Delta Live Tables)
+ notebooks
+ connectors to various security appliances and data sources
+ detections
+ hunting queries
+ AI/ML models

Sirens uses a code generation framework to manage the suite of Databricks assets and includes the ability to deploy those assets to multiple workspaces in multiple regions and multiple cloud environments.

Contact Author: derek.king@databricks.com

----

Quickstart Guide
----------------

If you just want to try out the sirens system in a Databricks workspace without any additional customization in terms of target database names log_sources.:

#. Ensure you have git integration setup in the workspace (see `Git integration with Databricks Repos <https://docs.databricks.com/repos/index.html>`_) and you are in the ``Data Science and Engineering`` persona.
#. Clone this repo to the workspace
#. Switch to the ``test_drive`` branch. 

   - The ``deploy/delta/okta/oktaIM2_log/`` folder will contain a generic version of the generated notebooks for the okta data source. The notebooks will (1) load a sample okta data set into a ``bronze`` table, (2) transform the ``bronze`` table to ``silver`` table, (3) normalize the ``silver`` table to two tables (``authentication``, ``user_management``) in the common data model, and (4) run a few sample queries on the generated tables. 
   - The ``log_sources/okta/oktaIM2_log/`` folder will contain the configuration yaml file relevant to the okta data source.
   - The ``log_sources/okta/oktaIM2_log/samples`` folder will contain one or more sample okta data files.

#. Run the ``RUNME.py`` notebook. The notebook will create a multi-task job for running all the notebooks. To actually run the multi-task job, you will set the ``run_job`` dropdown widget to ``True``. If you do not have the right permissions to run the multi-task job, you can, alternatively, run each of the notebooks in ``deploy/okta/oktaIM2_log/`` in the sequence of the prefix number. 

#. Open the ``04-analytics.py`` notebook, click ``Run all`` if you have not run it, and inspect the query results. Alternatively, go to the ``Data`` tab in the sidebar, navigate to the ``hive_metastore``, search for the ``sirens_test_drive`` database, and inspect the tables that were created.

If you want to customize or further build on the sirens system, follow the deployment and development guide in the next section.

----

Deployment Guide
----------------

The Sirens philosophy is to manage all configurations and deployed notebooks
as code using git processes for version control. It is crucial that the
actual deployed notebooks are versioned controlled to facilitate operational
debugging and troubleshooting.

Initial Deployment
^^^^^^^^^^^^^^^^^^

#. Fork this repo in your organization’s github/gitlab (henceforth ``my-sirens`` repo) & set the upstream to the original sirens repo `Fork a repo - GitHub Docs <https://docs.github.com/en/get-started/quickstart/fork-a-repo>`_ ). This ensures that any configurations and generated files will be in your organization’s git environment along with the proper access controls. The ``my-sirens`` repo will be a private repo in your organization.
#. Clone the ``my-sirens`` repo to your laptop or local development environment.
#. Create a Python virtual environment ``python3 -m venv .venv``
#. Activate the virtual environment ``source .venv/bin/activate``
#. Install the Sirens wheel provided to you ``pip3 install databricks_sirens-0.4.0-py3-none-any.whl``
#. Install dependencies by running ``pip3 install -U -r requirements.txt -r requirements_dev.txt``
#. Create a deployment branch ``deploy_v1``.
#. Edit the config files (sirens.config and log_sources/... ) to setup the data sources for the pipeline and the location of the raw data in cloud storage (the ``rawPath`` in each data source's ``inputs.yaml`` ).
#. Run ``sirens generate_notebooks`` to generate the notebooks (the generated files will reside in the deploy folder).
#. **Note:** Sirens is shipped to ``.gitignore`` the deploy directory. Either remove this requirement in your ``deploy_v1`` branch or run ``git add --force deploy/*``
#. Commit the config files and generated notebooks to the ``deploy_v1`` branch and push to the ``my-sirens`` origin
#. [The following steps in Databricks can be automated via CLI/API.] Create the git repo integration to ``my-sirens`` repo in your Databricks workspace
#. Navigate to the ``my-sirens`` repo in your Databricks workspace and set the branch to ``deploy_v1``
#. Setup the DLT pipeline or Multi-task jobs using generated notebooks in the deploy folder of the ``my-sirens`` repo (``deploy_v1`` branch) – This step can be automated via CLI in the future.

Redeployment after a configuration change
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Examples of configuration change include adding/removing a data source or
tweaking the notebook logic. Note that you will manage the config files and
generated notebooks as code using standard git processes.

#. Edit the config files or template files or source code to incorporate the change. (You are free to use a separate branch if you like)
#. Run ``sirens generate_notebooks`` to generate the notebooks (the generated files will reside in the deploy folder
#. Commit the config files and generated notebooks to the ``deploy_v1`` branch (or you can use a separate branch) and push to the ``my-sirens`` repo
#. [The following steps in Databricks can be automated via CLI/API.]  Navigate to the ``my-sirens`` repo in each of your Databricks workspace and “pull” the branch ``deploy_v1`` for the latest changes
#. Restart the DLT pipeline or Multi-task jobs if you are using DLT pipeline or Multi-task jobs in continuous processing or streaming mode, so that the latest notebooks and config files will be picked up. Batch or scheduled jobs should pick up the latest notebooks and config files automatically on the next scheduled run.

Unity Catalog
^^^^^^^^^^^^^

To use Sirens with Unity Catalog, please begin by following the instructions for getting started: `AWS <https://docs.databricks.com/data-governance/unity-catalog/get-started.html>`_ | `Azure <https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started>`_ | `GCP <https://docs.gcp.databricks.com/data-governance/unity-catalog/get-started.html>`_.

The user or service principal running the Sirens pipelines will need to have the necessary
permissions to create and write to tables in the target catalog and schema in Unity Catalog.

You will also need to configure external storage locations for loading raw data into your ingest pipelines.

When configuring the target database you need to use the catalog name plus the schema (aka "database") name, for example `main.sirens` otherwise it will default to `hive_metastore.sirens`.

Sirens upgrade workflow
^^^^^^^^^^^^^^^^^^^^^^^

There is a chance that after you have deployed sirens, a new
version of the Sirens is released. The following steps walk you
through the upgrade process assuming you have not made substantial changes to
the source code or the template files AND you do want to upgrade to that new
version.

#. Sync the version of Sirens you want using the appropriate git commands (`Syncing a fork - GitHub Docs <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork>`_ ). This will update ``my-sirens`` repo (main branch) with the latest changes.
#. Rebase/merge to the ``deploy_v1`` branch or create a new branch as needed.
#. Repeat the above deployment steps to generate notebooks and deploy to Databricks workspace

----

Development Guide
-----------------

Running tests (WIP)
^^^^^^^^^^^^^^^^^^^

To run the provided integration or smoke tests:

1. Clone the repo (either the original or the clone) to your laptop
2. Change directory to the root directory of the repo
3. Create virtual environment and ensure you have installed the required packages using
   ``pip3 install -r requirements.txt -r requirements_dev.txt`` (could be executed as ``make .venv``)
4. Run ``pytest`` (also could be run as ``make test``)

To get coverage report (could be invoked as ``make coverage``):

1. ``coverage run -m pytest``
2. ``coverage report -m``

Test-driven development workflow (WIP)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sirens is based on a code generation framework that is controlled by a
top-level configuration file ``sirens.config``. For generating simple
notebooks, there is a the jinja2 source template file in the ``templates/`` folder. The actual generation logic reside in the files in the ``src/`` folder.

Here are guidelines on how to develop and add a new output
notebook to sirens following a `test-driven development paradigm <https://en.wikipedia.org/wiki/Test-driven_development>`_.

#. First develop and test the new feature as a notebook (or DLT notebook) in a Databricks workspace. The rest of the steps are performed in your local development environment (e.g., your laptop).
#. Create a new branch and prefix the branch name with your name.
#. Create a new test case in the ``tests/`` folder using the notebook you developed and tested. The provided ``smoke01`` test will be a good example of how to create a test case.
#. Decide how you would like to generate the code for your notebook. Most notebooks can be templatized using Jinja2 syntax and generated as a simple notebook - see the code generation logic in ``src/controller.py``.
#. Use the test case you created earlier to drive the development and testing.
#. Ensure that running ``pytest`` at the repo root directory passes.
#. Push your branch to the origin and open a pull request.
#. Solicit code reviews for the pull request
#. Once the pull request is approved, ask the contact author to merge the code into main.
#. Optionally update the ``test_drive`` branch.
