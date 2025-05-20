Internal Development Guide
==========================

Running source code from repo (ie not a .whl file)
--------------------------------------------------

Version 13.2+ of DBR included the databricks connect lib which messes up finding the databricks.sirens package. 
If you are running 13.2+ and come up with NoModuleFound Errors, add the following before the databricks.sirens imports.

.. code-block:: python

    import databricks
    import os
    databricks.__path__.append(os.path.abspath("../databricks"))

Depending on where the notebooks are running, you may need to adjust the relative path so they land at databricks-sirens
directory. i.e.

.. code-block:: python

    databricks.__path__.append(os.path.abspath("../../../../databricks"))


Recommendations When Building
-----------------------------

To ensure you're not bundling cached or out of date code,
you should clear your build files before generating your wheel:

.. code-block:: sh

   rm -r build
   python3 setup.py clean
   python3 setup.py bdist_wheel

Updating Dashboards
-------------------

Databricks Terraform provider includes an exporter. This helps managing the large
complex dashboards that are made up of many different resources.

To simplify this you should have dashboards in your workspace with unique names so
you're not exporting duplicate resources!

Assuming you have Terraform initialized already, you can call the exporter like so:

.. code-block:: sh
    
    export tf-exporter=.terraform/providers/registry.terraform.io/databricks/databricks/1.13.0/darwin_amd64/terraform-provider-databricks_v1.13.0
    tf-exporter exporter -directory=tf_export -listing=sql-dashboards -services=sql-queries,sql-dashboards,sql-endpoints -match="My Dev Sirens - " -skip-interactive

This will export all dashboards starting with the name ``My Dev Sirens -`` and associated queries, widgets, and visualizations into the folder ``tf_export``.

Once exported you need to find-and-replace all references to ``data_source_id =`` and ``parent = "folders/..."``.
These should be changed to:

* ``data_source_id = local.sirens_endpoint``
* ``parent = "folders/${databricks_directory.sirens_dashboards_dir.object_id}"``

You can then copy the dashboards and queries into ``terraform/modules/sirens_dashboards/main.tf``

Known Errors and Work arounds
=============================

NoModuleFound pkg_resources
---------------------------
This has been seen with customers deploying sirens for the first time locally. When calling sirens.py
commands the error may occur. This is because setuptools has not been installed on the device. 

To fix:

.. code-block:: sh

    pip install setuptools

