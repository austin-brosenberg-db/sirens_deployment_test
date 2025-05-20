Threat Hunting
==============

Threat Hunting Lifecycle Management.
------------------------------------

This sections describes how the threat hunting library is used in
Sirens. It also includes examples that introduces the lifecycle
management and functions for creating, capturing, and triaging scheduled
and ad-hoc threat hunting activities.

|

What is the Threat Hunt Lifecycle in Sirens?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The threat hunt library is used to manage the end-to-end workflow for
threat hunting using sirens. It has the following components;

|

**Tracking:** Allows you to execute scheduled hunts, and record any
results for further analysis and tracking over time.

**Hunt Commands:** Allows you retrieve executed hunts and manage the
triage workflows, using annotations, marking, filtering, and risk
scoring.

**Hunt Library:** Allows you to configure, and package combinations of
individual notebooks into a hunt that can be scheduled to execute. A
hunt may be targeted at specific campaigns, actors, tactics or
techniques, or used as simple hypotheses based hunting to create
accurate detections.

**Hunt Samples:** Databricks contributed notebooks and sample hunts to
be used as inspiration and jump off points for organisational threat
hunting activities.

**Risk Framework:** Automatically annotate any captured results with
with a generated risk score during scheduled execution, or add a risk
score ad-hoc when later triaging a scheduled hunt.

**Hunt Workflow Tracking:** Track the status of executed hunts by
including workflow details. Assign to users, update priority, status &
severity of a hunt.

|

Hunt Tracking
~~~~~~~~~~~~~

Threat hunting is a speculative and iterative process, that at a very
high level defines a hypothesis, followed by a set of search conditions
to actively search an environment for a condition or set of conditions
that may or may not exist. Once defined, the hunt should be ran
regularly and tracked overtime for changes. Any significant indicators
should be raised to an analyst and investigated appropriately.

In Sirens you can track the output of all the individual search commands
executed in a notebook, and across multiple notebooks within a defined
hunt to keep track of hunt campaigns.

Sirens threat hunt framework uses a context manager, and decorators to
log and track your notebook activity and results. During notebook
development you can choose which commands to capture and track as part
of the hunt results. Sirens will capture the results dataframe and log
the elapsed time for the cell execution to track performance metrics.

The following code shows the basic flow to start, capture and complete a
threat hunt within a notebook.


.. code:: python

   import databricks.sirens.threathunting as TH

   # register the following search as an active threat hunt
   hunt_name = "HNT_EXEC_T1047.000-WMIC-Usage"

   with TH.ThreatHunt(hunt_name) as hunt:
       # capture the following dataframe results
       # to capture the results - define a function and decorate it with 
       # the capture_output() decorator
       @hunt.capture_ouput(command_name='analytic_1', description'a description')
       def analytic_1():
           df = df.select(<some_filter>)

   df = analytic_1()

   # register the hunt as finished in the hunt index.
   hunt.end()

..

|

.. note:: 
   
   You must call ``object.end()`` - to update the status in the
   hunt index.

|

-  Each time you want to capture results from searches, redefine the
   context manager with the same hunt name,.
-  It is a good practice to create a markdown cell above your code to
   tell other analysts what the search is intended for, and any
   technical analysis and context required. To effectively track results
   later, we recommend you name the markdown with a command name
   (analytic_1, analytic_2) etc.
-  Each time you use the capture_output decorator, capture the
   command_name, and description to be able to easily refer back to the
   original notebook cells.

::: See a sample notebook in notebooks/samples/threathunting.

|

Hunt Tracking Status and State
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


As analytic commands execute they maintain a current status and state,
and register their progress in the threathunt_index table. Long running
or failed hunts can be tracked by using the ``list_executed_hunts()``
function and searching for ``status = ‘started’`` and
``state =‘ something’``. When an executing hunt is between cells or is
executed uncaptured code blocks, the hunt is said to be in a ``paused``
state.

Hunts that have ran successfully will be in a status of ``finished`` and
a state of ``finished``.

|

Hunt Commands
~~~~~~~~~~~~~


These are python functions packaged together that allow you to gain
information about the threat hunt library, register risk scores against
users and devices, and triage executed threat hunts. Examples include,
marking, filtering, annotating and visualising result sets. See the
[functions section] for a detailed breakdown of each function available.

|

Hunt Library
~~~~~~~~~~~~


The hunt library defines a threat hunt to be executed. It includes an
ordered list of notebooks to be executed, and custom definable keys that
can be used in chained notebooks to define custom logic. For example,
the database and table the notebook should read, or a set of conditions
for branched logic.

Campaigns, hunt types, hunt datasources, attack frameworks such as MITRE
ATT&CK and Lockheed Kill Chain stages can be tracked in the hunt library
using custom tags and attacks keys.

See configuring a threat hunt for full details.

|

Hunt Samples
~~~~~~~~~~~~


Located in the notebooks directory of the Sirens repository, are a
number of sample notebooks that can be executed as a standalone ad-hoc
hunt, a configured hunt based on configuration in the hunt library, or
as part of a chained series of notebooks configured in the hunt library.
The notebooks often make use of public datasets, and are available as
inspiration or a jump off point for integration into an organisation
with the appropriate data collection.

|

Risk Framework
~~~~~~~~~~~~~~


The risk framework allows you to track risk objects (either a user or a
device), as part of threat hunting activity. Either during the execution
of a scheduled hunt, or as part of analyst triage process you apply
commands ``add_risk_score`` or ``write_risk_score_from_df`` to mark a
working data frame and later write all annotated rows to the risk table.
See Risk Framework section for details on how this works.

|

Hunt Workflow Tracking
~~~~~~~~~~~~~~~~~~~~~~


Scheduled and executed threat hunts register their execution to the
``threathunt_index`` and ``threathunt_results`` delta tables. By
default, each index record has a ``_workflow_status`` column to help
manage the overall lifecycle of the hunt.

See Workflows Section for more details.

--------------

.. _Threat Hunting Functions:
Threat Hunting Functions
------------------------

Hunting functions provide the interface to read, view, execute, and
write hunt results. To use functions import the threathunting framework
using;

.. code:: python

   import databricks.sirens.threathunting as TH

|

list_hunt_library overview
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The sirens ``list_hunt_library`` command lists defined threat hunts
located in the ``conf/threathunting`` directory.

|

Syntax
^^^^^^

.. code:: python

   df = list_hunt_library(args)

Required arguments
^^^^^^^^^^^^^^^^^^

None

Optional arguments
^^^^^^^^^^^^^^^^^^

**OutputFormat**
  **Syntax:** OutputFormat=asJson \| asDataFrame \| asObject
  
  **Description:** Describes the desired output format
  
  **Default:** asDataFrame

Examples
^^^^^^^^

.. code:: python

   df = list_hunt_library(outputFormat=OutputFormat.asJson)

--------------

list_hunt_library_by_name overview
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Given a specific hunt name, return high level information about it.

Syntax
^^^^^^

.. code:: python

   df = list_hunt_library_by_name(args)

.. _required-arguments-1:

Required arguments
^^^^^^^^^^^^^^^^^^

**hunt_name:**
  **Syntax:** <string>

  **Description:** Specify the name of a hunt defined in the threat_hunting library configuration

.. _optional-arguments-1:

Optional arguments
^^^^^^^^^^^^^^^^^^

**OutputFormat**
  **Syntax:** OutputFormat=asJson \| asDataFrame \| asObject

  **Description:** Describes the desired output format

  **Default:** asDataFrame

.. _examples-1:

Examples
^^^^^^^^

.. code:: python

   df = list_hunt_library_by_name(name="HNT_EXEC_T1047.000-WMIC-Usage", outputFormat=OutputFormat.asJson)

--------------

add_row_numbers overview
~~~~~~~~~~~~~~~~~~~~~~~~

syntax
^^^^^^

.. code:: python

   df = add_row_numbers(args)

.. _required-arguments-2:

Required arguments
^^^^^^^^^^^^^^^^^^

df:
  **Syntax:** <DataFrame>

  **Description:** Incoming DataFrame to add numbers to

.. _optional-arguments-2:

Optional arguments
^^^^^^^^^^^^^^^^^^

time_col:
  **Syntax:** <Column>

  **Description:** A DataFrame must include a time column that can be windowed over.

  **Default:** ``_event_time``

.. _examples-2:

Examples
^^^^^^^^

.. code:: python

   # Where a DataFrame does not have the sirens _event_time column name
   df = add_row_numbers(df=df, time_col="eventTime")

   # If the _event_time exists
   df = add_row_numbers(df=df)

--------------

annotate overview
~~~~~~~~~~~~~~~~~

Annotate a DataFrame row with custom key/value pairs.

Can be used to identify attack tactics and techniques, leave analyst
notes, make a suspicious, malicious or benign determination, and other
use cases.

syntax
^^^^^^

.. code:: python

   df = annotate(args)

.. _required-arguments-3:

Required arguments
^^^^^^^^^^^^^^^^^^

df:
  **Syntax:** <DataFrame>
  
  **Description:** Incoming DataFrame to add numbers to

row_number:
  **Syntax:** <integer>

  **Description:** row number to include annotation on

annotation:
  **Syntax:** <dict>
  
  **Description:** A dictionary of key/value pairs to add

.. _optional-arguments-3:

Optional arguments
^^^^^^^^^^^^^^^^^^

overwrite_existing:
  **Syntax:** <bool>

  **Description:** when supplying a duplicate key, control whether to EXCEPTION or Overwrite, defaults to EXCEPTION

.. _examples-3:

Examples
^^^^^^^^

.. code:: python

   annotation = {'tactic':'Defense Evasion',
                 'technique':'T1134',
                 'technique_name':'Access Token Manipulation'}
   df = annotate(df=df, row=1, annotation=annotation)

--------------

mark overview
~~~~~~~~~~~~~

Mark a row as ‘interesting’.

Highlight a row of interest, to make identification easier whilst
triaging results sets. Creates a new column ``_marked``. Once a row is
marked, the value of the cell is set to ``*`` .

syntax
^^^^^^

.. code:: python

   df = mark(args)

.. _required-arguments-4:

Required arguments
^^^^^^^^^^^^^^^^^^

df:
  **Syntax:** <DataFrame>
  
  **Description:** Incoming DataFrame to mark as interesting

rows:
  **Syntax:** <string>
  
  **Description:** either as single integer, a range, or comma list. (example ‘1, 3,4 7-10’)

.. _optional-arguments-4:

Optional arguments
^^^^^^^^^^^^^^^^^^

toggle:
  **Syntax:**: <bool>
  
  **Description:** Toggle a row ‘on’ or ‘off’
  
  **Default:** True

.. _examples-4:

Examples
^^^^^^^^

.. code:: python

   # mark rows 1 to 4 inclusive as inreresting
   df = mark(df=df, rows="1-4")

   # remove interesting row 3
   df = mark(df=df, rows=3, toggle=False)

   # mark rows 1, 3 and 5 as interesting
   df = mark(df=df, rows="1,3,5")

--------------

filter_marked overview
~~~~~~~~~~~~~~~~~~~~~~

Given a number of rows marked as interesting. See ``mark()`` - Filter
the DataFrame down to only those marked.

syntax
^^^^^^

.. code:: python

   df = filter_marked(args)

.. _required-arguments-5:

Required arguments
^^^^^^^^^^^^^^^^^^

df:
  **Syntax:** <DataFrame>

  **Description:** Incoming DataFrame to filter

.. _optional-arguments-5:

Optional arguments
^^^^^^^^^^^^^^^^^^

None

.. _examples-5:

Examples
^^^^^^^^

.. code:: python

   df = filter_marked(df=df)

--------------

list_executed_hunts overview
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

List all executed threat hunts that match the filter criteria in the
index table.

Each threat hunt that executes logs its activity to the
``threathunt_index`` and ``threathunt_results`` table (if results are
captured using the api). This command allows you to supply search
criteria to search for executed hunts.

Syntax
^^^^^^

.. code:: python

   df = list_executed_hunts(args)

.. _required-arguments-6:

Required arguments
^^^^^^^^^^^^^^^^^^

None

Returns all executed hunts.

.. _optional-arguments-6:

Optional arguments
^^^^^^^^^^^^^^^^^^

hunt_name:
  **Syntax:** <string>
  
  **Description:** Specify the name of the hunt to be filtered from the index

time_start:
  **Syntax:** <time_str>
  
  **Description:** Filter records by start time

time_end:
  **Syntax:** <time_str>
  
  **Description:** Filter records by end time. Use with ``time_start``
  
  **Default:** current_time

run_id:
  **Syntax:** <str>
  
  **Description:** Specify a unique id to filter results for

status:
  **Syntax:** <str> initializing \| running \| finished
  
  **Description:** filter hunts by one of three possible statuses.

state:
  **Syntax:** <str> initializing \| active \| paused \| finished
  
  **Description:** A hunt is ‘active’ when the cell is running, paused inbetween cells (possibly executing none captured operations), and finished when the notebook completes.

result:
  **Syntax:** <str> success \| failed
  
  **Description:** The overall result of the hunt

database:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc database has been used

hunt_index:
  **Syntax:** <string>

  **Description:** If an ad-hoc hunt_index has been used

hunt_table:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_table has been used

output_format:
  **Syntax:** OutputFormat=asJson \| asDataFrame \| asObject
  
  **Description:** Describes the desired output format
  
  **Default:** asDataFrame

.. _examples-6:

Examples
^^^^^^^^

.. code:: python

   # return a specific run_id
   df = list_executed_hunts(df=df, run_id="52345-34534-23242-234234")

   # return a hunt name executed after a date
   df = list_executed_hunts(df=df, hunt_name="HNT_EXEC_T1047.000-WMIC-Usage", start_time="2023-01-23 12:00:00")

--------------

show_hunt_results overview
~~~~~~~~~~~~~~~~~~~~~~~~~~

Retrieve the commands and results of a previously executed hunt.
Displays the HTML content about the analytic, and prints the original
DataFrame to the cell output. Results are passed back as a python
``Iterator`` object, so the result set should be printed as such.

|

.. note:: 
   
   See ``get_hunt_results`` to retrieve DataFrames as variables
   that can be interacted with like a live notebook.

|

.. _syntax-1:

Syntax
^^^^^^

.. code:: python

   df = show_hunt_results(args)

.. _required-arguments-7:

Required arguments
^^^^^^^^^^^^^^^^^^

None

.. _optional-arguments-7:

Optional arguments
^^^^^^^^^^^^^^^^^^

hunt_name:
  **Syntax:** <string>
  
  **Description:** Specify the name of the hunt to be filtered from the index

time_start:
  **Syntax:** <time_str>

  **Description:** Filter records by start time

time_end:
  **Syntax:** <time_str>
  
  **Description:** Filter records by end time. Use with ``time_start``
  
  **Default:** current_time

run_id:
  **Syntax:** <str>
  
  **Description:** Specify a unique id to filter results for

command_name:
  **Syntax:** <string>
  
  **Description:** search for a captured analytic command. This is the ``command_name`` given in ``capture_output()`` during a hunt.

database:
  **Syntax:** <string>

  **Description:** If an ad-hoc database has been used

hunt_index:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_index has been used

hunt_table:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_table has been used

.. _examples-7:

Examples
^^^^^^^^

.. code:: python

   # return a specific run_id
   for html, analytic_df in show_hunt_results(df=df, run_id="52345-34534-23242-234234"):
       print(html)
       print(analytic_df)

   # return a hunt name executed after a date
   for html, analytic_df in show_hunt_results(df=df, hunt_name="HNT_EXEC_T1047.000-WMIC-Usage", start_time="2023-01-23 12:00:00")
       print(html)
       print(analytic_df)

--------------

get_hunt_results overview
~~~~~~~~~~~~~~~~~~~~~~~~~

Closely related to ``show_hunt_results`` command. This command retrieves
a previously executed hunts results as the DataFrames were captured at
execution using the ``capture_output`` command decorator.

The command returns a list of DataFrame(s) which can then be manually
assigned to a variable and interacted with just like if you were running
the original command live time.

Syntax
^^^^^^

.. code:: python

   list_of_analytics = get_hunt_results(args)

.. _required-arguments-8:

Required arguments
^^^^^^^^^^^^^^^^^^

None

.. _optional-arguments-8:

Optional arguments
^^^^^^^^^^^^^^^^^^

hunt_name:
  **Syntax:** <string>

  **Description:** Specify the name of the hunt to be filtered from the index

time_start:
  **Syntax:** <time_str>
  
  **Description:** Filter records by start time

time_end:
  **Syntax:** <time_str>
  
  **Description:** Filter records by end time. Use with ``time_start``
  
  **Default:** current_time

run_id:
  **Syntax:** <str>
  
  **Description:** Specify a unique id to filter results for

database:
  **Syntax:** <string>
 
  **Description:** If an ad-hoc database has been used

hunt_index:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_index has been used

hunt_table:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_table has been used

.. _examples-8:

Examples
^^^^^^^^

.. code:: python

   list_of_analytics = get_hunt_results(run_id="21343-23423-234232")

   print(f"returned: {len(list_of_analytics)} DataFrames")

   analytic_1 = list_of_analytics[0]
   display(analytic_1)

--------------

write_hunt_index overview
~~~~~~~~~~~~~~~~~~~~~~~~~

Write a DataFrame that is a hunt index record back to the index.

When retrieving and updating a hunt records workflow or other columns,
this command will merge the record back as an updated record.

Syntax
^^^^^^

.. code:: python

   write_hunt_index(args)

.. _required-arguments-9:

Required arguments
^^^^^^^^^^^^^^^^^^

df:
  **Syntax:** <DataFrame>

  **Description:** DataFrame containing a hunt index record to be written back to the index table.

.. _optional-arguments-9:

Optional arguments
^^^^^^^^^^^^^^^^^^

database:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc database has been used

hunt_index:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_index has been used

hunt_table:
  **Syntax:** <string> 
  
  **Description:** If an ad-hoc hunt_table has been used

.. _examples-9:

Examples
^^^^^^^^

.. code:: python


   id="12345-12345-12345"

   # retrieve an index record
   df = list_executed_hunts(run_id = id)

   # update assignee and status
   df = assign_to_me(df, run_id = id)
   df = set_status(df, status=WorkflowStatus.IN_PROGRESS)

   # save the updated DataFrame to the index table
   result = write_hunt_index(df)

--------------

write_hunt_results overview
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Write a DataFrame that contains updated hunt results back to the results
table.

When the results sets of previously executed hunts are being triaged,
you may interact and update columns such as the ``_annotation`` column.
Once updated use this command to write the result set back.

Syntax
^^^^^^

.. code:: python

   write_hunt_index(args)

.. _required-arguments-10:

Required arguments
^^^^^^^^^^^^^^^^^^

df:
  **Syntax:** <DataFrame>
  
  **Description:** DataFrame containing a hunt result record to be written back to the results table.

analytic_command:
  **Syntax:** <string>
  
  **Description:** The original analytic command name. This is used to identify the correct results row in the results table for a merge operation to take place.

.. _optional-arguments-10:

Optional arguments
^^^^^^^^^^^^^^^^^^

database:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc database has been used

hunt_index:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_index has been used

hunt_table:
  **Syntax:** <string>
  
  **Description:** If an ad-hoc hunt_table has been used

.. _examples-10:

Examples
^^^^^^^^

.. code:: python

   # retrieve the results for run_id
   list_of_analytics = get_hunt_results(run_id="21343-23423-234232")

   print(f"returned: {len(list_of_analytics)} DataFrames")

   # assign the first analytic command
   analytic_1 = list_of_analytics[0]
   analytic_1 = add_row_numbers(analytic_1)

   # annotate a row
   analytic_1 = annotate(analytic_1, row=1, annotation={'analyst_notes':'please schedule for device scan'})

   # update the new DataFrame to the results table
   result = write_hunt_results(df=analytic1, analytic_command_name="analytic_1")

--------------


.. _risk-framework-1:

Risk Framework
--------------


The risk framework is used to surface risk objects that are generating
notable events and which may warrant further investigation. Ordinarily
detections and threat hunts may surface events that are notable, but in
and of themselves do not constitute a credible attack.

Using a risk based approach in Sirens threat hunting and detection
framework you can add risk modifiers to a risk object. (user or a
device).

Each risk modifier supplies a potential impact and confidence score
which is used to calculate an overall risk score for the risk object in
question. You can use the risk dashboard and related searches, or
develop additional detections to surface those objects that rise above a
predefined score within a specific time period.

Additionally, you can provide custom key/value pairs to add the source,
and context for the event(s) generating the risk modifier.

|

Risk Tables
~~~~~~~~~~~

By default the risk table is stored in the database ``sirens`` under a
name of ``risk``. You can choose to over-ride this behaviour using the
``sirens.config`` global configuration file.

|

Risk Scoring Algorithm
~~~~~~~~~~~~~~~~~~~~~~

When a ``impact`` and ``confidence`` scores (between 1 - 100) are
provided, the risk framework generates a ``risk_score`` for the event.

|

It is calculated as;


.. code:: python

   risk_score = round((impact * confidence)/100, 0)

|

Example:
^^^^^^^^
.. code:: python

   confidence = 20
   impact = 60
   risk_score = round((60 * 20)/ 100, 0) = 12

|

Change the Risk Database or Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To change the risk database and / or table name from its default. add a
new ``schema:risk`` stanza to the ``sirens.config`` file.

Example
^^^^^^^

.. code:: bash

   # example using unity catalog schema 
   [schema:risk]
   schema = sirens_schema.custom_sirens_database
   risk_table = custom_risk_table_name

   # example using the hive metastore
   [schema:risk]
   schema = custom_sirens_database
   risk_table = custom_risk_table_name

--------------

Workflows
---------

Executed threat hunts are registered into the ``threathunt_index``
table, and by default add a meta column, ``_workflow``, that can allow
you to assign and track the overall status of the hunt.

By default ``_workflow`` status is initially set to the following
defaults;

.. code:: bash

   assignee = 'Unassigned'
   severity = 'Unknown'
   priority = 'Unknown'
   status = 'New'

|

You can choose to update the workflow on a per hunt basis (within a
notebook), or during a subsequent triage phase once the results have
been analysed and further understood.

See Section :ref:`Threat Hunting Functions` see commands to update each
key/value pair.

--------------

Configuring a Threat Hunt
-------------------------

To develop a threat hunt, you generate either singular hunt notebooks,
or multiple notebooks that are then configured in the hunt library. The
hunt library defines what order notebooks should run in, any custom
arguments you want to pass between cascading notebooks, and any
contextual information that pertains to the hunt such as MITRE ATT&CK
information, campaign identifiers, IOC hunt type.

The modular approach to cascading notebooks, and free form arguments
means you can organize notebooks however you choose, and reuse notebook
functionality between different hunts.

For instance, a single notebook may connect to a third party API, and
collect some data. The following notebook is configured to receive the
arguments needed to operate on the data collected into a delta table.
Subsequent notebooks may operate as incident response playbooks,
gathering further information if the hunt notebook identified
interesting information.

Follow the below instructions for configuring, deploying and scheduling
notebook based threat hunts.

  1. Develop the the notebook threat hunts using the threat hunt API.
  2. Configure the hunt library to run single or chained notebooks.
  3. Add the hunt to sirens.config
  4. Generate notebooks to deploy
  5. Deploy the notebooks to Databricks workflows for scheduled execution.

|

1. Develop the threat hunt notebook(s)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Begin developing your hunt notebooks using the threat hunt template
located in ``notebooks/threat_hunting/Template``. You are free to
customize the template as needed.

|

.. note:: 

   Threat hunt notebooks are expected to be located in the
   ``notebooks/threat_hunting`` directory.

   Later updates will add support for user located notebooks


|

In addition to the Template hunt, there is an example hunt
``LSASS_Memory_Read_Access`` located in the ``notebooks/threat_hunting``
directory.

|

Threat hunt naming convention
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Hunt notebooks and the library hunt name are user configurable. You
should choose a scalable naming convention. One possible convention
based on `MITRE ATT&CK <https://attack.mitre.org/#>`__:

``HUNT-<TACTIC>-<TECHNIQUE>-<SUB_TECHNIQUE>-<INDEX>-<NAME>``

|

.. note:: 

   Example(s):

   ``HUNT-TA0007-T1087-001-100-LSASS_Memory_Read_Access``
   ``HUNT-Discovery-T1087-002-101-LSASS_Memory_Read_Access``

|

2. Configure the hunt library
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

|

.. note:: 
   
   The name of the hunt should follow your naming convention as
   described above.

   This will be name you use in subsequent sections.

|

a.) create a new directory under ``conf/threat_hunting`` with the name of your hunt.

|
  
  Example: ``HUNT-TA0007-T1087-001-100-LSASS_Memory_Read_Access``

|

b.) create a new file ``hunt.yaml`` in your hunt directory.

|

An example hunt.yaml is located under ``conf/threat_hunting/HNT-TA0007-T1087-001-100-LSASS_Memory_Read_Access``

|

Example:

.. code:: yaml

   meta:
     author: Derek King
     created: 11/15/2023
     version: 1.0
     last_updated: 11/15/2023
   hunt:
     name: HNT-TA0007-T10870-001-100-LSASS_Memory_Read_Access
     description: Processes accessing Local Security Authority Subsystem Service
     attacks:
       - mitre:
           tactic: discovery
           technique_id: T1087
           subtechnique_id: 001
     tags:
       campaign: trickbot
       hunt_type: filename
       source_type: device_process_events
   notebooks:
     - name: sentinel_fetch
       input:
         daterange: -24hours
     - name: LSASS_Memory_Read_Access
       input:
         table: sirens.sentinel_hunt
         custom: key_value_pairs
         that: you_read_in_your_notebook
       output:
         custom: key_value_pairs
         that: you_read_and_use_in_your_notebook

See section: :ref:`hunt.yaml.spec` for all key options.

|

3. Add the hunt to sirens.config
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The hunt notebooks are deployed and scheduled once configured as part of
the global config.

Configure your ``sirens.config`` file to include a new ``[threat_hunt]``
stanza with the name of the hunt.

|

.. warning:: 
   
   The hunt directory name *(under conf/threat_hunting)*, the
   hunt->name *(in hunt.yaml)*, and threat_hunt stanza names must all
   match.

|

Example:

.. code:: bash

   [threat_hunt:HNT-TA0007-T10870-001-100-LSASS_Memory_Read_Access]
   enabled = true
   schedule = 0 0 01 * * ?

|

4. Generate the hunt notebooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For sirens to add the hunt notebooks to databricks workflows, they must
exist in the ``deploy/`` directory.

run the following command from the top-level ``databricks-sirens``
directory.

.. code:: bash

   python3 sirens.py generate_threat_hunts


|

5. Deploy the notebooks
~~~~~~~~~~~~~~~~~~~~~~~

|

Auto deployment
^^^^^^^^^^^^^^^

Sirens is designed to be deployed using terraform. See the [deployment
section] to deploy all deploy artifacts to databricks workflows.

|

Manual Deployment
^^^^^^^^^^^^^^^^^

If you deploy and operate Sirens manually, add the notebooks to
databricks workflows manually, in the order they should be executed,
ensuring hunt_name is passed as key ``source_type``. This key should
then be used for each notebook to read its configuration held under the
notebook section in ``hunt.yaml``

--------------

Change the hunt database and table names
----------------------------------------

To change the threat hunt database and/or table name from its default
add or change a ``schema:threat_hunt`` stanza to the ``sirens.config``
file under the ``system schemas`` section.

|

Examples:

.. code:: bash

   # example schema using a unity catalog schema
   [schema:threat_hunt]
   schema = sirens.mycustomdatabase
   index_table = my_preferred_index
   results_table = my_preferred_threathunt_results

   # example schema using the hive metastore
   [schema:threat_hunt]
   schema = mycustomdatabase
   index_table = my_preferred_index
   results_table = my_preferred_threathunt_results

--------------

.. _hunt.yaml.spec:

Hunt yaml.spec
--------------

The following outlines the key/value pairs for ``hunt.yaml``

.. code:: yaml


   meta:

   author: <string>
   # required
   # specifies the author of the hunt

   created: <date>
   # required
   # Creation date of the hunt config
   # example: 2023-01-23

   version: <string>
   # required
   # specifies the version oof the hunt.yaml file

   last_updated: <date>
   # required
   # specifies the last time the file was updated
   # example: 2023-01-23


   hunt:

   name: <string>
   # required
   # specified the name of the hunt
   # must match the directory name its in, AND the threat_hunt stanza name

   description: <string>
   # required
   # a short description outlining the intent of the hunt

   attacks: <list>
   # optional
   # a freeform list of attack methodologies. This information will be carried
   # into the threat hunt index and used for reporting purposes
   # Example:
   # attacks:
   #   - mitre:
   #       tactic: discovery
   #       technique: T1087
   #       subtechnique: 001

   tags: <dict>
   # optional
   # a freeform dictionary of tags. This information will be carried into the 
   # threat hunt index and used for reporting purposes. Use this for tracking
   # campaigns, hunt types, source_types etc
   # example:
   # tags:
   #   campaign: trickbot
   #   hunt_type: filename
   #   sourcetype: device_process_events

   notebooks:

   - name: <string>
   # required
   # the name of the notebook to execute (without an extension)
   # MUST be located in the notebooks/threat_hunting directory
   # ** NOTEBOOKS may hold custom key value pairs underneath the name: key. 
   # if you generated hunting notebooks using the hunting Template - it will 
   # read these key/values - which you can sebsequently use for custom logic
   # in the notebook itself. 
   # Arguments like what delta table to read, a condition string to decide on 
   # branched logic etc.
   
   # Example:
   notebooks:
     - name: sentinel_fetch
       input:
         daterange: -24hours
     - name: LSASS_Memory_Read_Access
       input:
         table: sirens.sentinel_hunt
         custom: key_value_pairs
         that: you_read_in_your_notebook
       output:
         custom: key_value_pairs
         that: you_read_and_use_in_your_notebook
