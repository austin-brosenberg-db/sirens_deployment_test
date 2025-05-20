# Databricks notebook source
# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Inputs
# MAGIC
# MAGIC - Hunt Name as defined in the hunt library (conf/threat_hunting/<hunt_name>)

# COMMAND ----------

dbutils.widgets.text("sourcetype", "", label='hunt_name')

# COMMAND ----------

# DBTITLE 1,Threat Hunt Name
hunt_name = dbutils.widgets.get("sourcetype")

# COMMAND ----------

# MAGIC %md
# MAGIC # Hypothesis
# MAGIC \<\<Hypothesis\>\>
# MAGIC
# MAGIC *example: Adversaries might be accessing LSASS and extract credentials from memory.*
# MAGIC
# MAGIC # Technical Context
# MAGIC \<\<Include the technical write up here\>\>
# MAGIC
# MAGIC *example: After a user logs on, a variety of credentials are generated and stored in the Local Security Authority Subsystem Service (LSASS) process in memory. This is meant to facilitate single sign-on (SSO) ensuring a user is not prompted each time resource access is requested. The credential data may include Kerberos tickets, NTLM password hashes, LM password hashes (if the password is <15 characters, depending on Windows OS version and patch level), and even clear-text passwords (to support WDigest and SSP authentication among others.*
# MAGIC
# MAGIC # Offensive Tradecraft
# MAGIC \<\<Describe how the adversary may look to exploit the target\>\>
# MAGIC
# MAGIC *example: Adversaries look to get access to the credential data and do it so by finding a way to access the contents of memory of the LSASS process. For example, tools like Mimikatz get credential data by listing all available provider credentials with its SEKURLSA::LogonPasswords module. The module uses a Kernel32 function called OpenProcess to get a handle to lsass to then access LSASS and dump password data for currently logged on (or recently logged on) accounts as well as services running under the context of user credentials.*
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Workaround for imports
# depending on where this notebook runs from, the relative path could be ../databricks, ../../databricks, ../../../databricks, or traverse higher.
import databricks
import os
databricks.__path__.append(os.path.abspath("../../databricks"))

# COMMAND ----------

# DBTITLE 1,Import Libraries
# see above command for fixing issue: ModuleNotFoundError: No module named 'databricks.sirens'
import databricks.sirens.threathunting as TH
from databricks.sirens.config_reader import ThreatHuntReader
from databricks.sirens.utils.base_utils import BaseUtils
from databricks.sirens.exceptions import SirensException
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Get the hunt configuration
notebook_name = BaseUtils.get_notebook_name()

try:
    config_opts = ThreatHuntReader().read(hunt_name=hunt_name)
    notebook_config = config_opts.get('notebooks')
except NameError as exc:
    raise SirensException(f"Hunt Name must be entered as the widget input: {exc}") from exc
except (AttributeError, TypeError) as exc:
    raise SirensException(f"cant get config for {hunt_name}: {exc}") from exc

conf = dict([a for a in notebook_config if a.get("name") == notebook_name][0])
print(conf)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analytic I
# MAGIC
# MAGIC Describe what the analytic is looking for.
# MAGIC
# MAGIC Include the data source, event_log, event, event_id (if appropriate) (markdown table works well here.)
# MAGIC

# COMMAND ----------

# DBTITLE 1,run analytics and capture the output
# register the hunt execution
with TH.ThreatHunt(hunt_name) as hunt:
    # capture the results. Use the command name & short description to identify the results later.
    @hunt.capture_output(command_name="analytic_1", description="looks for non-system accounts accessing lsass")
    def analytic_1(df):
        # run the analytic search 
        df = df.filter()

        if not df.isEmpty():
            # optionally add a risk object                
            df = TH.add_risk_score(df, risk_object="SubjectUserName", object_type="user", impact=10, confidence=30, source=hunt_name,
                                   annotation={'mitre_technique': 'T1003', 'mitre_subtechnique': '001', 'mitre_tactic': 'Credential Access'})
        return df
analytic_1_df = analytic_1(df)
display(analytic_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Example
# MAGIC Below is an example.

# COMMAND ----------

# MAGIC %md
# MAGIC # Analytic II
# MAGIC Look for processes opening handles and accessing Lsass with potential dlls in memory (i.e UNKNOWN in CallTrace).
# MAGIC
# MAGIC |Data source|Event Provider|Relationship|Event|
# MAGIC |-----------|--------------|------------|-----|
# MAGIC |Process|Microsoft-Windows-Sysmon/Operational|Process accessed Process|10|

# COMMAND ----------

with TH.ThreatHunt(hunt_name) as hunt:
    @hunt.capture_output(command_name="analytic_2", description="Look for processes opening handles and accessing Lsass with potential dlls in memory ")
    def analytic_2(df):
        df = df.filter((F.col('Channel') == 'Microsoft-Windows-Sysmon/Operational')
            & (F.col('EventID') == 10)
            & (F.col('TargetImage').rlike('lsass.exe'))
            & (F.col('CallTrace').rlike('.*UNKNOWN*'))
            ).select('@timestamp','Hostname','SourceImage','TargetImage','GrantedAccess','SourceProcessGUID','CallTrace')
        return df
analytic_2_df = analytic_2(df)
display(analytic_2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Complete the Threat Hunt
# MAGIC
# MAGIC Register the hunt as completed in the Hunt Index.

# COMMAND ----------

hunt.end(hunt_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #Known Bypasses
# MAGIC
# MAGIC Include any known bypass information
# MAGIC
# MAGIC ##Example:
# MAGIC
# MAGIC |Idea|
# MAGIC |----|
# MAGIC |Inject into known processes accessing LSASS on a regular basis such as pmfexe.exe and as System|

# COMMAND ----------

# MAGIC %md
# MAGIC # False Positives
# MAGIC
# MAGIC Include any known false positives for analysts
# MAGIC
# MAGIC ## example: 
# MAGIC The Microsoft Monitoring Agent binary pmfexe.exe is one of the most common ones that accesses Lsass.exe with at least 0x10 permissions as System. That could be useful to blend in through the noise.

# COMMAND ----------

# MAGIC %md
# MAGIC # Hunter Notes
# MAGIC
# MAGIC Leave detailed notes for analysts triaging any information captured during the hunt
# MAGIC
# MAGIC
# MAGIC ##Example:
# MAGIC Looking for processes accessing LSASS with the 0x10(VmRead) rights from a non-system account is very suspicious and not as common as you might think.
# MAGIC
# MAGIC GrantedAccess code 0x1010 is the new permission Mimikatz v.20170327 uses for command “sekurlsa::logonpasswords”. You can specifically look for that from processes like PowerShell to create a basic signature.
# MAGIC
# MAGIC 0x00000010 = VMRead
# MAGIC
# MAGIC 0x00001000 = QueryLimitedInfo
# MAGIC
# MAGIC GrantedAccess code 0x1010 is less common than 0x1410 in large environment.
# MAGIC
# MAGIC Out of all the Modules that Mimikatz needs to function, there are 5 modules that when loaded all together by the same process is very suspicious.
# MAGIC
# MAGIC samlib.dll, vaultcli.dll, hid.dll, winscard.dll, cryptdll.dll
# MAGIC
# MAGIC For signatures purposes, look for processes accessing Lsass.exe with a potential CallTrace Pattern> /C:\Windows\SYSTEM32\ntdll.dll+[a-zA-Z0-9]{1,}|C:\Windows\System32\KERNELBASE.dll+[a-zA-Z0-9]{1,}|UNKNOWN.*/
# MAGIC
# MAGIC You could use a stack counting technique to stack all the values of the permissions invoked by processes accessing Lsass.exe. You will have to do some filtering to reduce the number of false positives. You could then group the results with other events such as modules being loaded (EID 7). A time window of 1-2 seconds could help to get to a reasonable number of events for analysis.*

# COMMAND ----------

# MAGIC %md
# MAGIC # References
# MAGIC
# MAGIC Include links to research and references: 
# MAGIC
# MAGIC ##Example
# MAGIC
# MAGIC https://adsecurity.org/?page_id=1821#SEKURLSALogonPasswords
# MAGIC
# MAGIC https://github.com/PowerShellMafia/PowerSploit/tree/dev
# MAGIC
# MAGIC http://clymb3r.wordpress.com/2013/04/09/modifying-mimikatz-to-be-loaded-using-invoke-reflectivedllinjection-ps1/
