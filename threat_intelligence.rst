Threat Intelligence Guide
==========================

Sirens ships with over 30 open source threat intelligence sources, which can be enabled. Additionally, you can develop custom parsers
and normalizers to ingest data from other sources.

This section provides an overview of the threat intelligence sources that Sirens can ingest and normalize.

Threat Intelligence Collection Types
-------------------------------------
Threat Intelligence can be collected in one of two ways:

- Simple Threat Intelligence: This involves collecting data from a single source, which is easily parsed and ingested into Sirens.
- Complex Threat Intelligence: This involves collecting data, which requires writing custom parsers and normalizers to ingest into into the intelligence table.

Simple Threat Intelligence
--------------------------

Simple threat collection is done by using the in-built `simple_collector` notebook, and is configured in the
`sirens.config`, and `conf/threat_intelligence/intel.yaml` files. No additional code is required to collect and ingest the data.

Using the simple collector you specify a collection url, match REGEX pattern for the indicator, along with keys required to identify the indicator type, context etc.

See configuring the simple collector for more information.


Complex Threat Intelligence
---------------------------

For more complex threat intelligence sources, you will need to write custom parsers and normalizers to ingest the data into the intelligence table, and
then configure the collector and normalizer to run at regular intervals within `sirens.config` and `intel.yaml` files.

See the 'developing threat collectors' section for more information.


Threat Intelligence Pipeline Overview
======================================
# TODO

.. image:: ../images/threat_intelligence_pipeline.png
    :alt: Threat Intelligence Pipeline
    :align: center


Configuring Threat Intelligence
===============================

Enabling and scheduling
-----------------------
For sources that are configured with the same schedule and jobcluster, Sirens will build aggregated notebooks for collection and 
normalization to run on the same cluster, in order to optimize resource usage.


To enable threat collection for a specific source, the following conditions must be met:

1) Intelligence staging and enrichment notebooks must be enabled in the `sirens.config` file.
2) The source must exist and be enabled in the `sirens.config` file.
3) The source must be scheduled in the `sirens.config` file. 


Firstly, ensure the `intelligence` stanza is enabled in the `sirens.config` file, 
as this controls the subsequent staging and enrichment of intelligence data into the intelligence table.

- To enable a threat intelligence source, locate the [threat_intel:<source>] section in the `sirens.config` file and set the `enabled` parameter to `true`.

- Schedule the collection in line with the required timeframes - This is a cron expression that specifies the schedule for the source to run.

- Optionally, you can specify a jobcluster specification for the source.


Example:
^^^^^^^^

.. code-block:: shell

    [threat_intel:disposable-email-domains]
    enabled = true
    jobcluster = threat_intelligence_ingest
    schedule = 0 0 7 ? * * *

.. note::

    For intelligence collection, ensure the `intelligence` stanza is enabled in the `sirens.config` file, 
    as this controls the normalization of all intelligence data into the final ingelligence table. 

    .. code-block:: shell

        [threat_intel:intelligence]
        # DO NOT DISABLE IF ANY INTEL COLLECTION IS REQUIRED.
        enabled=true
        jobcluster = threat_intelligence_ingest
        schedule = 0 0 0 ? * * *


Intelligence Database and Table 
-------------------------------

Configure the intelligence database and table in the `sirens.config` file, under the `[schema:threat_intel]` stanza.

By default, the intelligence database is `sirens_intelligence` and the table is `intelligence`.

Example:
^^^^^^^^

.. code-block:: shell

    [schema:threat_intel]
    schema = sirens_intelligence
    intel_table = intelligence
    maxmind_db_location = conf/threat_intelligence/data


Maxmind Database Location
-------------------------

Sirens ships with a default maxmind database, which is used to enrich IP addresses with geolocation information. 
The location of the databases is controlled by the `maxmind_db_location` parameter in the `[schema:threat_intel]` stanza.

You can specify different locations, and / or update the installation by downloading the latest database from the Maxmind website.


Internal IP Ranges
------------------
Avoiding false positives is important when working with threat intelligence data. To prevent this, you can specify internal IP ranges in the `internal_ip_ranges.yaml`
file, which will be included in the intelligence table and can used to filter out internal IP addresses.

Example:

.. code-block:: yaml

    - ip_prefix: '83.200.10.120/30'
      provider: 'Prism Cloud'
      service: 'Global Protect'
      type: 4
      region: 'Amsterdam Office'
    - ip_prefix: '83.200.72.120/30'
      provider: 'ZScaler'
      service: 'VPN'
      type: 4
      region: 'Singapore Office'

    
Building Notebooks
------------------
Once the threat intelligence configuration is complete, you can build the notebooks required to stage and enrich the intelligence data,
by running the following command:

.. code-block:: shell

    python3 sirens.py generate_threat_intel
    

This will generate notebooks for collection, and normalization, along with two other notebooks for staging and enrichment. Once
the notebooks are deployed, they will execute as workflows using either the specified jobcluster or the default jobcluster.



Configurable Threat Intelligence Sources
========================================

The following are examples of configurable threat intelligence sources that can be enabled in Sirens, using `sirens.config`.

| Name                                  | Link                                                                                                                                     |
|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| **disposable-email-domains**          | [https://github.com/martenson/disposable-email-domains](https://github.com/martenson/disposable-email-domains)                           |
| **freemail_disposable-email-domains** | [https://github.com/martenson/disposable-email-domains](https://github.com/martenson/disposable-email-domains)                           |
| **freemail_free-email-domains**       | [https://github.com/disposable-email-domains/free-email-domains](https://github.com/disposable-email-domains/free-email-domains)         |
| **talos_ip_blacklist**                | [https://talosintelligence.com/documents/ip-blacklist](https://talosintelligence.com/documents/ip-blacklist)                             |
| **maltrail-dynamic_domain**           | [https://github.com/stamparm/maltrail](https://github.com/stamparm/maltrail)                                                             |
| **maltrail-parking_site**             | [https://github.com/stamparm/maltrail](https://github.com/stamparm/maltrail)                                                             |
| **maltrail-parking_ip**               | [https://github.com/stamparm/maltrail](https://github.com/stamparm/maltrail)                                                             |
| **maltrail-ipinfo**                   | [https://github.com/stamparm/maltrail](https://github.com/stamparm/maltrail)                                                             |
| **cinscore-ci_badguys**               | [http://cinsscore.com/list/ci-badguys.txt](http://cinsscore.com/list/ci-badguys.txt)                                                     |
| **blocklist_de_ssh**                  | [https://www.blocklist.de/downloads/export-ssh-ips_all.txt](https://www.blocklist.de/downloads/export-ssh-ips_all.txt)                   |
| **blocklist_de_mail**                 | [https://www.blocklist.de/downloads/export-mail-ips_all.txt](https://www.blocklist.de/downloads/export-mail-ips_all.txt)                 |
| **blocklist_de_imap**                 | [https://www.blocklist.de/downloads/export-imap-ips_all.txt](https://www.blocklist.de/downloads/export-imap-ips_all.txt)                 |
| **blocklist_de_apache**               | [https://www.blocklist.de/downloads/export-apache-ips_all.txt](https://www.blocklist.de/downloads/export-apache-ips_all.txt)             |
| **blocklist_de_ftp**                  | [https://www.blocklist.de/downloads/export-ftp-ips_all.txt](https://www.blocklist.de/downloads/export-ftp-ips_all.txt)                   |
| **blocklist_de_sip**                  | [https://www.blocklist.de/downloads/export-sip-ips_all.txt](https://www.blocklist.de/downloads/export-sip-ips_all.txt)                   |
| **blocklist_de_bots**                 | [https://www.blocklist.de/downloads/export-bot-ips_all.txt](https://www.blocklist.de/downloads/export-bot-ips_all.txt)                   |
| **blocklist_de_ircbot**               | [https://www.blocklist.de/downloads/export-irc-ips_all.txt](https://www.blocklist.de/downloads/export-irc-ips_all.txt)                   |
| **blocklist_de_bruteforcelogin**      | [https://www.blocklist.de/downloads/export-login-ips_all.txt](https://www.blocklist.de/downloads/export-login-ips_all.txt)               |
| **binarydefense**                     | [https://www.binarydefense.com/banlist.txt](https://www.binarydefense.com/banlist.txt)                                                   |
| **dan-me-uk_toe_exit_nodes**          | [https://www.dan.me.uk/torlist/](https://www.dan.me.uk/torlist/)                                                                         |
| **zerodot-coin_blocker_lists**        | [https://github.com/ZeroDot1/CoinBlockerLists](https://github.com/ZeroDot1/CoinBlockerLists)                                             |
| **Neo23x0_c2_domains**                | [https://github.com/Neo23x0/signature-base](https://github.com/Neo23x0/signature-base)                                                   |
| **rutgers_attackers**                 | [http://report.rutgers.edu/DROP/attackers](http://report.rutgers.edu/DROP/attackers)                                                     |
| **Neo23x0_hash_iocs**                 | [https://github.com/Neo23x0/signature-base](https://github.com/Neo23x0/signature-base)                                                   |
| **charles-the-haleys_ssh_brute_force**| [https://charles.the-haleys.org/ssh_dico_attack_with_timestamps.php?days=1](https://charles.the-haleys.org/ssh_dico_attack_with_timestamps.php?days=1)]|
| **danger-rulez_brute_force_ips**      | [http://danger.rulez.sk/projects/bruteforceblocker/blist.php](http://danger.rulez.sk/projects/bruteforceblocker/blist.php)               |
| **infobloxopen_threat_intelligence**  | [https://www.infoblox.com/open-data-initiative/](https://www.infoblox.com/open-data-initiative/)                                         |
| **drb-ra_c2_domains**                 | [https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/domainC2s-30day-filter-abused.csv](https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/domainC2s-30day-filter-abused.csv)|
| **drb-ra_c2_ips**                     | [https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv](https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv)|
| **drb-ra_vpn_ips**                    | [https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/vpn/NordVPNIPs.csv](https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/vpn/NordVPNIPs.csv)|
| **alienvault_ip_reputation**          | [https://reputation.alienvault.com/reputation.generic](https://reputation.alienvault.com/reputation.generic)                             |
| **abusech_sslbl**                     | [https://sslbl.abuse.ch/blacklist/](https://sslbl.abuse.ch/blacklist/)                                                                   |
| **magicsword-io_malicious_hash**      | [https://raw.githubusercontent.com/magicsword-io/LOLDrivers/main/detections/hashes/samples_malicious.sha256](https://raw.githubusercontent.com/magicsword-io/LOLDrivers/main/detections/hashes/samples_malicious.sha256)|
| **magicsword-io_vulnerable_hash**     | [https://raw.githubusercontent.com/magicsword-io/LOLDrivers/main/detections/hashes/samples_vulnerable.sha256](https://raw.githubusercontent.com/magicsword-io/LOLDrivers/main/detections/hashes/samples_vulnerable.sha256)|
| **public_suffix_list**                | [https://publicsuffix.org/list/public_suffix_list.dat](https://publicsuffix.org/list/public_suffix_list.dat)                             |
| **abusech**                           | [https://abuse.ch](https://abuse.ch)                                                                                                     |
| **cloud_ranges**                      | [https://github.com/eshork/Cloud-IPs](https://github.com/eshork/Cloud-IPs)                                                               |
| **cve**                               | [https://nvd.nist.gov/vuln/data-feeds](https://nvd.nist.gov/vuln/data-feeds)                                                             |
| **eset_malware**                      | [https://github.com/eset/malware-ioc/](https://github.com/eset/malware-ioc/)                                                             |
| **itisac**                            | [https://station.trustar.co/](https://station.trustar.co/)                                                                               |
| **misp_warnings**                     | [https://www.misp-project.org](https://www.misp-project.org)                                                                             |
| **phishtank**                         | [https://www.phishtank.com/developer_info.php](https://www.phishtank.com/developer_info.php)                                             |
| **private_intelligence**              | *Not publicly available*                                                                                                                 |
| **tranco**                            | [https://tranco-list.eu](https://tranco-list.eu)                                                                                         |





Developing Threat Collectors
============================

The following section outlines, how to use the in-built simple_collector for additional sources, and how to develop custom parsers for complex threat intelligence sources.


Simple Collector
----------------
For simple feeds, collected over http(s), in csv / text and unauthenticated, you can use the `simple_collector` notebook to collect and ingest the data.

To do this, add a new entry in the `intel.yaml` file, and specify the following parameters:

.. code-block:: yaml


    source_name: <intel_source> # must match the sirens.config stanza entry.
    ingest_notebook: simple_ingest
    collector:
      name: simple_collector
      options:
        url: <hosting URL>
        match_pattern: r'^(?P<indicator>[a-fA-F0-9]{64})$'
    context:
      type: <threat_type>
      indicator_type: <indicator_type>
      tags: <arbitrary tags>

Other options and parameters can be found in `intel.yaml.spec` section.

Add a matching entry in the `sirens.config` file, under the `threat_intel` stanza, and set the `enabled` parameter to `true`, along 
with the jobcluster (optional), and the desired collection schedule.


Custom collector
----------------
To develop custom collector notebooks, firstly create a new notebook in the `notebooks/threat_intelligence/collectors` directory and add the following code:

.. code-block:: python3

    # Databricks notebook source
    import os

    sirens_home = spark.conf.get("sirens.home", None)
    if sirens_home is not None:
    os.chdir(sirens_home)

    import databricks
    import os
    databricks.__path__.append(os.path.abspath("../../../databricks"))

    # COMMAND ----------

    from databricks.sirens.intel.abusech import *
    from databricks.sirens.intel.utils import write_ignore_duplicates
    from databricks.sirens.utils.schema_utils import Schemas
    from databricks.sirens.modules import get_modules
    module = get_modules()

    # COMMAND ----------
    database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)


Add the required code to collect the data, and then write the data using the `write_ignore_duplicates` function.


Example: 
^^^^^^^^

.. code-block:: python3

    TABLE = 'my_custom_source' + '_raw'

    write_ignore_duplicates(
    spark,
    df.collect(),
    f'{database}.{TABLE}'
    )

The dataframe / raw data must be written using the following raw schema:

| Field          | Type           | Nullable |
|----------------|----------------|----------|
| _source        | StringType      | True     |
| _type          | StringType      | True     |
| _raw_record    | StringType      | True     |
| _collection_ts | TimestampType   | True     |


Custom Ingest
-------------
The ingest notebook is used to normalize the raw data into a format that can be staged and subsequently enriched.

1) Generate a new notebook with an `_ingest.py` suffix in the `notebooks/threat_intelligence/normalize` directory, and add the following code:

.. code-block:: python3

    # Databricks notebook source
    import os

    sirens_home = spark.conf.get("sirens.home", None)
    if sirens_home is not None:
    os.chdir(sirens_home)

    # COMMAND ----------

    import databricks_sirens.fix_package_import
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from databricks.sirens.utils.schema_utils import Schemas
    from databricks.sirens.utils.global_config import ConfigReader
    from databricks.sirens.modules import get_modules
    from databricks.sirens.utils.base_utils import BaseUtils
    from databricks.sirens.logging import get_logger
    logger = get_logger('threat_collection: '+os.path.basename(BaseUtils.get_notebook_path()))
    module = get_modules()

    # COMMAND ----------

    logger.info("executing")
    database = spark.conf.get('intel.database', Schemas.get_schema(module=module.THREAT_INTEL).name)

    # COMMAND ----------
    RAW_TABLE = 'my_custom_source' + '_raw'
    raw_df = spark.readStream.option('ignoreChanges', 'true').table(f'{database}.{TABLE}')


2) Execute custom logic to normalize the data, into the following schema definition:

.. note::

    All Threat Types (type) and Indicator Types (indicator_type) can be found in the Threat and Indicator Types table below.



| Column Name     | Data Type                        | Nullable |
|-----------------|----------------------------------|----------|
| indicator       | StringType                       | True     |
| type            | StringType                       | True     |
| indicator_type  | StringType                       | True     |
| source          | StringType                       | True     |
| source_locator  | StringType                       | True     |
| tlp             | StringType                       | True     |
| tags            | ArrayType(StringType)            | True     |
| flags           | MapType(StringType, StringType)  | True     |
| context         | MapType(StringType, StringType)  | True     |
| date_first      | DateType                         | True     |
| date_last       | DateType                         | True     |
| _collection_ts  | TimestampType                    | True     |
| _raw_record     | StringType                       | True     |


.. code-block:: python3

    schema = StructType([
        StructField("indicator", StringType(), True),
        StructField("type", StringType(), True),
        StructField("indicator_type", StringType(), True),
        StructField("source", StringType(), True),
        StructField("source_locator", StringType(), True),
        StructField("tlp", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("flags", MapType(StringType(), StringType()), True),
        StructField("context", MapType(StringType(), StringType()), True),
        StructField("date_first", DateType(), True),
        StructField("date_last", DateType(), True),
        StructField("_collection_ts", TimestampType(), True),
        StructField("_raw_record", StringType(), True)
    ])


3) You can then write the normalized dataframe to the source normalized table using the following code:

.. code-block:: python3

    TABLE = 'my_custom_source' + '_normalized'
    checkpoint_dir = os.path.join(ConfigReader.get_config_key(ConfigReader.read(), "default", 'scratch_dir'), "checkpoints", database, "threat_intel", TABLE)

    query = (normalized_df.writeStream
        .option("checkpointLocation", checkpoint_dir)
        .trigger(once=True)
        .toTable(f'{database}.{TABLE}')
        )
    query.awaitTermination()
    logger.info("completed")


.. note::

    Providing the normalized table has the suffix `_normalized`, the data will be automatically staged and enriched by the intelligence pipeline.


4) Generate the notebooks for deployment, using the Configure Threat Intelligence section above.


Threat Intelligence Table Schema
================================

The final intelligence table schema is defined as follows:


| Field            | Type                                 | Nullable |
|------------------|--------------------------------------|----------|
| indicator        | StringType                           | True     |
| type             | StringType                           | True     |
| indicator_type   | StringType                           | True     |
| source           | StringType                           | True     |
| source_locator   | StringType                           | True     |
| tlp              | StringType                           | True     |
| tags             | ArrayType(StringType, True)          | True     |
| flags            | MapType(StringType, BooleanType, True)| True    |
| context          | MapType(StringType, StringType, True)| True     |
| date_first       | DateType                             | True     |
| date_last        | DateType                             | True     |
| ip_enrichment    | ArrayType(StructType(...))           | True     |
|   - ip           | StringType                           | True     |
|   - rdns         | StringType                           | True     |
|   - asn          | IntegerType                          | True     |
|   - asname       | StringType                           | True     |
|   - country      | StringType                           | True     |
|   - city         | StringType                           | True     |
|   - latitude     | FloatType                            | True     |
|   - longitude    | FloatType                            | True     |
| hash_enrichment  | StructType(...)                      | True     |
|   - md5          | StringType                           | True     |
|   - sha1         | StringType                           | True     |
|   - sha256       | StringType                           | True     |
|   - ssdeep       | StringType                           | True     |
|   - filetype     | StringType                           | True     |
|   - vt_detections| IntegerType                          | True     |



Threat Types and Indicator Types
================================
Sirens intelligence table provides for a type and indicator column, which can be used to classify the threat type and indicator type respectively. These
fields are not constrained to specific values, however the following table presents possible values for consistency. 


| Type           | Description          |
| ---------------|----------------------|
| domain         | Domain Name          |
| email          | Email Address        |
| ip             | IP Address           |
| url            | URL                  |
| file_hash      | File Hash            |


Indicator Types
---------------

Indicator Types can be sub classified into different types based on the threat they represent. The following are examples of indicator types used in Sirens.



| Indicator Type     | Threat Type       | Indicator Name         | Indicator Description                                                                 |
|--------------------|-------------------|------------------------|---------------------------------------------------------------------------------------|
| actor_ip           | p2p               | Actor IP               | IP address associated with a system involved in malicious activity.                   |
| adware_domain      | adware            | Adware Domain          | A domain name associated with adware or other Potentially Unwanted Applications (PUA). |
| anon_proxy         | anonymization      | Anonymous Proxy IP     | IP address of the system on which anonymous proxy software is hosted.                 |
| anon_vpn           | anonymization      | Anonymous VPN IP       | IP address associated with commercial or free Virtual Private Networks (VPN).         |
| apt_domain         | apt               | APT Domain             | Domain name associated with a known Advanced Persistent Threat (APT) actor used for command and control, launching exploits, or data exfiltration. |
| apt_email          | apt               | APT Email              | Email address used by a known Advanced Persistent Threat (APT) actor for sending targeted, spear phishing emails. |
| apt_ip             | apt               | APT IP                 | IP address associated with known Advanced Persistent Threat (APT) actor for command and control, data exfiltration, or targeted exploitation. |
| apt_md5            | apt               | APT MD5 File Hash      | MD5 hash of a malware sample used by a known Advanced Persistent Threat (APT) actor.   |
| apt_subject        | apt               | APT Subject Line       | Email subject line used by a known Advanced Persistent Threat (APT) actor.            |
| apt_ua             | apt               | APT User Agent         | User agent string used by a known Advanced Persistent Threat (APT) actor.             |
| apt_url            | apt               | APT URL                | URL used by a known Advanced Persistent Threat (APT) actor for command and control, launching web-based exploits, or data exfiltration. |
| bot_ip             | bot               | Infected Bot IP        | IP address of an infected machine acting as an autonomous bot.                        |
| brute_ip           | brute             | Brute Force IP         | IP address associated with password brute force activity.                             |
| c2_domain          | c2                | Malware C&C Domain Name | Domain name used by malware for command and control communication.                    |
| c2_ip              | c2                | Malware C&C IP         | IP address used by malware for command and control communication.                     |
| compromised_domain | compromised       | Compromised Domain     | Domain name of website or server that has been compromised.                           |
| compromised_email  | compromised       | Compromised Account Email | Email address that has been compromised and/or taken over by a threat actor.          |
| compromised_ip     | compromised       | Compromised IP         | IP address of website or server that has been compromised.                            |
| compromised_url    | compromised       | Compromised URL        | URL of the website or server that has been compromised.                               |
| ddos_ip            | ddos              | DDOS IP                | IP address associated with Distributed Denial of Service (DDoS) attacks.              |
| dyn_dns            | dyn_dns           | Dynamic DNS            | Domain name used for hosting Dynamic DNS services.                                    |
| exfil_domain       | exfil             | Data Exfiltration Domain | Domain name associated with the infrastructure used for data exfiltration.            |
| exfil_ip           | exfil             | Data Exfiltration IP   | IP address used for data exfiltration.                                                |
| exfil_url          | exfil             | Data Exfiltration URL  | URL used for data exfiltration.                                                       |
| exploit_domain     | exploit           | Exploit Kit Domain     | Domain name associated with the web server hosting an exploit kit or launching web-based exploits. |
| exploit_ip         | exploit           | Exploit Kit IP         | IP address associated with the web server hosting an exploit kit or launching web-based exploits. |
| exploit_url        | exploit           | Exploit Kit URL        | URL used for launching web-based exploits.                                            |
| geolocation_url    | anomalous         | IP Geolocation URL     | URL that can be used to provide IP Geo location services.                             |
| hack_tool          | hack_tool         | Hacking Tool           | MD5 hash of general hacking software tools used by threat actors.                     |
| ipcheck_url        | anomalous         | IP Check URL           | URL that can be used to provide IP checking services, such as echoing the Internet facing IP address of the client. |
| mal_domain         | malware           | Malware Domain         | Domain contacted by malware sample; could be for command and control commands, or to check if the client is online. |
| mal_email          | malware           | Malware Email          | Email address used to send malware through malicious links or attachments.            |
| mal_ip             | malware           | Malware C&C IP         | IP address contacted by malware sample; could be for command and control commands, or to check if the client is online. |
| mal_md5            | malware           | Malware MD5 File Hash  | MD5 hash of malware sample.                                                          |
| mal_ua             | malware           | Malware User Agent     | User agent string used by malware sample when communicating via HTTP.                 |
| mal_url            | malware           | Malware URL            | URL contacted by malware sample when run on an infected host.                         |
| p2pcnc             | p2p               | Peer-to-Peer C&C IP Address | IP address associated with a peer-to-peer command and control infrastructure.        |
| parked_ip          | parked            | Domain Parking IP      | An IP address used for parking newly registered or inactive domain names.             |
| pastesite_url      | data_leakage      | Paste Site URL         | A URL that can be used for sharing pastes or text content anonymously.                |
| phish_domain       | phish             | Phishing Domain        | A domain used to perform phishing or spear phishing attacks or contained in a phishing email. |
| phish_email        | phish             | Phishing Email Address | An email address associated with sending phishing or spear phishing emails to victims. |
| phish_url          | phish             | Phishing URL           | A URL used to perform phishing or spear phishing attacks or contained in a phishing email. |
| proxy_ip           | anonymization     | Open Proxy IP          | IP address hosting open or anonymous proxy software. Allows user to hide their IP address from target. |
| scan_ip            | scan              | Scanning IP            | IP address observed to perform port scanning and vulnerability scanning activities.    |
| sinkhole_domain    | sinkhole          | Sinkhole Domain        | A domain name that researchers or security companies typically sinkhole.              |
| sinkhole_ip        | sinkhole          | Sinkhole IP            | An IP address that is known to be used to sinkhole malicious domain names.            |
| spam_domain        | spam              | Spam Domain            | A malicious domain name contained in the SPAM email messages.                         |
| spam_email         | spam              | Spam Email             | Email address associated with sending SPAM emails to victims.                         |
| spam_ip            | spam              | Spammer IP             | An IP address that is known to send SPAM emails.                                      |
| spam_url           | spam              | Spam URL               | A malicious URL contained in the SPAM email messages.                                 |
| speedtest_url      | anomalous         | Speed Test URL         | A URL that can be used to perform internet speed tests or bandwidth measurements of the client's network connection. |
| ssh_ip             | brute             | SSH Brute Force IP     | IP addresses associated with SSH brute force attempts.                                |
| suppress           | suppress          | Suppress               | Not a true iType. Used by Arcsight for suppressing false positives.                   |
| suspicious_domain  | suspicious        | Suspicious Domain      | A domain name that appears to be registered for suspect reasons, but may not be associated with known malicious activity yet. |
| tor_ip             | tor               | TOR Node IP            | An IP address operating as part of The Onion Router (TOR) Network, also known as a TOR exit node. |
| torrent_tracker_url| p2p               | Torrent Tracker URL    | A URL used for tracking BitTorrent file transfer activity.                            |
| vpn_domain         | anonymization     | Anonymous VPN Domain   | A domain name associated with commercial or free Virtual Private Networks (VPN).       |
| vps_ip             | vps               | Cloud Server IP        | An IP address that is used for hosting Virtual Private Servers (VPS) or other server rentals. |


intel.yaml Specification
========================

.. code-block:: yaml

    - source_name: <collector_source>
        # required
        # must match sirens.config stanza entry
      ingest_notebook: simple_ingest
        # required
        # ingest notebook to use
      collector:
        name: simple_collector
            # required
            # collector notebook to use
        options:
          url: https://sslbl.abuse.ch/blacklist/sslipblacklist.csv
            # required
            # url to collect data from
          match_pattern: r'^(?P<first_seen>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),(?P<indicator>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}),(?P<dst_port>\d+)$'
            # required
            # regex pattern to match the indicator
          ignore_pattern:
            # optional
            # REGEX to ignore specific lines
      context:
        type: ip
            # optional
            # may also come from match_pattern
        indicator_type: c2_ip
            # recommended
            # indicator type