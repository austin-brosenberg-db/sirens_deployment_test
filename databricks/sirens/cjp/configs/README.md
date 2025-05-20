# CJ Poller configs
The CJ Poller code requires two JSON files per data source:
1. _config_ : containing execution context (e.g. API endpoint, credentials)
1. _schema_ : containing mapping of desired output fields/columns from the input data fields/column (including support for select transformations)

## Config
- This file will be used via a class instantiated from the `lib/CrownJewelPollerConfig.py` library
- The full set of required fields is listed within the `_required_config_keys` list within `lib/CrownJewelPollerConfig.py`, these include:
    - `credentials` : Information on credentials to be used for data retrieval
    - `output` : Information on where to write data
    - `poller_type` : Name of CrownJewelPoller subclass to be used for data source
    - `url` : Url of data source (to be used by CrownJewelPoller.get_data())
    - `test_interval` :  Time offset (in minutes) to which cursor should be set (in dev) for testing purposes
    - `timestamp_format` : format string passed to datetime.datetime.strftime() to store test cursor
    - `authentication` : Authentication type and credentials information
- Optional parameters:
    - `actions`
    - `custom_config`
    - `record_type`
    - `transform` : to specify the processing of input data w.r.t. a specified schema, supported values are:
      - `demote` to pack columns not specified in the schema into a column named `extra`
      - `drop_extra` to drop columns not specified in the schema
      - null (or absent) to skip transformations and pass all input columns through as output
    - `cursor` : dictionary with a `location` parameter with the path to which a cursor value should be stored
    - `api_response`

## Schema
- This file will be used via a class instantiated from the `lib/CrownJewelSchema.py` library
- Methods of this class support the application of an output schema by
    - filtering by means of including specified data from input source
    - (optional) column renaming
    - (optional) basic transformations on data (e.g. str -> datetime)
