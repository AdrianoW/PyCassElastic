# PyCassElastic
Class to make data synchronization between a Cassandra DB and Elasticsearch.

It connects to Cassandra, get all data from the tables with a date field between last run and now and write them to indexes on Elastic Search. Does the opposite as well.

## Business Scenario
- The business needs to sync both systems;
- Cassandra is a DB and Elasticsearch is more a search tool. Most of the time data will be transfered from C* to ES, so the CS->ES operation should be faster than ES->CS;
- The data in Cassandra will grow, so bring all the data and filer it at the app will be impossible sometime;
- The data is written to C* and ES with a timestamp, version (unix time millisecond) and source of the system;

## Features
- Sync more than one table. Just create other structures on `syncs`;
- Filter Cassandra table by timestamp, not bringing all data along;
- Use Elasticsearch as the version conflict mediator;
- Brings to C* only information that did not come from Cassandra. Use the `ignore_same_source` and `source_id` for that;
- The sync respect the newest version of the documents using and id field as the key and a version field as the criteria to merge;

## PS:
With the `filter_date` flag set, it will filter the C* data using the date field. In the tests, it was slower than bringing all the data, but if the data in C* was big enough, that would probably not happen.
The best practice would probably have the main table with a partition key by hour or day (depending on the amount of data) and filter using this parameter and an auxiliary table with just the information ID and the partition number so that the ID information could be easily retrieved in case it was needed.

## Setup
- Install python packages with pip install -r /path/to/requirements.txt
- Use the file config.json as the base to insert Cassandra and Elasticsearch configurations
- Create or insert a date in the format YYYYMMDD HH:MI on the file lastruntime.log to synchronise all data from this date
- Chmod the .sh to be runnable:
```
chmod +x run.sh
chmod +x kill.sh
chmod +x status.sh
```

## Config.json
```
{
  "period": 1, # in seconds
  "cassandra":{
    "url":["localhost"], # url to connect to
    "keyspace":"test"  # keyspace of the tables
  },
  "elasticsearch":{
    "url":["localhost"] # url to connect to
  },
  "syncs":[ # list of sync to be executed. More than one can occur
    {
      "name":"sync_name1", # name of the sync. Will appear in some log messages
      "comments": "from cassandra to elastic", # used for nothing. Just for own organization
      "id_col":"id", # the id column. Primary key
      "date_col": "date", # timestamp of the data. If "filter_date": true, will query this field to filter by date
      "filter_date":true, # if true, will filter C* data by date. The date_col should be part of the key as clustering key
      "version_col":"version", # version of the documents. a unix time of date used to resolve conflict
      "ignore_same_source": true, # true - filter data on ES data does not belong to cassandra
      "cassandra": {
        "table": "bogus", # table that will be sync. all fields will be sync
        "source_id":"cassandra" # cassandra data signature
      },
      "elasticsearch":{
        "index": "bogus_index", # index to write to
        "type": "bogusdoc", # type of doc
        "source_id":"Elastic" # elastic data signature
      }
    }
  ]
```

## Run the process
./run.sh
## Check running
./status.sh
## Kill the process
./kill.sh
## Tests
- The tests will connect to the databases and create bogus data to copy around. After the tests, data is deleted. The parameter TestSyncClass.setup.data_amt controls the number of rows to be created.

## Possible TODOs
- Change the programming to use a date partition key and create an auxiliary table/process to populate the ID/date infomation
- make the bulk insert on cassandra multithread
