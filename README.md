# PyCassElastic
Class to make data synchronization between a Cassandra DB and Elasticsearch.

It connects to Cassandra, get all data from the tables with a date field between last run and now and write them to indexes on Elastic Search. Does the opposite as welll.

The sync respect the newest version of the documents using and id field as the key.

## Setup
- Install python packages with pip install -r /path/to/requirements.txt
- Use the file config.json as the base to insert Cassandra and Elasticsearch configurations
- Create or insert a date in the format YYYYMMDD HH:MI on the file lastruntime.log to sincronise all data from this date
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
      "version_col":"version", # version of the documents. a unix time of date used to resolve conflict
      "cassandra": {
        "table": "bogus" # table that will be sync. all fields will be sync
      },
      "elasticsearch":{
        "index": "bogus_index", # index to write to
        "type": "bogusdoc" # type of doc
      }
    }
  ]
```

## Run the process
run.sh

## Tests
- The tests will connect to the databases and create bogus data to copy around. After the tests, data is deleted. There is a parameter on TestSyncClass.setup with the number of rows to be created.
