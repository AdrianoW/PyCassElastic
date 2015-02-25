# PyCassElastic
Class to make data synchronization between a Cassandra DB and Elasticsearch.

It connects to Cassandra, get all data from the tables with a date field between last run and now and write them to indexes on Elastic Search. Does the opposite as welll.

The sync respect the newest version of the documents using and id field as the key.

## Setup
- Install python packages with pip install -r /path/to/requirements.txt
- Use the file config.json as the base to insert Cassandra and Elasticsearch configurations
- Create or insert a date in the format YYYYMMDD HH:MI on the file lastruntime.log to sincronise all data from this date

## Run example
python main.py

## Tests
- The tests will connect to the databases and create bogus data to copy around. After the tests, data is deleted.
