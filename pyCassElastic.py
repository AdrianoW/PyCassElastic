import logging
from cassandra.cluster import Cluster, BatchStatement
from elasticsearch import Elasticsearch
from datetime import datetime

log = logging.getLogger('sync-cass-elastic')

class SyncCassElastic():

    def __init__(self, config={}):
        '''soc
        Syncronize the data from Cassandra to/from ElasticSearch
        :param config: configuration dictionary. Expects cassandra, elasticsearch, schedule
        :return:
        '''
        if not config:
            log.warning('No config file passed for the sync class')

        # general information
        self.default_time = 10
        self.time_last_run = None
        self.time_this_run = None
        self.time_delta = None
        self.lastRunFileName = 'lastruntime.log'

        # cassandra configuration
        self.cassandra = config.get('cassandra', None)
        if not self.cassandra:
            self.cassandra = {}
            self.cassandra['url'] = None
            self.cassandra['keyspace'] = None
            self.cassandra['cluster'] = None
            self.cassandra['session'] = None

        # elastic search configuration
        self.elastic = config.get('elasticsearch', None)
        if not self.elastic:
            self.elastic = {}
            self.elastic['url'] = None
            self.elastic['session'] = None

        # connect to both databases

    def setup(self):
        '''
        Will connect to databases and setup things. Raise exception in case of a problem
        :return: None
        '''
        self._init_cassandra()
        self._init_elasticsearch()
        self._getLastRunTime()
        self._getThisRunTime()


    def _getLastRunTime(self):
        '''
        Get the last time it was sync. If the file does not exists, it will create an exception and ask the user to
        create the file with the desired date
        '''
        try:
            f = open(self.lastRunFileName)
        except IOError as e:
            log.error('A file %s should exists with the last run date in the format YYYYMMDD HH:MM'
                %self.lastRunFileName)
            raise e
        else:
            with  f:
                date = f.read()
            self.time_last_run = datetime.strptime(date, '%Y%m%d %H:%M')

    def _getThisRunTime(self):
        '''
        Get this run time and will create a delta to be used on the queries.
        '''
        # get the current time rounded to the minute before it started
        self.time_this_run = datetime.strptime(datetime.now().strftime('%Y%m%d %H:%M'), '%Y%m%d %H:%M')
        if self.time_last_run:
            self.time_delta = self.time_this_run - self.time_last_run

    def _writeThisRunTime(self):
        '''
        Will create a file with the run time in format YYYYMMDD HH:MM
        '''
        with open(self.lastRunFileName, 'w') as f:
            f.write(self.time_this_run.strftime('%Y%m%d %H:%M'))

    def run(self):
        pass

    def prepareQueries(self):
        '''
        Will create prepared statements for the queries, running faster
        :return:
        '''
        pass

    def syncCassandra(self):
        # read all the tables and insert the rows in elastic search

        #
        pass

    def _init_cassandra(self):
        '''
        Connects to cassandra. Static so it can be called directly
        :param url: url to connect to cassandra. Null means local machine
        :param keyspace: keyspace to connect to
        :return: cassandra session
        '''
        self.cassandra['cluster'] = Cluster(self.cassandra['url'])
        self.cassandra['session'] = self.cassandra['cluster'].connect(self.cassandra['keyspace'])
        log.info('Connected to Cassandra: %s / %s' %(self.cassandra['url'], self.cassandra['keyspace']))

    def _init_elasticsearch(self):
        '''
        Connects to cassandra. Static so it can be called directly
        :param url: url to connect to cassandra. Null means local machine
        :param keyspace: keyspace to connect to
        :return: cassandra session
        '''
        self.elastic['session'] = Elasticsearch(self.elastic['url'])
        log.info('Connected to Elasticsearch: %s' %(self.elastic['url']))

    def insert_cassandra(self):
        pass

    def get_cassandra_latest(self):

        rows = session.execute('''SELECT columnfamily_name, column_name
                    FROM system.schema_columns WHERE keyspace_name='test' ''')


        rows = session.execute('SELECT name, age, email FROM users')
        for user_row in rows:
            print user_row.name, user_row.age, user_row.email

    def insert_elasticsearch(self):
        '''
        Will insert data on elasticsearch
        http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-update.html
        https://github.com/elasticsearch/elasticsearch-py/blob/master/example/load.py
        :return:
        '''
        es = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
        es = Elasticsearch()
        es.bulk()

    def get_elasticsearch_latest(self):
        res = es.get(index="test-index", doc_type='tweet', id=1)
        result = es.get('/_search', data={
            "query": {
                "filtered": {
                    "query" : { "match_all" : {}},
                    "filter": {
                        "range": {
                           "@timestamp": {
                              "from": datetime(2013, 3, 11, 8, 0, 30,   tzinfo=paristimezone),
                               "to": datetime(2013, 3, 12, 11, 0, 30, tzinfo=paristimezone)
                            }
                        }
                    }
                }
            }
        })



    def shutdown(self):
        '''
        Close connections and clean ups
        :return: None
        '''
        if self.cassandra['cluster']:
            self.cassandra['cluster'].shutdown()
            log.info('Disconnected from Cassandra')


