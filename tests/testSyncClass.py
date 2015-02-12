import unittest
import os
from testfixtures import LogCapture
from os.path import abspath, dirname
from utilsTest import id_generator
from datetime import datetime, timedelta
from elasticsearch.helpers import bulk
import json

# hack to test on a different dir
import sys
sys.path.append(dirname(abspath(__file__))+'/..')
from pyCassElastic import SyncCassElastic, Cluster, Elasticsearch, BatchStatement
from utils import timeit

class TestSyncClass(unittest.TestCase):

    def setUp(self):
        # create bogus json
        self.filename = 'bogus.json'

        # get current dir
        self.cur_dir = dirname(abspath(__file__))

        # create a valid json
        with open(self.filename, 'w') as f:
            f.write('''{
                      "cassandra":{
                        "url":["localhost"],
                        "keyspace":"test"
                      },
                      "elasticsearch":{
                        "url":["localhost"]
                      }
                    }''')

        # create config structure
        with open(self.filename) as f:
            self.config = json.load(f)

        # create cassandra structure
        clus = Cluster(['localhost'])
        sess = clus.connect()
        rows = sess.execute('''select * from system.schema_keyspaces where keyspace_name = 'test' ''')
        if len(rows)>0:
            sess.execute('''DROP keyspace test ''')
        sess.execute('''CREATE KEYSPACE test
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };''')

        # save for later
        cas = {}
        cas['cluster'] = clus
        cas['session'] = sess
        self.cassandra = cas
        self.data_amt = 1000

        # connect to elasticsearch and create the data
        self.elastic = {}
        self.elastic['session'] = Elasticsearch()


    def tearDown(self):
        # remove files
        os.remove(self.filename)

        # delete cassandra structure created for tests
        self.cassandra['session'].execute('''DROP keyspace test ''')
        self.cassandra['cluster'].shutdown()

        # delete elasticsearch indexes
        self.elastic['session'].indices.delete(index='bogus_data', ignore=[400, 404])

    def testLoadConfig(self):
        '''
        check that the config loading works properly
        :return:
        '''
        #  check if it is complaining about not having a json
        l = LogCapture('sync-cass-elastic')
        sync = SyncCassElastic()
        l.check(('sync-cass-elastic', 'WARNING', 'No config file passed for the sync class'),)
        l.clear()

        # check if it has setup correctly
        sync = SyncCassElastic(self.config)

        # assert the last time log file is created and properly
        os.remove(sync.lastRunFileName) if os.path.exists(sync.lastRunFileName) else None
        self.assertRaises(IOError, sync.setup)
        with open(sync.lastRunFileName, 'w') as f:
            f.write(' ')
        self.assertRaises(ValueError, sync.setup)

        # check connection
        l.clear()
        minutesAgo = self._createLastRunFile()
        sync.setup()
        l.check(('sync-cass-elastic', 'INFO', u"Connected to Cassandra: [u'localhost'] / test"),
                ('sync-cass-elastic', 'INFO', u"Connected to Elasticsearch: [u'localhost']"),)
        self.assertEqual(minutesAgo.strftime('%Y%m%d %H:%M'), sync.time_last_run.strftime('%Y%m%d %H:%M'),
                         'The time should be the same')
        self.assertNotEqual(sync.time_this_run, None, 'Time of this run should be filled')
        self.assertNotEqual(sync.time_delta, None, 'Time of this run should be filled')

        # get rid of the logger checker
        l.uninstall()
        os.remove(sync.lastRunFileName) if os.path.exists(sync.lastRunFileName) else None

    def testFromCassandraToElastic(self):
        pass

    def testFromElasticToCassandra(self):
        pass


    @timeit
    def _createCassandraData(self, currTime=None):
        '''
        Will create 2 'tables' and populate data
        :return:
        '''
        session = self.cassandra['session']
        if not currTime:
            currTime = datetime.now()

        # create a table
        session.execute(''' CREATE TABLE test.bogus_data (
                                      hourKey int,
                                      key int,
                                      text varchar,
                                      source varchar,
                                      date timestamp,
                                      PRIMARY KEY ((hourKey),date, key)
                                    );''')

        #Prepare the statements involved in a profile update
        user_track_statement = session.prepare(
            "INSERT INTO test.bogus_data (hourKey, key, text, source, date) VALUES (?, ?, ?, ?, ?)")

        # add the prepared statements to a batch
        count = 0
        batch = BatchStatement()
        for i in range(1,self.data_amt):
            batch.add(user_track_statement,
                  [int(currTime.strftime('%Y%m%d%H')), i, id_generator(10), 'CASSANDRA',currTime])
            count += 1

            # every x records, commit. The parameter 65000 was giving timeout
            if (count%65000)==0:
                # execute the batch
                session.execute(batch)
                # hack to get around the 65k limit of python driver
                batch._statements_and_parameters = []
                count = 0

        if count>0:
            session.execute(batch)
            batch._statements_and_parameters = []

    @timeit
    def _createElasticSearchData(self, currTime=None):
        '''
        Creates data on an index on Elasticsearch
        :return:
        '''
        es = self.elastic['session']
        es.indices.create(index='bogus_data', ignore=400)
        if not currTime:
            currTime = datetime.now()

        data = []
        for i in range(1,self.data_amt):
            action = {'_op_type': 'index',
                      '_type': 'document',
                      '_id': i,
                      'doc': {
                          'text': id_generator(10),
                          'source': 'Elastic',
                          'date': currTime
                      }
                }
            data.append(action)

        # write to elasticsearch
        bulk(es, data, chunk_size=700, index='bogus_data')

    def _createLastRunFile(self):
        '''
        Creates a dummy file with 15 minutes ago from now
        '''
        minutesAgo = datetime.now() - timedelta(minutes=5)
        sync = SyncCassElastic(self.config)
        with open(sync.lastRunFileName, 'w') as f:
            f.write(minutesAgo.strftime('%Y%m%d %H:%M'))

        return minutesAgo

