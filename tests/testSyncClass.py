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

sys.path.append(dirname(abspath(__file__)) + '/..')
from  pyCassElastic import SyncCassElastic, Cluster, Elasticsearch, BatchStatement
from utils import timeit, unix_time_millis


class TestSyncClass(unittest.TestCase):
    def setUp(self):
        # create bogus json
        self.filename = 'testConfig.json'

        # get current dir
        self.cur_dir = dirname(abspath(__file__))

        # load config
        self.config = self.loadConfigFile(self.filename)

        # create cassandra structure
        cassandra = self.config['cassandra']
        clus = Cluster(cassandra['url'])
        sess = clus.connect()
        rows = sess.execute('''
                                select *
                                from system.schema_keyspaces
                                where keyspace_name = '{}' '''
                            .format(cassandra['keyspace']))
        if len(rows) > 0:
            sess.execute('''DROP keyspace {} '''.format(cassandra['keyspace']))
        sql = ''' CREATE KEYSPACE %s
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }''' \
              % cassandra['keyspace']
        sess.execute(sql)

        # save for later
        cas = {'cluster': clus, 'session': sess}
        self.cassandra = cas
        self.data_amt = 3

        # connect to elasticsearch and create the data
        self.elastic = {'session': Elasticsearch()}

    def tearDown(self):
        # remove files
        # os.remove(self.filename)

        # delete cassandra structure created for tests
        self.cassandra['session'].execute('''DROP keyspace test ''')
        self.cassandra['cluster'].shutdown()

        # delete elasticsearch indexes
        for sync in self.config['syncs']:
            self.elastic['session'].indices.delete(index=sync['elasticsearch']['index'], ignore=[400, 404])

    def testLoadConfig(self):
        """
        check that the config loading works properly
        :return:
        """
        # check if it is complaining about not having a json
        l = LogCapture('sync-cass-elastic')
        sync = SyncCassElastic()
        l.check(('sync-cass-elastic', 'WARNING', 'No config file passed for the sync class'), )
        l.clear()

        # check if it has setup correctly
        sync = SyncCassElastic(self.config)

        # assert the last time log file is created properly
        os.remove(sync.lastRunFileName) if os.path.exists(sync.lastRunFileName) else None
        self.assertRaises(IOError, sync.setup)
        with open(sync.lastRunFileName, 'w') as f:
            f.write(' ')
        self.assertRaises(ValueError, sync.setup)

        # check connection
        l.clear()
        minutes_ago = self._createLastRunFile()
        sync.setup()
        l.check(('sync-cass-elastic', 'INFO', u"Connected to Cassandra: [u'localhost'] / test"),
                ('sync-cass-elastic', 'INFO', u"Connected to Elasticsearch: [u'localhost']"), )
        self.assertEqual(minutes_ago.strftime('%Y%m%d %H:%M'), sync.time_last_run.strftime('%Y%m%d %H:%M'),
                         'The time should be the same')
        self.assertNotEqual(sync.time_this_run, None, 'Time of this run should be filled')
        self.assertNotEqual(sync.time_delta, None, 'Time of this run should be filled')

        # get rid of the logger checker
        l.uninstall()
        os.remove(sync.lastRunFileName) if os.path.exists(sync.lastRunFileName) else None

    def testFromCassandraToElastic(self):
        """
        Will create data on cassandra and send to elastic search
        """
        config = self.loadConfigFile('testConfig.json')
        sync = SyncCassElastic(config)
        self._createLastRunFile()

        # create the data on cassandra two minutes ago
        two_minutes_ago = datetime.utcnow() - timedelta(minutes=2)
        self._createCassandraData(config['syncs'][0], two_minutes_ago )

        # run the setup
        sync.setup()

        # assert the amount found
        rows = sync.get_cassandra_latest(config['syncs'][0])
        self.assertEqual(len(rows), self.data_amt)

        # check the results
        ok, errors = sync.insert_elasticsearch(config['syncs'][0], rows)
        self.assertEqual(ok, self.data_amt)

    def testFromElasticToCassandra(self):
        '''
        Will check if data is being sent properly from elasticsearch to cassandra
        :return:
        '''
        # initial setup for this test
        config = self.loadConfigFile('testConfig.json')
        sync = SyncCassElastic(config)

        # create last run file for 5 minutes ago and setup sync
        self._createLastRunFile()
        sync.setup()

        # create new documents on elasticsearch and read the data
        amount = 10
        two_min_ago = datetime.utcnow() - timedelta(minutes=2)
        self._createElasticSearchData(config['syncs'][1], amount=amount, curr_time=two_min_ago)
        rows = sync.get_elasticsearch_latest(config['syncs'][1])
        self.assertIsNotNone(rows, 'The query should have returned %s rows' %amount)

        # create some previous data and write on cassandra
        ten_min_ago = datetime.utcnow() - timedelta(minutes=10)
        self._createCassandraData(config['syncs'][1], amount=5, curr_time=ten_min_ago )
        ok, errors = sync.insert_cassandra(config['syncs'][1], rows)
        self.assertEqual(ok, amount)


    ######################################################
    # helpers
    ######################################################
    @timeit
    def _createCassandraData(self, config, curr_time=None, createTable=True, amount=None):
        """
        Will create 'tables' and populate data
        :param config: configuration
        :param currTime:
        :param createTable:
        :param amount:
        :return:
        """
        session = self.cassandra['session']
        if not curr_time:
            curr_time = datetime.utcnow()
        if not amount:
            amount = self.data_amt
        params = config['cassandra']

        # create a table
        if createTable:
            stmt = ''' CREATE TABLE {table} (
                          {id_col} int,
                          {version_col} bigint,
                          text varchar,
                          source varchar,
                          {date_col} timestamp,
                          PRIMARY KEY ({id_col})
                        );'''
            stmt = stmt.format(table=params['table'],
                               version_col=params['version_col'],
                               id_col=params['id_col'],
                               date_col=params['date_col'])
            session.execute(stmt)

        # Prepare the statements
        stmt = "INSERT INTO {table} (" \
               "{id_col}, " \
               "{version_col}, " \
               "text, " \
               "source, " \
               "{date_col}) " \
               "VALUES (?, ?, ?, ?, ?)" \
               "USING TIMESTAMP ? "
        stmt = stmt.format(table=params['table'],
                           version_col=params['version_col'],
                           id_col=params['id_col'],
                           date_col=params['date_col'])
        data_statement = session.prepare(stmt)

        # add the prepared statements to a batch
        count = 0
        batch = BatchStatement()
        for i in range(0, amount):
            batch.add(data_statement,
                      [i, unix_time_millis(curr_time),
                       id_generator(10), 'CASSANDRA', unix_time_millis(curr_time),
                       unix_time_millis(curr_time)])
            count += 1

            # every x records, commit. The parameter 65000 was giving timeout
            if (count % 65000) == 0:
                # execute the batch
                session.execute(batch)
                # hack to get around the 65k limit of python driver
                batch._statements_and_parameters = []
                count = 0

        if count > 0:
            session.execute(batch)
            batch._statements_and_parameters = []

    @timeit
    def _createElasticSearchData(self, config, curr_time=None, amount=None):
        """
        Creates data on an index on Elasticsearch
        :return:
        """
        es = self.elastic['session']
        es.indices.create(index='bogus_data', ignore=400)
        if not curr_time:
            curr_time = datetime.utcnow()
        if not amount:
            amount = self.data_amt
        params = config['elasticsearch']

        # create data
        data = []
        for i in range(0, amount):
            action = {'_type': params['type'],
                      '_id': i,
                      '_version_type': 'external',
                      '_version': unix_time_millis(curr_time),
                      '_source': {
                          'text': id_generator(10),
                          'source': 'Elastic',
                          'date': curr_time,
                          'version': unix_time_millis(curr_time)
                      }
            }
            data.append(action)

        # write to elasticsearch
        bulk(es, data, chunk_size=700, index=params['index'])
        es.indices.flush(index=params['index'])

    def _createLastRunFile(self):
        '''
        Creates a dummy file with 15 minutes ago from now
        '''
        minutesAgo = datetime.utcnow() - timedelta(minutes=5)
        sync = SyncCassElastic(self.config)
        os.remove(sync.lastRunFileName) if os.path.exists(sync.lastRunFileName) else None
        with open(sync.lastRunFileName, 'w') as f:
            f.write(minutesAgo.strftime('%Y%m%d %H:%M'))

        return minutesAgo

    def loadConfigFile(self, filename):
        '''
        will load a config file
        :param filename: json file to be opened
        :return:
        '''
        with open(filename) as f:
            return json.load(f)
