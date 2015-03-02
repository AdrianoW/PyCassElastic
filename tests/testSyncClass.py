import unittest
import os
from testfixtures import LogCapture
from os.path import abspath, dirname
from utilsTest import id_generator
from datetime import datetime, timedelta
from elasticsearch.helpers import bulk
import json
from cassandra.query import dict_factory
from cassandra.policies import RetryPolicy
import uuid
from time import sleep

# hack to test on a different dir
import sys

sys.path.append(dirname(abspath(__file__)) + '/..')
from pyCassElastic import PyCassElastic, Cluster, Elasticsearch, BatchStatement
from utils import timeit, unix_time_millis, setupLog


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
        clus = Cluster(cassandra['url'], default_retry_policy=RetryPolicy())
        sess = clus.connect()
        sess.row_factory = dict_factory
        sess.default_timeout = None

        # save for later
        cas = {'cluster': clus, 'session': sess, 'keyspace': cassandra['keyspace']}
        self.cassandra = cas
        self.data_amt = 10000
        self.idList = self._generateIDs(self.data_amt)

        # connect to elasticsearch and create the data
        self.elastic = {'session': Elasticsearch()}

        # reset data
        self.resetData()
        sql = ''' CREATE KEYSPACE %s
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }''' \
              % cassandra['keyspace']
        sess.execute(sql)

    def tearDown(self):
        # remove files
        # os.remove(self.filename)

        # delete cassandra structure created for tests
        self.resetData()
        self.cassandra['cluster'].shutdown()

    def resetData(self):
        """
        Deletes all data created
        :return:
        """
        self.cassandra['session'].execute('''DROP keyspace IF EXISTS test''')

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
        _ = PyCassElastic()
        l.check(('sync-cass-elastic', 'WARNING', 'No config file passed for the sync class'), )
        l.clear()

        # check if it has setup correctly
        sync = PyCassElastic(self.config)

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
        l.check(('sync-cass-elastic', 'DEBUG', u"Connected to Cassandra: [u'localhost'] / test"),
                ('sync-cass-elastic', 'DEBUG', u"Connected to Elasticsearch: [u'localhost']"), )
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
        sync = PyCassElastic(config)
        sync_params = config['syncs'][0]
        self._createLastRunFile()

        # create the data on cassandra two minutes ago
        two_minutes_ago = datetime.utcnow() - timedelta(minutes=2)
        ten_minutes_ago = datetime.utcnow() - timedelta(minutes=10)
        self._createCassandraData(sync_params, two_minutes_ago, amount=self.data_amt / 2)
        self._createCassandraData(
            sync_params,
            amount=self.data_amt / 2,
            curr_time=ten_minutes_ago,
            start=self.data_amt / 2,
            create_table=False
        )

        # run the setup
        sync.setup()

        # assert the amount found
        rows = sync.get_cassandra_latest(sync_params)

        # check if it will use date as a filter
        ok, errors = sync.insert_elasticsearch(sync_params, rows)
        if sync_params.get('filter_date', None):
            self.assertEqual(ok, self.data_amt / 2)
        else:
            self.assertEqual(ok, self.data_amt)

        # check if data is the same
        ret = self.checkSync(sync, sync_params)
        self.assertEquals(ret, -1,
                          'The DBs data should be the same. Error on id %s' % ret)

    def testFromElasticToCassandra(self):
        """
        Will check if data is being sent properly from elasticsearch to cassandra
        """
        # initial setup for this test
        config = self.loadConfigFile('testConfig.json')
        sync = PyCassElastic(config)

        # create last run file for 5 minutes ago and setup sync
        self._createLastRunFile()
        sync.setup()

        # create new documents on elasticsearch and read the data
        amount = 10
        two_min_ago = datetime.utcnow() - timedelta(minutes=2)
        self._createElasticSearchData(config['syncs'][1], amount=amount, curr_time=two_min_ago)
        rows = sync.get_elasticsearch_latest(config['syncs'][1])
        self.assertIsNotNone(rows, 'The query should have returned %s rows' % amount)

        # create some previous data and write on cassandra
        ten_min_ago = datetime.utcnow() - timedelta(minutes=10)
        self._createCassandraData(config['syncs'][1], amount=5, curr_time=ten_min_ago)
        ok, errors = sync.insert_cassandra(config['syncs'][1], rows)
        self.assertEqual(ok, amount)

        # check if data are the same
        ret = self.checkSync(sync, config['syncs'][1])
        self.assertEquals(ret, -1,
                          'The DBs data should be the same. Error on id %s' % ret)

    def testBothSides(self):
        """
        Will check if data come and go correctly maintaining the last document
        """
        # initial setup for this test
        config = self.loadConfigFile('testConfig.json')
        sync = PyCassElastic(config)
        sync_params = config['syncs'][0]

        # create last run file for 5 minutes ago
        self._createLastRunFile()

        # create data on C* and ES
        two_min_ago = datetime.utcnow() - timedelta(minutes=2)
        three_min_ago = datetime.utcnow() - timedelta(minutes=3)

        # data will be kept
        self._createElasticSearchData(sync_params, amount=self.data_amt / 2,
                                      curr_time=two_min_ago)
        self._createCassandraData(sync_params, amount=self.data_amt / 2, start=self.data_amt / 2,
                                  curr_time=two_min_ago)

        # data will be overwritten
        self._createElasticSearchData(sync_params, amount=self.data_amt / 2, start=self.data_amt / 2,
                                      curr_time=three_min_ago)
        self._createCassandraData(sync_params, amount=self.data_amt / 2,
                                  curr_time=three_min_ago, create_table=False)

        # run forrest
        sync.run()

        # check if data is the same
        ret = self.checkSync(sync, sync_params)
        self.assertEquals(ret, -1,
                          'The DBs data should be the same. Error on id %s' % ret)

    def testDifferentSchemas(self):
        """
        Test changes in tables and indexes
        :return:
        """
        # initial setup for this test
        es = self.elastic['session']
        session = self.cassandra['session']
        config = self.loadConfigFile('testConfig.json')
        sync = PyCassElastic(config)
        sync.setup()
        sync_params = config['syncs'][0]

        # create last run file for 5 minutes ago
        two_min_ago = datetime.utcnow() - timedelta(minutes=2)
        self._createLastRunFile()

        self._createElasticSearchData(sync_params, amount=self.data_amt / 2,
                                      curr_time=two_min_ago)
        self._createCassandraData(sync_params, amount=(self.data_amt / 2) - 1, start=self.data_amt / 2,
                                  curr_time=two_min_ago)

        # TODO: put different column types to check
        # put another schema doc
        doc = es.get(index='bogus_index', id=str(self.idList[1]))
        doc['_source']['new_col'] = 'lalala'
        doc['_id'] = str(uuid.uuid4())
        doc['_version_type'] = 'external'
        del doc['found']
        bulk(es, [doc], chunk_size=700)

        sync.sync_schemas(sync_params)

        # check if table was created
        self.assertEqual(
            len(session.execute('''select * from system.schema_columns
            where columnfamily_name = 'bogus'
            and column_name = 'new_col'
            allow filtering;''')), 1,
            'Column was not created')

        # check if the data are the same
        self.assertNotEquals(self.checkSync(sync, sync_params), -1,
                             'The DBs should be different')

        # sync and check results
        sync.run()
        ret = self.checkSync(sync, sync_params)
        self.assertEquals(ret, -1,
                          'The DBs data should be the same. Error on id %s' % ret)

        # remove new column for other tests
        session.execute("""ALTER TABLE test.bogus DROP new_col""")

    ######################################################
    # helpers
    ######################################################
    def checkSync(self, sync_class, sync_params):
        """
        Check if both have the same info
        :param sync_params:
        :return: -1 if it is ok or the id of the first problem
        """
        es = self.elastic['session']
        es.indices.refresh(index=sync_params['elasticsearch']['index'])

        # wait for the indices to be refreshed
        sleep(3)

        # get cassandra
        cs_rows = sync_class.get_cassandra_latest(sync_params)

        # convert all to a dict
        cs_dict = {}
        for r in cs_rows:
            cs_dict[r['id']] = r

        # get elastic
        es_rows = es.search(index=sync_params['elasticsearch']['index'])

        # get the hits and compare with cassandra
        hits = es_rows['hits']['hits']
        for hit in hits:
            try:
                # check if the id exists
                row = cs_dict[uuid.UUID(hit['_id'])]
                source = hit['_source']

                # compare each field value
                for col in row:
                    # don't need to check this, done before
                    if col in [sync_params['id_col']]:
                        continue

                    # convert date to make them talk
                    if col in [sync_params['date_col']]:
                        # convert both to the same second as C* truncates the date
                        es_date = datetime.strptime(source[sync_params['date_col']], '%Y-%m-%dT%H:%M:%S.%f')
                        cs_date = row[sync_params['date_col']]
                        es_date = es_date.strftime('%Y-%m-%d %H:%M:%S')
                        cs_date = cs_date.strftime('%Y-%m-%d %H:%M:%S')

                        if es_date == cs_date:
                            continue

                    # compare the rest of the data. The field may be on ES but it must be in C*
                    if source.get(col, None) == row[col]:
                        continue
                    else:
                        log.error('Different Data on id %s -  field %s' % (hit['_id'], col))
                        log.error('Elastic %s ==== Cassandra %s' % (source[col], row[col]))
                        # return the row id with problem
                        return hit['_id']
            except:
                # id does not exists
                return hit['_id']

        return -1

    @timeit
    def _createCassandraData(self, config, curr_time=None, create_table=True, amount=None, start=0):
        """
        Will create 'tables' and populate data
        :param config: configuration
        :param curr_time: time to create data
        :param create_table: if the table should be created
        :param amount: amount to be created
        :param start: first id
        :return:
        """
        session = self.cassandra['session']
        keyspace = self.cassandra['keyspace']
        if not curr_time:
            curr_time = datetime.utcnow()
        if not amount:
            amount = self.data_amt
        params = config['cassandra']

        # create a table
        if create_table:
            stmt = ''' CREATE TABLE {keyspace}.{table} (
                          {id_col} UUID,
                          {version_col} bigint,
                          text varchar,
                          source varchar,
                          {date_col} timestamp,
                          PRIMARY KEY ({primary_key})
                        );'''

            # check if it will use date as a filter
            if config.get('filter_date', None):
                primary_key = '%s, %s' % (config['id_col'], config['version_col'])
            else:
                primary_key = config['id_col']
            stmt = stmt.format(keyspace=keyspace,
                               table=params['table'],
                               version_col=config['version_col'],
                               id_col=config['id_col'],
                               date_col=config['date_col'],
                               primary_key=primary_key)
            session.execute(stmt)
            if config.get('filter_date', None):
                session.execute('CREATE INDEX ON %s.%s (%s)' % (keyspace, params['table'], config['version_col']))

        # Prepare the statements
        stmt = "INSERT INTO {keyspace}.{table} (" \
               "{id_col}, " \
               "{version_col}, " \
               "text, " \
               "source, " \
               "{date_col}) " \
               "VALUES (?, ?, ?, ?, ?)" \
               "USING TIMESTAMP ? "
        stmt = stmt.format(keyspace=keyspace,
                           table=params['table'],
                           version_col=config['version_col'],
                           id_col=config['id_col'],
                           date_col=config['date_col'])
        data_statement = session.prepare(stmt)

        # add the prepared statements to a batch
        count = 0
        batch = BatchStatement()
        for i in range(start, start + amount):
            batch.add(data_statement,
                      [self.idList[i],
                       unix_time_millis(curr_time),
                       id_generator(10), 'CASSANDRA',
                       unix_time_millis(curr_time),
                       unix_time_millis(curr_time)])
            count += 1

            # every x records, commit. The parameter 65000 was giving timeout
            if (count % 5000) == 0:
                # execute the batch
                session.execute(batch)
                # hack to get around the 65k limit of python driver
                batch._statements_and_parameters = []
                count = 0

        if count > 0:
            session.execute(batch)
            batch._statements_and_parameters = []

    @timeit
    def _createElasticSearchData(self, config, curr_time=None, amount=None, start=0):
        """
        Creates data on an index on Elasticsearch
        :param config: configuration
        :param curr_time: time to create data
        :param amount: amount to be created
        :param start: first id
        :return:the response from bulk insertion
        """
        params = config['elasticsearch']
        es = self.elastic['session']
        es.indices.create(index=params['index'], ignore=400)
        if not curr_time:
            curr_time = datetime.utcnow()
        if not amount:
            amount = self.data_amt

        # create data
        data = []
        for i in range(start, start + amount):
            action = dict(_type=params['type'], _id=str(self.idList[i]), _version_type='external',
                          _version=unix_time_millis(curr_time),
                          _source={'text': id_generator(10), 'source': 'Elastic', 'date': curr_time,
                                   'version': unix_time_millis(curr_time)})
            data.append(action)

        # write to elasticsearch
        ret = bulk(es, data, chunk_size=700, index=params['index'])
        es.indices.flush(index=params['index'])

        return ret

    def _createLastRunFile(self):
        """
        Creates a dummy file with 15 minutes ago from now
        """
        minutes_ago = datetime.utcnow() - timedelta(minutes=5)
        sync = PyCassElastic(self.config)
        os.remove(sync.lastRunFileName) if os.path.exists(sync.lastRunFileName) else None
        with open(sync.lastRunFileName, 'w') as f:
            f.write(minutes_ago.strftime('%Y%m%d %H:%M'))

        return minutes_ago

    def loadConfigFile(self, file_name):
        """
        will load a config file
        :param file_name: json file to be opened
        :return:
        """
        with open(file_name) as f:
            return json.load(f)

    def _generateIDs(self, amount):
        """
        Create IDs based on UUID4
        :param amount: number of UUID4 to be generated
        :return:
        """
        self.idList = [uuid.uuid4() for _ in range(0, amount)]
        return self.idList


filename = 'testSyncClass.log'
log = setupLog(filename)