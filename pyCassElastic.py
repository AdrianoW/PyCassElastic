import logging
from cassandra.cluster import Cluster, BatchStatement
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import time
from utils import unix_time_millis
from cassandra.query import dict_factory
import sys

log = logging.getLogger('sync-cass-elastic')


class SyncCassElastic():
    def __init__(self, config=None):
        """soc
        Syncronize the data from Cassandra to/from ElasticSearch
        :param config: configuration dictionary. Expects cassandra, elasticsearch, schedule
        :return:
        """
        if not config:
            log.warning('No config file passed for the sync class')
            config = {}

        # general information
        self.default_time = 10
        self.time_last_run = None
        self.time_this_run = None
        self.time_delta = None
        self.lastRunFileName = 'lastruntime.log'

        # cassandra configuration
        self.cassandra = config.get('cassandra', None)
        if not self.cassandra:
            self.cassandra = {'url': None,
                              'keyspace': None,
                              'cluster': None,
                              'session': None}

        # elastic search configuration
        self.elastic = config.get('elasticsearch', None)
        if not self.elastic:
            self.elastic = {'url': None,
                            'session': None}

        # get sync configuration
        self.sync = config.get('syncs')

    def run(self):
        """
        Run the sync process. This should be the only parameter called by the end user.
        :return:
        """
        # setup the sync
        self.setup()

        # TODO: if there is no sync on config file, try to sync all the col families/index with a predefined field

        # execute each of the syncs
        for sync in self.sync:
            log.info('%s - Starting sync' % sync['name'])
            ts = time.time()

            # get the new info from elastic and write to cassandra
            rows = self.get_elasticsearch_latest(sync)
            if rows:
                ok, errors = self.insert_cassandra(sync, rows)
                if ok is not None and errors is not None:
                    log.info('%s - Ok: %i Errors: %i - Elastic ----->> Cassandra'
                             % (sync['name'], ok, errors))

            # get the new info from cassandra and write to elastic
            rows = self.get_cassandra_latest(sync)
            if rows:
                res = self.insert_elasticsearch(sync, rows)
                if res:
                    log.info('%s - Ok: %i Errors: %i - Elastic <<----- Cassandra '
                             % (sync['name'], res[0], len(res[1])))

            te = time.time()
            log.info('%s - End sync' % sync['name'])
            log.info('%s - Elapsed time %2.2f sec' % (sync['name'], te - ts))

        # do the cleanup
        self.shutdown()

    def setup(self):
        """
        Will connect to databases and setup things. Raise exception in case of a problem
        :return: None
        """
        self._init_cassandra()
        self._init_elasticsearch()
        self._get_last_run_time()
        self._get_this_run_time()

    def shutdown(self):
        """
        Close connections and clean ups
        :return: None
        """
        if self.cassandra['cluster']:
            # error when shutting down
            # https://datastax-oss.atlassian.net/browse/PYTHON-30
            # self.cassandra['cluster'].shutdown()
            log.debug('Disconnected from Cassandra')

    def _get_last_run_time(self):
        """
        Get the last time it was sync. If the file does not exists, it will create an exception and ask the user to
        create the file with the desired date
        """
        try:
            f = open(self.lastRunFileName)
        except IOError as e:
            log.error('A file %s should exists with the last run date in the format YYYYMMDD HH:MM'
                      % self.lastRunFileName)
            raise e
        else:
            with f:
                date = f.read()
            self.time_last_run = datetime.strptime(date, '%Y%m%d %H:%M')

    def _get_this_run_time(self):
        """
        Get this run time and will create a delta to be used on the queries.
        """
        # get the current time rounded to the minute before it started
        self.time_this_run = datetime.strptime(datetime.utcnow().strftime('%Y%m%d %H:%M'), '%Y%m%d %H:%M')
        if self.time_last_run:
            self.time_delta = self.time_this_run - self.time_last_run

    def _write_this_run_time(self):
        """
        Will create a file with the run time in format YYYYMMDD HH:MM
        """
        with open(self.lastRunFileName, 'w') as f:
            f.write(self.time_this_run.strftime('%Y%m%d %H:%M'))

    def _init_cassandra(self):
        """
        Connects to cassandra.
        """
        self.cassandra['cluster'] = Cluster(self.cassandra['url'])
        self.cassandra['session'] = self.cassandra['cluster'].connect(self.cassandra['keyspace'])
        self.cassandra['session'].row_factory = dict_factory
        log.debug('Connected to Cassandra: %s / %s' % (self.cassandra['url'], self.cassandra['keyspace']))

    def _init_elasticsearch(self):
        """
        Connects to cassandra. Static so it can be called directly
        """
        self.elastic['session'] = Elasticsearch(self.elastic['url'])
        log.debug('Connected to Elasticsearch: %s' % (self.elastic['url']))

    def get_cassandra_latest(self, sync_params):
        """
         Get a cassandra family column latests updates
        :return:
            rows that are new or updated
        """
        # helpers
        session = self.cassandra['session']
        params = sync_params['cassandra']

        # TODO: get the number of partition key values
        # date_part_key_value = int(self.time_this_run.strftime(params['date_partition_format']))

        # construct the query and run it
        stmt = '''SELECT {fields_list}
                    FROM {table} '''

        # WHERE {date_partition_col} = {date_part_key_value}
        #   AND {date_col} >  {date_begin}
        #   AND {date_col} <= {date_end}

        stmt = stmt.format(fields_list=(params.get('fields_list', '*')),
                           table=params['table'])
        # date_partition_col = params['date_partition_col'],
        # date_part_key_value = date_part_key_value,
        # date_col = params['date_col'],
        # date_begin = unix_time_millis(self.time_last_run),
        # date_end = unix_time_millis(self.time_this_run))

        try:
            rows = session.execute(stmt)
        except:
            log.error('Sync: %s - Step: %s - Table: %s Problem getting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name, params['table']))
            exc_info = sys.exc_info()
            log.error(exc_info[1])
            return None
        return rows

    def insert_cassandra(self, sync_params, rows):
        """
        Insert data into
        :rtype : object
        :param sync_params:
        :param rows:
        :return:
        """
        # helpers
        session = self.cassandra['session']
        params = sync_params['cassandra']

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
        try:
            data_statement = session.prepare(stmt)
        except:
            log.error('Sync: %s - Step: %s - Problem inserting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name ))
            exc_info = sys.exc_info()
            log.error(exc_info[1])
            log.error(exc_info[2])
            return None, None

        # add the prepared statements to a batch
        count = 0
        total = 0
        errors = 0
        batch = BatchStatement()
        for row in rows:
            batch.add(data_statement,
                      [int(row['_id']),
                       row['_source']['version'],
                       row['_source']['text'],
                       row['_source']['source'],
                       datetime.strptime(row['_source']['date'], '%Y-%m-%dT%H:%M:%S.%f'),
                       row['_source']['version']])
            count += 1

            # every x records, commit. There is a limitation on the driver
            if (count % 65000) == 0:
                try:
                    # execute the batch
                    session.execute(batch)
                    total += count
                except:
                    exc_info = sys.exc_info()
                    log.error(exc_info[1])
                    log.error(exc_info[2])
                    errors += count

                count = 0
                # hack to get around the 65k limit of python driver
                batch._statements_and_parameters = []

        if count > 0:
            try:
                # execute the batch
                session.execute(batch)
                total += count
            except:
                log.error('Sync: %s - Step: %s - Problem inserting data'
                          % (sync_params['name'], sys._getframe().f_code.co_name ))
                exc_info = sys.exc_info()
                log.error(exc_info[1])
                errors += count

        return total, errors

    def insert_elasticsearch(self, sync_params, rows):
        """
        Will insert data on elasticsearch. The newer will be kept based on version
        http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-update.html
        https://github.com/elasticsearch/elasticsearch-py/blob/master/example/load.py
        http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/optimistic-concurrency-control.html
        :return:
        """
        # get configuration
        es = self.elastic['session']
        es_params = sync_params['elasticsearch']
        cs_params = sync_params['cassandra']

        # crate indice and ignore error if it exists already
        es.indices.create(index=es_params['index'], ignore=400)

        # create bulk requests
        data = []
        for row in rows:
            # insert information trusting that es will correctly get types
            action = {
                '_type': es_params['type'],
                '_id': row[cs_params['id_col']],
                '_version_type': 'external',
                '_version': row[cs_params['version_col']],
                '_source': {
                    'text': row['text'],
                    'source': row['source'],
                    'date': row['date'],
                    'version': row[cs_params['version_col']]
                }
            }
            data.append(action)

        # write to elasticsearch
        try:
            res = helpers.bulk(es, data, chunk_size=700, index=es_params['index'])
            return res
        except:
            log.error('Sync: %s - Step: %s - Problem inserting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name))
            exc_info = sys.exc_info()
            log.error(exc_info[1])
            return None

    def get_elasticsearch_latest(self, sync_params):
        """
        Get the latest docs
        :param sync_params: parameters of the sync
        :return:
        """
        # get configuration
        es = self.elastic['session']
        es_params = sync_params['elasticsearch']
        cs_params = sync_params['cassandra']

        # construct query params
        query = {
            "query": {
                "constant_score": {
                    "filter": {
                        "range": {
                            cs_params['version_col']: {
                                "gte": unix_time_millis(self.time_last_run),
                                "lte": unix_time_millis(self.time_this_run)
                            }
                        }
                    }
                }
            }
        }

        # execute using scan to get all rows, unordered
        try:
            res = helpers.scan(es, query=query, index=es_params['index'])
            return res
        except Exception, e:
            log.error('Sync: %s - Step: %s - Problem getting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name ))
            exc_info = sys.exc_info()
            log.error(exc_info[1])
            return None