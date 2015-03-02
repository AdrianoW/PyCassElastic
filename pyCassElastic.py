import logging
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import dict_factory
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
import time
from utils import unix_time_millis, getError
import uuid
import re

import sys

log = logging.getLogger('sync-cass-elastic')


class PyCassElastic():
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

        # create elastic to c* mappings
        self.es_to_cs = self._create_map()

    def run(self):
        """
        Run the sync process. This should be the only parameter called by the end user.
        :return:
        """
        # setup the sync
        self.setup()

        # execute each of the syncs
        for sync in self.sync:
            log.info('%s - Starting sync' % sync['name'])
            ts = time.time()

            # sync structures
            if not self.sync_schemas(sync):
                log.error('Unable to sync database schemas')

            # get the new info from cassandra and write to elastic
            rows = self.get_cassandra_latest(sync)
            if rows:
                res = self.insert_elasticsearch(sync, rows)

                # is using data as a parameter, delete on C* data that exists on Elastic
                if sync.get('filter_date', None):
                    ok, errors = self._delete_cassandra_records(sync, res)

                    # if there were errors deleting the data, do not continue sync or data will be duplicated
                    if errors is None or errors > 0:
                        log.error('Unable to delete old records from Cassandra')
                        continue
                else:
                    ok = res[0]
                    errors = len(res[1])

                log.info('%s - Ok: %i Errors: %i - Elastic <<----- Cassandra '
                         % (sync['name'], ok, errors))

            # get the new info from elastic and write to cassandra
            rows = self.get_elasticsearch_latest(sync)
            if rows:
                ok, errors = self.insert_cassandra(sync, rows)
                if ok is not None and errors is not None:
                    log.info('%s - Ok: %i Errors: %i - Elastic ----->> Cassandra'
                             % (sync['name'], ok, errors))

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

        self._write_this_run_time()

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

        # construct the query and run it
        stmt = '''SELECT {fields_list}
                    FROM {table} '''
        stmt = stmt.format(fields_list=(params.get('fields_list', '*')),
                           table=params['table'])

        # check if could filter
        if sync_params.get('filter_date', None):
            stmt += '''WHERE {version_col} > {time_last_run}
                        AND {version_col} <= {time_this_run}
                        ALLOW FILTERING'''

            stmt = stmt.format(version_col=sync_params['version_col'],
                               time_last_run=unix_time_millis(self.time_last_run),
                               time_this_run=unix_time_millis(self.time_this_run))

        try:
            rows = session.execute(stmt)
        except:
            log.error('Sync: %s - Step: %s - Table: %s Problem getting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name, params['table']))
            log.error(getError())
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
        keyspace = self.cassandra['keyspace']

        # get the table schema and order so that we can insert on query in correct order
        schema = self._get_table_schema(keyspace, params['table'])
        if not schema:
            return None, None
        cols = schema.keys()
        cols.sort()

        # Prepare the statements
        stmt = "INSERT INTO {keyspace}.{table} ("
        stmt += ", ".join(['%s' % k for k in cols])
        stmt += ") VALUES ("
        stmt += ", ".join([':' + k for k in cols])
        stmt += ") USING TIMESTAMP :p_timestamp "
        stmt = stmt.format(
            keyspace=keyspace,
            table=params['table'])

        try:
            data_statement = session.prepare(stmt)
        except:
            log.error('Sync: %s - Step: %s - Problem inserting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name ))
            log.error(getError())
            return None, None

        # add the prepared statements to a batch
        count = 0
        total = 0
        errors = 0
        batch = BatchStatement()
        cols.remove(sync_params['id_col'])
        for row in rows:
            # convert to the cassandra structure
            try:
                # fill the data dictionary and put none on columns that are not present
                data = {}
                source = row['_source']
                for col in cols:
                    data[col] = source.get(col, None)
                date = datetime.strptime(source[sync_params['date_col']], '%Y-%m-%dT%H:%M:%S.%f')
                data[sync_params['id_col']] = uuid.UUID(row['_id'])
                data[sync_params['date_col']] = unix_time_millis(date)
                data['p_timestamp'] = data['version']
                batch.add(data_statement,
                          data)
                count += 1
            except:
                log.error('Problem converting data {}'.format(row['_id']))
                log.error(getError())
                continue

            # every x records, commit. There is a limitation on the driver
            if (count % 5000) == 0:
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
                log.error(getError())
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

        # crate indice and ignore error if it exists already
        es.indices.create(index=es_params['index'], ignore=400)

        # create bulk requests
        data = []
        for row in rows:
            # insert information trusting that es will correctly get types
            uid = row[sync_params['id_col']]
            del row[sync_params['id_col']]
            action = {
                '_type': es_params['type'],
                '_id': str(uid),
                '_version_type': 'external',
                '_version': row[sync_params['version_col']],
                '_source': row
            }
            data.append(action)

        # write to elasticsearch
        try:
            res = helpers.bulk(es, data, chunk_size=700, index=es_params['index'])
            return res
        except:
            log.error('Sync: %s - Step: %s - Problem inserting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name))
            log.error(getError())
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

        # construct query params
        query = {
            "query": {
                "constant_score": {
                    "filter": {
                        "range": {
                            sync_params['version_col']: {
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
            es.indices.refresh(index=es_params['index'])
            res = helpers.scan(es, query=query, index=es_params['index'])
            return res
        except:
            log.error('Sync: %s - Step: %s - Problem getting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name ))
            log.error(getError())
            return None

    def sync_schemas(self, sync_params):
        """
        Check if the tables are the same and sync them if needed
        :param sync_params: the process it is being run
        :return: True if sync was successful
        """
        # helpers
        session = self.cassandra['session']
        es_params = sync_params['elasticsearch']
        cs_params = sync_params['cassandra']
        keyspace = self.cassandra['keyspace']
        table = cs_params['table']

        # create structures and compare them
        t_schema = self._get_table_schema(keyspace, table)
        d_schema = self._get_doc_schema(es_params['index'], es_params['type'])

        if not t_schema or not d_schema:
            return False

        # remove the id columns and check if structures are the same
        del t_schema[sync_params['id_col']]
        if len(d_schema.keys()) > len(t_schema.keys()):
            for col in d_schema.keys():
                if col not in t_schema:
                    # add column
                    stmt = '''ALTER TABLE {keyspace}.{table} ADD {column} {type}'''.format(
                        keyspace=keyspace,
                        table=table,
                        column=col,
                        type=self.es_to_cs[d_schema[col]]
                    )
                    session.execute(stmt)

        return True

    def _get_table_schema(self, keyspace, table):
        """
        Create a dict with the table schema
        :param keyspace: cassandra keyspace
        :param table: table to get schema
        :return:
        """
        # get the information
        cluster = self.cassandra['cluster']
        try:
            schema = cluster.metadata.keyspaces[keyspace].tables[table]
        except:
            log.error('Keyspace: %s - Table: %s - Schema does not exists' % (keyspace, table))
            log.error(getError())
            return None

        # create the structure
        ret = {}
        for c in schema.columns:
            ret[c] = schema.columns[c].typestring

        return ret

    def _get_doc_schema(self, index, doc_type):
        """
        Create a dict with the doc schema
        :param index: index where the doc is
        :param doc_type: the doc to get the schema
        :return:
        """
        # make sure the index is refreshed.
        es = self.elastic['session']
        es.indices.refresh(index=index, ignore=[400, 404])

        # create the structure
        ret = {}
        try:
            schema = es.indices.get_mapping(index=index, doc_type=doc_type)
            cols = schema[index]['mappings'][doc_type]['properties']
        except:
            log.error('Index: %s - Doc: %s - Schema does not exists' % (index, doc_type))
            log.error(getError())
            return None

        for c in cols:
            ret[c] = cols[c]['type']

        return ret

    @staticmethod
    def _create_map():
        """
        Create the mapping from elasticsearch types to cassandra types
        :return:
        """
        # TODO: make bigger mapping, for all types
        return {
            'string': 'varchar'
        }

    def _delete_cassandra_records(self, sync_params, res):
        """
         Delete the records from Cassandra. The correct version will be brought from elastic search
        :param sync_params: parameters of this sync
        :param res: response from the elastic search insert operation
        :return:
        """
        # helpers
        session = self.cassandra['session']
        cs_params = sync_params['cassandra']
        keyspace = self.cassandra['keyspace']
        re_comp = re.compile('provided \[(.*?)\]')

        # prepare statement
        stmt = '''delete from  {keyspace}.{table}
                  where {id_col} = :{id_col} and {version_col}=:{version_col}'''.format(
            keyspace=keyspace,
            table=cs_params['table'],
            version_col=sync_params['version_col'],
            id_col=sync_params['id_col']
        )
        try:
            data_statement = session.prepare(stmt)
        except:
            log.error('Sync: %s - Step: %s - Problem deleting data'
                      % (sync_params['name'], sys._getframe().f_code.co_name ))
            log.error(getError())
            return None, None

        # prepare dictionary with records that should be deleted
        batch = BatchStatement()
        count = 0
        total = 0
        errors = 0
        for row in res[1]:
            # for each "error" entry, check if it is the type of conflict
            row = row['index']
            if row['status'] == 409:
                # TODO: create a log table for deletions
                # reg ex to find the current version
                version_col = long(re.findall(re_comp, row['error'])[0])
                data = {sync_params['id_col']: uuid.UUID(row['_id']),
                        sync_params['version_col']: version_col}
                #sync_params['date_col']:version_col}
                batch.add(data_statement, data)
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
                log.error(getError())
                errors += count

        return total, errors
