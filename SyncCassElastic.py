#!/usr/bin/env python

from pyCassElastic import PyCassElastic
from utils import setupLog, getError
import argparse
import json
import time
import sys
from os.path import abspath, dirname
from os import getpid

# setup the logger
filename = 'SyncCassElastic.log'
log = setupLog(filename)


def main(config_file):
    """
    Will create the class and call the daemon
    :return: nothing
    """
    # try reading the file
    log.info('Starting information sync between Cassandra and Elastic Search')
    config = None
    try:
        with open(config_file) as f:
            config = json.load(f)
    except:
        log.error('Problem reading config file')
        log.error(getError())
        sys.exit(-1)

    # get the period to be sync or complain
    try:
        sync_period = float(config['period']) * 60
    except:
        log.error('No period parameter found on config. The period must be in seconds')
        sys.exit(-1)

    # init the configuration
    sync = PyCassElastic(config)
    start_time = time.time()

    # run until killed. Catch any exception to log to the file
    while True:
        sync.run()
        time.sleep(sync_period - ((time.time() - start_time) % sync_period))


# main program
if __name__ == '__main__':
    # check the arguments passed
    parser = argparse.ArgumentParser(description='Sync tables between cassandra and elasticsearch')
    parser.add_argument('config',
                        help='The .json configuration file located in the same directory.' \
                             ' Check config_ex.json for example',
                        default='config.json')
    args = parser.parse_args()

    # append the path to the config file
    dir_name = dirname(abspath(__file__)) + '/'
    config_file = dir_name + args.config

    # create the pid file to ease management
    pid = str(getpid())
    pid_file = "SyncCassElastic.pid"
    file(dir_name + pid_file, 'w').write(pid)

    # run logging any error
    try:
        main(config_file)
    except:
        log.error(getError())
        log.info('Exiting. Bye')