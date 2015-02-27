from  pyCassElastic import SyncCassElastic
from  utils import setupLog, getError
import argparse


import json
from os.path import abspath

def main(args):
    '''
    Will create the class and call the daemon
    :return: nothing
    '''
    try:
        with open(args.config) as f:
            config = json.load(f)
    except:
        log.error('Problem reading config file')
        log.error(getError())
        exit(-1)

    sync = SyncCassElastic(config)
    sync.run()

# setup the logger
filename = abspath(__file__).replace('.py', '.log')
log = setupLog(filename)

# check the arguments
if __name__ == '__main__':
    # check the arguments passed
    parser = argparse.ArgumentParser(description='Sync tables between cassandra and elasticsearch')
    parser.add_argument('config', help='Path to the config .json file. Check config_ex.json for example')
    args = parser.parse_args()
    main(args)

