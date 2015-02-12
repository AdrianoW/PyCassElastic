'''
    Helper functions

'''

import logging
import traceback
import time

def setupLog(filename):
   # create logger with 'spam_application'
    logger = logging.getLogger('sync-cass-elastic')
    logger.setLevel(logging.INFO)

    # create file handler which logs even debug messages
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.INFO)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)-8s - %(filename)-20s:%(lineno)s - %(message)s'))
    ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)-8s - [ %(filename)-20s:%(lineno)s - %(funcName)-10s() ] %(message)s'))

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    # create file handler which logs even debug messages
    return logger

def getError():
    return traceback.format_exc().splitlines()[-2:]

def timeit(method):
    '''
    Decorator to check how long functions take to execute
    https://www.andreas-jung.com/contents/a-python-decorator-for-measuring-the-execution-time-of-methods
    :param method:
    :return:
    '''
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print '%r (%r, %r) %2.2f sec' % \
              (method.__name__, args, kw, te-ts)
        return result

    return timed