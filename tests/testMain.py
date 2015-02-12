import unittest
import os
from subprocess import Popen, PIPE
from os.path import abspath, dirname


class TestMain(unittest.TestCase):

    def setUp(self):
        # create bogus json
        self.filename = 'bogus.json'
        with open(self.filename, 'w') as f:
            f.write('')

        # get current dir
        self.cur_dir = dirname(abspath(__file__))

    def tearDown(self):
        os.remove(self.filename)

    def xtestParams(self):
        # assert parameter is missing
        out, err = Popen(['python', 'main.py'], stdout=PIPE, stderr=PIPE).communicate()
        print err
        assert 'too few arguments' in err

        # assert there is no config
        out, err = Popen(['python', 'main.py', 'any.json'], stdout=PIPE, stderr=PIPE).communicate()
        assert 'No such file' in err

        out, err = Popen(['python', 'main.py', self.filename], stdout=PIPE, stderr=PIPE).communicate()
        assert 'No JSON' in err





