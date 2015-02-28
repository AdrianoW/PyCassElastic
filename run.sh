#!/bin/sh

# run the process in the background directing the output to null dev
python SyncCassElastic.py config.json > /dev/null 2> /dev/null &