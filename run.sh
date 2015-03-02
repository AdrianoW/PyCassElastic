#!/bin/sh

# run the process in the background directing the output to null dev
python SyncCassElastic.py config.json 2>SyncCassElastic.log &
echo Tailing the log file. Press ctrl+c to exit. Process will keep running
# wait a litle for file creation
sleep 1
tail -f SyncCassElastic.log
