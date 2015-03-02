#!/bin/bash
num=$(ps -ef | grep -i SyncCassElastic | grep -v 'grep' | wc -l)
pids=$(ps -ef | grep -i SyncCassElastic | grep -v 'grep' | awk '{print $2}')
echo Processes running: $num
echo Pids: $pids
