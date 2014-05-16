#!/bin/bash
lua mapreduce/test.lua &&
screen -d -m ./execute_example_worker.sh
diff <(./execute_example_server.sh | awk '{ print $1,$2 }' | sort) \
    <(cat mapreduce/server.lua \
    mapreduce/worker.lua \
    mapreduce/test.lua \
    mapreduce/utils.lua | lua misc/naive.lua | awk '{ print $1,$2 }' | sort)
if [[ $? -ne 0 ]]; then
    echo "ERROR"
    exit 1
fi
echo "Ok"
