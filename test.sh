#!/bin/bash
lua mapreduce/test.lua &&
## COMBINER + ASSOCIATIVE COMMUTATIVE IDEMPOTENT REDUCER
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
## NO COMBINER + ASSOCIATIVE COMMUTATIVE IDEMPOTENT REDUCER
screen -d -m ./execute_example_worker.sh
diff <(lua execute_server.lua localhost wordcount \
    examples.WordCount.taskfn \
    examples.WordCount.mapfn \
    examples.WordCount.partitionfn \
    examples.WordCount.reducefn \
    examples.WordCount.finalfn | awk '{ print $1,$2 }' | sort) \
    <(cat mapreduce/server.lua \
    mapreduce/worker.lua \
    mapreduce/test.lua \
    mapreduce/utils.lua | lua misc/naive.lua | awk '{ print $1,$2 }' | sort)
if [[ $? -ne 0 ]]; then
    echo "ERROR"
    exit 1
fi
## NO COMBINER + GENERAL REDUCER
screen -d -m ./execute_example_worker.sh
diff <(lua execute_server.lua localhost wordcount \
    examples.WordCount.taskfn \
    examples.WordCount.mapfn \
    examples.WordCount.partitionfn \
    examples.WordCount.reducefn2 \
    examples.WordCount.finalfn | awk '{ print $1,$2 }' | sort) \
    <(cat mapreduce/server.lua \
    mapreduce/worker.lua \
    mapreduce/test.lua \
    mapreduce/utils.lua | lua misc/naive.lua | awk '{ print $1,$2 }' | sort)
if [[ $? -ne 0 ]]; then
    echo "ERROR"
    exit 1
fi
echo "Ok"
