#!/bin/bash
LUA_PATH="?.lua;?/init.lua"
lua execute_server.lua django wordcountBIG \
    mapreduce.examples.WordCountBig.taskfn \
    mapreduce.examples.WordCount.mapfn \
    mapreduce.examples.WordCount.partitionfn \
    mapreduce.examples.WordCount.reducefn \
    mapreduce.examples.WordCount.finalfn \
    mapreduce.examples.WordCount.reducefn
