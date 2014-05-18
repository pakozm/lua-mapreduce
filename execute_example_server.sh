#!/bin/bash
lua execute_server.lua localhost wordcount \
    mapreduce.examples.WordCount.taskfn \
    mapreduce.examples.WordCount.mapfn \
    mapreduce.examples.WordCount.partitionfn \
    mapreduce.examples.WordCount.reducefn \
    mapreduce.examples.WordCount.finalfn \
    mapreduce.examples.WordCount.reducefn $@
