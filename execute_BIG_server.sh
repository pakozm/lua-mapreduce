#!/bin/bash
LUA_PATH="?.lua"
lua execute_server.lua django wordcountBIG \
    examples.WordCountBig.taskfn \
    examples.WordCount.mapfn \
    examples.WordCount.partitionfn \
    examples.WordCount.reducefn \
    examples.WordCount.finalfn
