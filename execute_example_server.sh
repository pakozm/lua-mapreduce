#!/bin/bash
lua execute_server.lua localhost wordcount \
    examples.WordCount.taskfn \
    examples.WordCount.mapfn \
    examples.WordCount.reducefn \
    examples.WordCount.finalfn