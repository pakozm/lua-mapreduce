#!/bin/bash
ns=$1
if [ ! -z $ns ]; then
    echo "use $ns
db.dropDatabase()
use ${ns}_fs
db.dropDatabase()
" | mongo
else
    echo "Needs a namespace as argument!!"
fi
