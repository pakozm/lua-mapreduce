lua-mapreduce
=============

Lua map-reduce implementation based in MongoDB. It differs from
[ohitjoshi/lua-mapreduce](https://github.com/rohitjoshi/lua-mapreduce)
in the basis of the communication between the processes. In order to
allow fault tolerancy, and to reduce the communication protocol
complexity, this implementation relies on mongoDB. So, all the data
is stored at auxiliary mongoDB collections.

This software depends in:

- [Lua 5.2](http://www.lua.org/)
- [luamongo](https://github.com/moai/luamongo/), mongoDB driver
  for Lua 5.2.

Installation
------------

Copy the `mapreduce` directory to a place visible from your `LUA_PATH`
environment variable. In the same way, in order to test the example, you need to
put the `examples` directory visible through your `LUA_PATH`. It is possible to
add the active directory by writing in the terminal:

```
$ export LUA_PATH='?.lua;?/init.lua'
```

Documentation
-------------

Available at [wiki pages](https://github.com/pakozm/lua-mapreduce/wiki).

Performance notes
-----------------

Word-count example using [Europarl v7 English data](http://www.statmt.org/europarl/),
with *1,965,734 lines* and *49,158,635 running words*. The data has been splitted
in 197 files with a maximum of *10,000* lines per file. The task is executed
in *one machine* with *four cores*. The machine runs a MongoDB server, a
lua-mapreduce server and four lua-mapreduce workers. **Note** that this task
is not fair because the data could be stored in the local filesystem.

The output of lua-mapreduce was:

```
$ ./execute_BIG_server.sh  > output
# Iteration 1
# 	 Preparing Map
# 	 Map execution, size= 197
	  100.0 % 
# 	 Preparing Reduce
# 	 Reduce execution, num_files= 1970  size= 10
	  100.0 % 
# 	 Final execution
#   Map sum(cpu_time)    99.278813
#   Reduce sum(cpu_time) 57.789231
# Sum(cpu_time)          157.068044
#   Map real time    42
#   Reduce real time 22
# Real time          64
# Total iteration time 66 seconds
```

**Note:** using only one worker takes: 117 seconds

A naive word-count version implemented with pipes and shellscripts takes:

```
$ time cat /home/experimentos/CORPORA/EUROPARL/en-splits/* | \
  tr ' ' '\n'  | sort | uniq -c > output-pipes
real    2m21.272s
user    2m23.339s
sys     0m2.951s
```

A naive word-count version implemented in Lua takes:

```
$ time cat /home/experimentos/CORPORA/EUROPARL/en-splits/* | \
  lua misc/naive.lua > output-naivetime
real    0m26.125s
user    0m17.458s
sys     0m0.324s
```

Looking to these numbers, it is clear that the better is to work in main memory
and in local storage filesystem, as in the naive Lua implementation, which needs
only 17 seconds (user time), but uses local disk files. The map-reduce approach
takes 64 seconds (real time) with four workers and 146 seconds (user time) with
only one worker. These last two numbers are comparable with the naive
shellscript implementation using pipes, which takes 143 seconds (user
time). Concluding, the preliminar lua-mapreduce implementation, using MongoDB
for communication and GridFS for auxiliary storage, is up to **2** times faster
than a shellscript implementation using pipes. Both implementations sort the
data in order to aggregate the results. In the future, a larger data task will
be choosen to compare this implementation with raw map-reduce in MongoDB and/or
Hadoop.

Last notes
----------

This software is in development. More documentation will be added to the
wiki pages, while we have time to do that. Collaboration is open, and all your
contributions will be welcome.
