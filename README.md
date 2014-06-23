Lua-MapReduce
=============

Lua MapReduce implementation based in MongoDB. It differs from
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
environment variable. It is possible to add the active directory by writing in
the terminal:

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
$ ./execute_BIG_server.sh > output
# Iteration 1
# 	 Preparing Map
# 	 Map execution, size= 197
	  100.0 % 
# 	 Preparing Reduce
# 	 Reduce execution, num_files= 1970  size= 10
	  100.0 % 
#   Map sum(cpu_time)     80.297174
#   Reduce sum(cpu_time)  56.829328
# Sum(cpu_time)           137.126502
#   Map sum(real_time)    84.476371
#   Reduce sum(real_time) 63.458693
# Sum(real_time)          147.935064
# Sum(sys_time)           10.808562
#   Map cluster time      26.661836
#   Reduce cluster time   20.710385
# Cluster time            47.372221
# Failed maps     0
# Failed reduces  0
# Server time 49.229152
# 	 Final execution
```

**Note 1:** using only one worker takes: 146 seconds

**Note 2:** using 30 mappers and 15 reducers (30 workers) takes: 32 seconds

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
only 26 seconds (real time), but uses local disk files. The map-reduce approach
takes 49 seconds (real time) with four workers and 146 seconds (real time) with
only one worker. These last two numbers are comparable with the naive
shellscript implementation using pipes, which takes 146 seconds (real
time). Concluding, the preliminar lua-mapreduce implementation, using four workers
and MongoDB
for communication and GridFS for auxiliary storage, is up to **3** times faster
than a shellscript implementation using pipes. Both implementations sort the
data in order to aggregate the results. In the future, a larger data task will
be choosen to compare this implementation with raw map-reduce in MongoDB and/or
Hadoop.

Last notes
----------

This software is in development. More documentation will be added to the
wiki pages, while we have time to do that. Collaboration is open, and all your
contributions will be welcome.
