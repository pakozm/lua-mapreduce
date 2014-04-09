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
- [pabloromeu/luamongo](https://github.com/pabloromeu/luamongo/), a fork of 
  [moai/luamongo](https://github.com/moai/luamongo) for Lua 5.2 and with minor
  improvements.
