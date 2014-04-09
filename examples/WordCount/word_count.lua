local server = require "mapreduce.server"
local s = server.new("localhost","lua-mapreduce-wordcount")
s:configure{
  taskfn   = "examples.WordCount.taskfn",
  mapfn    = "examples.WordCount.mapfn",
  reducefn = "examples.WordCount.reducefn",
  finalfn  = "examples.WordCount.finalfn",
}
--
s:drop_collections()
--
s:loop()
--
s:drop_collections()
