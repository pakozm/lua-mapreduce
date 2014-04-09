local server = require "mapreduce.server"
local s = server.new("localhost","lua-mapreduce-wordcount")
s:configure{
  taskfn   = "examples.WordCount.taskfn",
  mapfn    = "examples.WordCount.mapfn",
  reducefn = "examples.WordCount.reducefn",
  finalfn  = "examples.WordCount.finalfn",
  task_args = {a=1,b=2,c=3},
  -- map_args = {},
  -- reduce_args = {},
  -- final_args = {},
}
--
s:drop_collections()
--
s:loop()
--
s:drop_collections()
