local connection_string = table.remove(arg, 1)
local dbname   = table.remove(arg, 1)
local taskfn   = table.remove(arg, 1)
local mapfn    = table.remove(arg, 1)
local reducefn = table.remove(arg, 1)
local finalfn  = table.remove(arg, 1)
--
local server = require "mapreduce.server"
local utils = require "mapreduce.utils"
local s = server.new(connection_string, dbname)
s:configure{
  taskfn      = taskfn,
  mapfn       = mapfn,
  reducefn    = reducefn,
  finalfn     = finalfn,
  task_args   = arg,
  map_args    = arg,
  reduce_args = arg,
  final_args  = arg,
}
utils.sleep(10)
s:loop()
