-- This is a generic server executing script. Its arguments are:
--
--  [1] => connection_string
--
--  [2] => name of the database
--
--  [3] => taskfn Lua module, it could be given as a Lua require string (using
--  dots as sep) or as a path (using / as sep)
--
--  [4] => mapfn Lua module, idem
--
--  [5] => reducefn Lua module, idem
--
--  [6] => finalfn Lua module, idem
--
-- IMPORTANT: the Lua modules (taskfn, mapfn, reducefn, ...) need to be in the
-- LUA_PATH in all the machines where this code need to be executed
--
local connection_string = table.remove(arg, 1)
local dbname   = table.remove(arg, 1)
local taskfn   = table.remove(arg, 1)
local mapfn    = table.remove(arg, 1)
local reducefn = table.remove(arg, 1)
local finalfn  = table.remove(arg, 1)
--
local function normalize(name)
  return name:gsub("/","."):gsub("%.lua$","")
end
--
local server = require "mapreduce.server"
local utils = require "mapreduce.utils"
local s = server.new(connection_string, dbname)
s:configure{
  taskfn      = normalize(taskfn),
  mapfn       = normalize(mapfn),
  reducefn    = normalize(reducefn),
  finalfn     = normalize(finalfn),
  task_args   = arg,
  map_args    = arg,
  reduce_args = arg,
  final_args  = arg,
}
utils.sleep(4)
s:loop()
