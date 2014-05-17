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
--  [5] => partitionfn Lua module, idem
--
--  [6] => reducefn Lua module, idem
--
--  [7] => finalfn Lua module, idem (OPTIONAL, by default it is nothing)
--
--  [8] => combinerfn Lua module, idem (OPTIONAL, by default it is nothing. It could be nil)
--
--  [9] => storage (OPTIONAL, by default is nothing. It could be nil)
--
-- IMPORTANT: the Lua modules (taskfn, mapfn, reducefn, ...) need to be in the
-- LUA_PATH in all the machines where this code need to be executed
--
local connection_string = table.remove(arg, 1)
local dbname      = table.remove(arg, 1)
local taskfn      = table.remove(arg, 1)
local mapfn       = table.remove(arg, 1)
local partitionfn = table.remove(arg, 1)
local reducefn    = table.remove(arg, 1)
local finalfn     = table.remove(arg, 1)
local combinerfn  = table.remove(arg, 1)
local storage     = table.remove(arg, 1)
if combinerfn == "nil" then combinerfn = nil end
if storage == "nil" then storage = nil end
--
local function normalize(name)
  return name:gsub("/","."):gsub("%.lua$","")
end
--
local mapreduce = require "mapreduce"
local s = mapreduce.server.new(connection_string, dbname)
s:configure{
  taskfn         = normalize(taskfn),
  mapfn          = normalize(mapfn),
  partitionfn    = normalize(partitionfn),
  reducefn       = normalize(reducefn),
  finalfn        = (finalfn and normalize(finalfn)) or nil,
  combinerfn     = (combinerfn and normalize(combinerfn)) or nil,
  init_args      = arg,
  storage        = storage,
  -- storage = "gridfs[:PATH]", -- 'gridfs', 'shared', 'sshfs', with the
  -- optional string :PATH. if not given PATH will be os.tmpname()
  -- storage = "gridfs:/tmp/wordcount",
  -- storage = "shared:/home/experimentos/tmp/wordcount",
  -- storage = "sshfs:/tmp/wordcount",
  -- storage = "gridfs",
  -- storage = "shared",
  -- storage = "sshfs",
}
mapreduce.utils.sleep(4)
s:loop()
