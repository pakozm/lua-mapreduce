local mapreduce = require "mapreduce"

local connection_string = "localhost"
local dbname            = "mr_exp_digits"

local s = mapreduce.server.new(connection_string, dbname)
s:configure{
  taskfn         = "examples.April-ANN",
  mapfn          = "examples.April-ANN",
  partitionfn    = "examples.April-ANN",
  reducefn       = "examples.April-ANN",
  finalfn        = "examples.April-ANN",
  init_args      = arg,
  storage        = "gridfs",
}
mapreduce.utils.sleep(4)
s:loop()
