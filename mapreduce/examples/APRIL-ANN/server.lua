local mapreduce = require "mapreduce"

local connection_string = "localhost"
local dbname            = "mr_exp_digits"

local s = mapreduce.server.new(connection_string, dbname)
s:configure{
  taskfn         = "mapreduce.examples.April-ANN",
  mapfn          = "mapreduce.examples.April-ANN",
  partitionfn    = "mapreduce.examples.April-ANN",
  reducefn       = "mapreduce.examples.April-ANN",
  finalfn        = "mapreduce.examples.April-ANN",
  init_args      = arg,
  storage        = "gridfs",
}
mapreduce.utils.sleep(4)
s:loop()
