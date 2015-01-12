local mapreduce = require "mapreduce"

local connection_string = "localhost"
local dbname            = "mr_exp_digits"

local s = mapreduce.server.new(connection_string, dbname)
s:configure{
  taskfn         = "mapreduce.examples.APRIL-ANN",
  mapfn          = "mapreduce.examples.APRIL-ANN",
  partitionfn    = "mapreduce.examples.APRIL-ANN",
  reducefn       = "mapreduce.examples.APRIL-ANN",
  finalfn        = "mapreduce.examples.APRIL-ANN",
  init_args      = arg,
  storage        = "gridfs",
}
mapreduce.utils.sleep(4)
s:loop()
