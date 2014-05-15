local worker = require "mapreduce.worker"
local server = require "mapreduce.server"
local utils  = require "mapreduce.utils"

local mapreduce = {
  _VERSION = "0.2.2",
  _NAME    = "mapreduce",
  worker   = worker,
  server   = server,
  utils    = utils,
}

-- integrity test
mapreduce.utest = function(connection_string, dbname)
  server.utest(connection_string, dbname)
end

return mapreduce
