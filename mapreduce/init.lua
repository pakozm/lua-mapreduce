local worker = require "mapreduce.worker"
local server = require "mapreduce.server"

local mapreduce = {
  _VERSION = "0.1",
  _NAME    = "mapreduce",
  worker   = worker,
  server   = server,
}

-- integrity test
mapreduce.utest = function(connection_string, dbname)
  server.utest(connection_string, dbname)
end

return mapreduce
