local mapreduce = require "mapreduce"

local connection_string = "localhost"
local dbname            = "mr_exp_digits"

local w = mapreduce.worker.new(connection_string, dbname)
w:execute()
