local connection_string = arg[1]
local dbname = arg[2]
local worker = require "mapreduce.worker"
local w = worker.new(connection_string, dbname)
w:execute()
