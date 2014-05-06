-- This is a generic worker executing script. Its arguments are:
--
--  [1] => connection_string
--
--  [2] => name of the database
--
local connection_string = arg[1]
local dbname = arg[2]
local mapreduce = require "mapreduce"
local w = mapreduce.worker.new(connection_string, dbname)
w:execute()
