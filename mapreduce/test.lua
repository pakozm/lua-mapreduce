local utils  = require "mapreduce.utils"
local cnn    = require "mapreduce.cnn"
local fs     = require "mapreduce.fs"
local job    = require "mapreduce.job"
local task   = require "mapreduce.task"
local server = require "mapreduce.server"
local worker = require "mapreduce.worker"

utils.utest()
cnn.utest()
fs.utest()
job.utest()
task.utest()
server.utest()
worker.utest()

print("Ok")
