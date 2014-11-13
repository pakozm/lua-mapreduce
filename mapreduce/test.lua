--[[
  This file is part of Lua-MapReduce
  
  Copyright 2014, Francisco Zamora-Martinez
  
  The Lua-MapReduce toolkit is free software; you can redistribute it and/or modify it
  under the terms of the GNU General Public License version 3 as
  published by the Free Software Foundation
  
  This library is distributed in the hope that it will be useful, but WITHOUT
  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
  for more details.
  
  You should have received a copy of the GNU General Public License
  along with this library; if not, write to the Free Software Foundation,
  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
]]
local utils  = require "mapreduce.utils"
local cnn    = require "mapreduce.cnn"
local fs     = require "mapreduce.fs"
local job    = require "mapreduce.job"
local task   = require "mapreduce.task"
local server = require "mapreduce.server"
local worker = require "mapreduce.worker"
local persistent_table = require "mapreduce.persistent_table"
local heap  = require "mapreduce.heap"
local tuple = require "mapreduce.tuple"

utils.utest()
cnn.utest()
fs.utest()
job.utest()
task.utest()
server.utest()
worker.utest()
persistent_table.utest()
heap.utest()
tuple.utest()

print("Ok")
