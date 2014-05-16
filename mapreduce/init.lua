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
local worker = require "mapreduce.worker"
local server = require "mapreduce.server"
local utils  = require "mapreduce.utils"

local mapreduce = {
  _VERSION = "0.3.0",
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
