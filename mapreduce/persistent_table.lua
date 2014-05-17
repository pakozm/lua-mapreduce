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

-- The class persistent_table allows to store data in a collection of
-- singletons, allowing to retrieve data if after a system breakdown, or to use
-- several data configuration through several distributed processes.

local persistent_table = {
  _VERSION = "0.1",
  _NAME = "mapreduce.persistent_table",
}

local utils = require "mapreduce.utils"
local cnn   = require "mapreduce.cnn"

-- PUBLIC METHODS

local methods = {}

-- updates current object with MongoDB collection; in case of inconsistency
-- between this object and database, an assert will be thrown
function methods:update()
  local self    = getmetatable(self).obj
  local cnn     = rawget(self,"cnn")
  local dirty   = rawget(self,"dirty")
  local content = rawget(self,"content")
  local db      = cnn:connect()
  local remote_content = db:find_one(self.singleton_dbname,
                                     { _id = content._id })
  if not remote_content then
    -- FIXME: between find_one and insert could occur a race condition :S
    db:insert(self.singleton_dbname, content)
  else
    local merged_content = {}
    content.timestamp = utils.time()
    remote_content.timestamp = content.timestamp
    for key,value in pairs(content) do merged_content[key] = value end
    for key,value in pairs(remote_content) do
      if merged_content[key] then
        assert(merged_content[key] == value,
               "Inconsistent data update retrieved from MongoDB")
      else
        merged_content[key] = value
        rawset(self.content,key,value)
      end
    end
    db:update(self.singleton_dbname,
              { _id = content._id },
              { ["$set"] = merged_content })
  end
  rawset(self,"dirty",false)
end

-- removes the collection at the database, allowing to start from zero
function methods:drop()
  local self = getmetatable(self).obj
  local db = self.cnn:connect()
  db:drop_collection(self.singleton_dbname)
end

local reserved = { _id=true, timestamp=true, set=true, update=true, drop=true }
-- sets a collection of pairs key,value from the given table
function methods:set(tbl)
  local self = getmetatable(self).obj
  for key,value in pairs(tbl) do
    utils.assert_check(value)
    if reserved[key] then
      error(string.format("%s field is reserved",key))
    end
    rawset(self.content,key,value)
  end
  self.dirty=true
end
local local_set = methods.set

------------------------------------------------------------------------------

-- constructor using encapsulation design pattern in Lua
function persistent_table:__call(name, cnn_string, dbname, auth_table)
  assert(type(name) == "string","First argument is a string name for the table")
  local cnn_string = cnn_string or "localhost"
  local dbname = dbname or "tmp"
  -- obj is hidden inside a closure
  local obj = {
    cnn     = cnn(cnn_string, dbname, auth_table),
    name    = name,
    dirty   = true,
    singleton_dbname = dbname .. ".singletons",
    content = { _id = name, timestamp = utils.time() },
  }
  -- visible_table has been prepared to capture obj table as closure of its
  -- metatable, allowing to implement data encapsulation design pattern in Lua
  local visible_table = {}
  setmetatable(visible_table,
               {
                 -- retrieve the obj table from an instance of visible_table
                 obj = obj,
                 -- index a key
                 __index = function(self,key)
                   if methods[key] then
                     return methods[key]
                   else
                     return rawget(obj.content,key)
                   end
                 end,
                 -- sets the value of a key,value pair
                 __newindex = function(self,key,value)
                   local_set(self,{ [key]=value })
                 end
               })
  return visible_table
end
setmetatable(persistent_table, persistent_table)

------------------------------------------------------------------------------

persistent_table.utest = function()
  local conf = persistent_table("conf")
  conf:drop()
  conf:set({ key = "test" })
  conf:update()
  local conf2 = persistent_table("conf")
  conf2:update()
  assert(conf.key == conf2.key)
end

------------------------------------------------------------------------------

persistent_table.utest()

return persistent_table
