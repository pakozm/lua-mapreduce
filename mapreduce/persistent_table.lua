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
  _VERSION = "0.2",
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
  local cnn     = self.cnn
  local dirty   = self.dirty
  local content = self.content
  local db      = cnn:connect()
  local remote_content = db:find_one(self.singleton_ns, { _id = content._id })
  if not remote_content then
    content.timestamp = 0
    -- FIXME: between find_one and insert could occur a race condition :S
    assert( db:insert(self.singleton_ns, content) )
  else
    if dirty then
      -- write results
      assert(content.timestamp == remote_content.timestamp)
      content.timestamp = assert(content.timestamp) + 1
      assert( db:update(self.singleton_ns,
                        { _id = content._id }, content, true, false) )
    else
      for k,v in pairs(content) do content[k] = nil end
      for k,v in pairs(remote_content) do content[k] = v end
    end
  end
  self.dirty = false
end

-- removes the collection at the database, allowing to start from zero
function methods:drop()
  local self = getmetatable(self).obj
  local db = self.cnn:connect()
  db:remove(self.singleton_ns, { _id = self.content._id }, true)
end

local reserved = { _id=true, timestamp=true, set=true, update=true, drop=true,
                   read_only=true, dirty=true }
-- sets a collection of pairs key,value from the given table
function methods:set(tbl)
  local self = getmetatable(self).obj
  assert(not self.read_only,
         "Unable to write in a read_only persistent table")
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

function methods:read_only(v)
  local self = getmetatable(self).obj
  self.read_only = v
end

function methods:dirty()
  local self = getmetatable(self).obj
  return self.dirty
end

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
    dirty   = false,
    read_only = false,
    singleton_ns = dbname .. ".singletons",
    content = { _id = name },
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
                     return obj.content[key]
                   end
                 end,
                 -- sets the value of a key,value pair
                 __newindex = function(self,key,value)
                   if value == nil then
                     obj.content[key] = nil
                     obj.dirty = true
                   else
                     local_set(self,{ [key]=value })
                   end
                 end,
                 -- shows a JSON string
                 __tostring = function(self)
                   -- copy to aux all the content fields except the reserved
                   -- fields
                   local aux = {}
                   for k,v in pairs(obj.content) do
                     if not reserved[k] then aux[k] = v end
                   end
                   return utils.tojson(aux)
                 end,
               })
  visible_table:update()
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

return persistent_table
