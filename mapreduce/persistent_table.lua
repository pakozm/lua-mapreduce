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

local mongo = require "mongo"
local utils = require "mapreduce.utils"
local cnn   = require "mapreduce.cnn"

local LOCK_SLEEP = 0.1 -- in seconds

-- PUBLIC METHODS

local methods = {}

-- updates current object with MongoDB collection; in case of inconsistency
-- between this object and database, an assert will be thrown
function methods:update()
  local self       = getmetatable(self).obj
  local cnn        = self.cnn
  local dirty      = self.dirty
  local content    = self.content
  local db         = cnn:connect()
  local query      = { _id = content._id, }
  local update     = { }
  if dirty then
    query.timestamp   = content.timestamp
    content.timestamp = nil
    local t = {}
    update["$set"] = t
    for k,v in pairs(content) do t[k] = v end
    t._id = nil
    update["$inc"] = { timestamp = 1 }
  else
    update["$set"] = { __dummy__ = true }
  end
  -- findAndModify returns the document AFTER the modification (new=true)
  local result =
    assert( db:run_command(self.dbname,
			   {
			     cmd = "findAndModify",
			     findAndModify = self.document,
			     query  = query,
			     update = update,
			     upsert = false,
			     new    = true, }) )
  local remote_content = result.value
  assert(mongo.type(remote_content) ~= "mongo.NULL",
	 "Impossible to update, data is not consistent")
  self.dirty = false
end

-- removes the collection at the database, allowing to start from zero
function methods:drop()
  local aux = self
  local self = getmetatable(self).obj
  local db = self.cnn:connect()
  -- findAndModify returns the document AFTER the modification (new=true)
  local result =
    assert( db:run_command(self.dbname,
			   {
			     cmd = "findAndModify",
			     findAndModify = self.document,
			     query = { _id = self.content._id },
			     update = { __dummy__ = true },
			     upsert=true,
			     new=true, }) )
  assert(mongo.type(result.value) ~= "mongo.NULL")
  self.content = result.value
end

local reserved = { _id=true, timestamp=true, set=true, update=true, drop=true,
                   read_only=true, dirty=true, locked=true, __dummy__=true }
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

function methods:lock()
  local self    = getmetatable(self).obj
  local db      = self.cnn:connect()
  local content = self.content
  local remote_content
  repeat
    -- findAndModify returns the document value BEFORE the modification
    -- (new=false) ...
    local result  =
      assert( db:run_command(self.dbname,
			     {
			       cmd = "findAndModify",
			       findAndModify = self.document,
			       query  = { _id = content._id },
			       update = {
				 ["$set"] = { locked = true },
			       },
			       upsert = false,
			       new    = false, }) )
    remote_content = result.value
    assert(mongo.type(remote_content) ~= "mongo.NULL")
    -- and before the modification it must be locked=false, otherwise sleep.
    if remote_content.locked then utils.sleep(LOCK_SLEEP) end
  until not remote_content.locked
  self.locked = true
end

function methods:unlock()
  local self    = getmetatable(self).obj
  local db      = self.cnn:connect()
  local content = self.content
  local remote_content
  -- findAndModify returns the document value BEFORE the modification
  -- (new=false) ...
  local result  =
    assert( db:run_command(self.dbname,
			   {
			     cmd = "findAndModify",
			     findAndModify = self.document,
			     query  = { _id  = content._id },
			     update = {
			       ["$set"] = { locked = false },
			     },
			     upsert = false,
			     new    = false, }) )
  remote_content = result.value
  assert(mongo.type(remote_content) ~= "mongo.NULL")
  self.locked = false
end

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
function persistent_table:__call(name, cnn_string, dbname,
				 document, auth_table)
  assert(type(name) == "string","First argument is a string name for the table")
  local cnn_string = cnn_string or "localhost"
  local dbname = dbname or "tmp"
  local document = document or "singletons"
  -- obj is hidden inside a closure
  local obj = {
    cnn     = cnn(cnn_string, dbname, auth_table),
    name    = name,
    dirty   = false,
    read_only = false,
    dbname = dbname,
    document = document,
    singleton_ns = dbname .. "." .. document,
    content  = { _id = name },
  }
  local db = obj.cnn:connect()
  -- findAndModify returns the document AFTER the modification (new=true)
  local result =
    assert( db:run_command(obj.dbname,
			   {
			     cmd = "findAndModify",
			     findAndModify = obj.document,
			     query = obj.content,
			     update = {
			       ["$set"] = { __dummy__ = true },
			     },
			     upsert=true,
			     new=true, }) )
  assert(mongo.type(result.value) ~= "mongo.NULL")
  obj.content = result.value
  -- visible_table has been prepared to capture obj table as closure of its
  -- metatable, allowing to implement data encapsulation design pattern in Lua
  local visible_table = {}
  setmetatable(visible_table,
               {
                 -- retrieve the obj table from an instance of visible_table
                 obj = obj,
		 -- unlock for just in case
		 __gc = function()
		   if obj.locked == true then
		     self:unlock()
		   end
		 end,
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
