--[[
  This file is part of Lua-MapReduce
  
  Copyright 2014, Francisco Zamora-Martinez
  
  The APRIL-ANN toolkit is free software; you can redistribute it and/or modify it
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
local mongo = require "mongo"
local utils = require "mapreduce.utils"

local cnn   = {
  _VERSION = "0.2",
  _NAME = "cnn",
}

-- performs the connection, allowing to retrive a lost connection, and returns a
-- dbclient object
function cnn:connect()
  if not self.db or self.db:is_failed() then
    self.db = utils.connect(self.connection_string, self.auth_table)
  end
  return self.db
end

function cnn:gridfs()
  local db = self:connect()
  local gridfs = assert( mongo.GridFS.New(db, self.gridfs_dbname) )
  return gridfs
end

function cnn:grid_file_builder()
  return mongo.GridFileBuilder.New(self:connect(), self.gridfs_dbname)
end

function cnn:get_dbname()
  return self.dbname
end

function cnn:insert_error(who,msg)
  local ns = string.format("%s.errors", self.dbname)
  local db = self:connect()
  db:insert(ns, { worker = who, msg = msg })
end

function cnn:get_errors()
  local ns = string.format("%s.errors", self.dbname)
  local db = self:connect()
  return db:query(ns, {})
end

function cnn:remove_errors(ids)
  local ns = string.format("%s.errors", self.dbname)
  local db = self:connect()
  db:remove(ns,{ _id = { ["$in"] = ids } })
end

function cnn:annotate_insert(ns,tbl,callback)
  self.pending_inserts = self.pending_inserts or {}
  self.pending_callbacks = self.pending_callbacks or {}
  self.pending_inserts[ns] = self.pending_inserts[ns] or {}
  self.pending_callbacks[ns] = self.pending_callbacks[ns] or {}
  table.insert(self.pending_inserts[ns], tbl)
  if callback then table.insert(self.pending_callbacks[ns], callback) end
  if #self.pending_inserts[ns] >= utils.MAX_PENDING_INSERTS then
    local db = self:connect()
    db:insert_batch(ns,self.pending_inserts[ns])
    for i,func in ipairs(self.pending_callbacks[ns]) do
      func(self.pending_inserts[ns][i])
    end
    self.pending_inserts[ns] = {}
    self.pending_callbacks[ns] = {}
  end
end

function cnn:flush_pending_inserts(max)
  local max = max or 0
  if self.pending_inserts then
    local db = self:connect()
    for ns,tbl in pairs(self.pending_inserts) do
      if #tbl > max then
        db:insert_batch(ns,tbl)
        for i,func in ipairs(self.pending_callbacks[ns]) do func(tbl[i]) end
      end
    end
    self.pending_inserts   = nil
    self.pending_callbacks = nil
  end
end

function cnn:__call(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = dbname,
                gridfs_dbname = dbname,
                auth_table = auth_table }
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(cnn,cnn)

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
cnn.utest = function()
  local c = cnn("localhost","test")
  local db = assert( c:connect() )
  assert( not db:is_failed() )
  local gridfs = assert( c:gridfs() )
  local builder = assert( c:grid_file_builder() )
  assert(c:get_dbname() == "test")
  -- insert error
  db:drop_collection("test.errors")
  for _,who in ipairs{ "utest1", "utest2" } do
    for _,msg in ipairs{ "error 1", "error 2" } do
      c:insert_error(who, msg)
    end
  end
  assert(db:count("test.errors") == 4)
  -- get errors
  local ids = {}
  local q = c:get_errors()
  for r in q:results() do
    assert(r.worker == "utest1" or r.worker == "utest2")
    assert(r.msg == "error 1" or r.msg == "error 2")
    table.insert(ids, r._id)
  end
  -- remove errors
  c:remove_errors(ids)
  assert(db:count("test.errors") == 0)
  -- annotate insert
  db:drop_collection("test.inserts")
  local insert_results = {}
  for i=1,utils.MAX_PENDING_INSERTS+10 do
    c:annotate_insert("test.inserts", { _id=i, value=utils.time() },
                      function(v)
                        assert(v._id == i)
                        table.insert(insert_results, i)
                      end)
  end
  assert(#insert_results == utils.MAX_PENDING_INSERTS)
  assert(db:count("test.inserts") == utils.MAX_PENDING_INSERTS)
  c:flush_pending_inserts(0)
  assert(db:count("test.inserts") == utils.MAX_PENDING_INSERTS+10)
  assert(#insert_results == utils.MAX_PENDING_INSERTS+10)
  for i=1,#insert_results do assert(insert_results[i] == i) end
end

return cnn
