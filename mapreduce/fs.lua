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

-- This module contains class abstraction which implement gridfs API, allowing
-- to use different kinds of intermediate storage, the gridfs storage, the
-- shared storage which uses directories in a NFS/samba storage, or the sshfs
-- storage which allows to use local file system for writing but uses scp for
-- reading. The router function allow to decide which kind of storage will be
-- used, and returns an instance of GridFS or sshfs or sharedfs classes.

-- module fs
local fs = {
  _VERSION = "0.1",
  _NAME = "mapreduce.fs",
}

local utils = require "mapreduce.utils"

local make_wildcard_from_mongo_match = function(match_tbl)
  return match_tbl.filename["$regex"]:gsub("%.%*","*"):gsub("[$^]","")
end

------------------------------------------------------------------------------

local cursor = {}

function cursor:next()
  local line = self.f:read("*l")
  if line then return { filename = line } end
end

function cursor:results()
  return function() return self:next() end
end

function cursor:has_more() error("NOT IMPLEMENTED") end

function cursor:itcount() error("NOT IMPLEMENTED") end

function cursor:is_dead() return false end

function cursor:is_tailable() error("NOT IMPLEMENTED") end

function cursor:has_result_flag() return false end

function cursor:get_id() error("NOT IMPLEMENTED") end

function cursor_gc(self)
  self.f:close()
end

function cursor:__call(match_tbl)
  local wildcard_str = make_wildcard_from_mongo_match(match_tbl)
  local obj = {
    f = io.popen(string.format("ls -d %s", wildcard_str), "r")
  }
  setmetatable(obj, { __index=self, __gc=cursor_gc })
  return obj
end
setmetatable(cursor,cursor)
------------------------------------------------------------------------------

local file_builder = {}

local function file_builder_open(self)
  self.f = assert(io.open(self.tmpname,"w"),
                  string.format("Impossible to open %s", self.tmpname))  
  self.f:setvbuf("full")
end

function file_builder:append(data)
  self.f:write(data)
  return true
end
file_builder.write = file_builder.append

function file_builder:build(path)
  local basename = path:match("(.*)/[^/]+")
  assert(basename and #basename > 0,
         string.format("Given an incorrect path '%s'", path))
  os.execute(string.format("mkdir -p %s 2> /dev/null", basename))
  self.f:close()
  assert( utils.rename(self.tmpname, path) )
  file_builder_open(self)
  return true
end

function file_builder_gc(self)
  utils.remove(self.tmpname)
end

function file_builder:__call()
  local obj = { tmpname = os.tmpname() }
  setmetatable(obj, { __index=self, __gc=file_builder_gc })
  file_builder_open(obj)
  return obj
end
setmetatable(file_builder,file_builder)

------------------------------------------------------------------------------

local sharedfs = {}

function sharedfs:list(match_tbl)
  local match_tbl = match_tbl or
    { filename = { ["$regex"] = string.format("%s/.*", self.path) } }
  return cursor(match_tbl)
end

function sharedfs:remove_file(filename)
  return utils.remove(filename)
end

function sharedfs:__call(path,hostnames)
  local obj = { path=path }
  os.execute(string.format("mkdir -p %s 2> /dev/null", path))
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(sharedfs,sharedfs)

------------------------------------------------------------------------------

local sshfs = {}

function sshfs:list(match_tbl)
  local match_tbl = match_tbl or
    { filename = { ["$regex"] = string.format("%s/.*", self.path) } }
  local wildcard_str = make_wildcard_from_mongo_match(match_tbl)
  local processed = {}
  for _,hostname in ipairs(self.hostnames) do
    if not processed[hostname] then
      local ok,exit,signal = os.execute(string.format("scp -CB %s:%s %s/",
                                                      hostname, wildcard_str,
                                                      self.tmpname))
      assert(ok, string.format("Impossible to SCP remote files from %s:%s",
                               hostname, wildcard_str))
      processed[hostname] = true
    end
  end
  match_tbl.filename["$regex"] = match_tbl.filename["$regex"]:gsub(self.path,self.tmpname)
  return cursor(match_tbl)
end

function sshfs:remove_file(filename)
  local filename = filename:gsub(self.path,self.tmpname)
  return utils.remove(filename)
end

function sshfs_gc(self)
  -- remove only if it is empty
  utils.remove(self.tmpname)
end

function sshfs:__call(path,hostnames)
  local obj = { path=path:gsub("/$",""),
                tmpname=os.tmpname(),
                hostnames=hostnames }
  utils.remove(obj.tmpname)
  os.execute(string.format("mkdir -p %s 2> /dev/null", obj.tmpname))
  setmetatable(obj, { __index=self, __gc=sshfs_gc })
  return obj
end
setmetatable(sshfs, sshfs)

------------------------------------------------------------------------------

local router = function(cnn, hostnames, storage, path)
  if storage == "gridfs" then
    return cnn:gridfs(),
    function() return cnn:grid_file_builder() end,
    function(filename)
      return utils.gridfs_lines_iterator(cnn:gridfs(), filename)
    end
  elseif storage == "sshfs" and hostnames and #hostnames>0 then
    local obj = sshfs(path,hostnames)
    return obj,
    function() return file_builder() end,
    function(filename)
      local filename = filename:gsub(path,obj.tmpname)
      return io.lines(filename)
    end
  elseif ( storage == "shared" or
           (storage == "sshfs" and (not hostnames or #hostnames == 0)) ) then
    return sharedfs(path),
    function() return file_builder() end,
    function(filename) return io.lines(filename) end
  else
    error(string.format("Given incorrect storage %s", storage))
  end
end

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
fs.utest = function()
  assert(make_wildcard_from_mongo_match({
                                          filename = {
                                            ["$regex"] = "/tmp/.*"
                                          }
                                        }) == "/tmp/*")
  local cnn  = require "mapreduce.cnn"
  local path = os.tmpname()
  utils.remove(path)
  os.execute("mkdir -p " .. path)
  local c = cnn("localhost", "test")
  local db = c:connect()
  db:drop_collection("test.fs.files")
  db:drop_collection("test.fs.chunks")
  local hostnames = { "localhost" }
  local lines = "first line\nsecond line\n"
  for _,storage in ipairs{ "gridfs", "shared", "sshfs" } do
    local fs,make_builder,make_lines_iterator = router(c, hostnames,
                                                       storage, path)
    local builder = make_builder()
    local inv_files = {}
    for _,file in ipairs{ "test1", "test2" } do
      inv_files[file] = true
      local file = (path .. "/" .. file):gsub("//", "/")
      builder:append(lines)
      builder:build(file)
    end
    local q = fs:list()
    for v in q:results() do
      assert(inv_files[v.filename:match("([^/]+)$")])
      local lines_tbl = {}
      for line in make_lines_iterator(v.filename) do
        table.insert(lines_tbl, line)
      end
      assert(table.concat(lines_tbl,"\n").."\n" == lines)
      assert( fs:remove_file(v.filename) )
    end
  end
end

------------------------------------------------------------------------------

fs.router   = router
fs.sshfs    = sshfs
fs.sharedfs = sharedfs

return fs
