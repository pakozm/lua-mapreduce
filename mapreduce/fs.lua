-- This module contains class abstraction which implement gridfs API, allowing
-- to use different kinds of intermediate storage, the gridfs storage, the
-- shared storage which uses directories in a NFS/samba storage, or the sshfs
-- storage which allows to use local file system for writing but uses scp for
-- reading.

-- router function allow to decide which kind of storage will be used, and
-- returns an instance of GridFS or sshfs or sharedfs classes.

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
  self.f = assert(io.open(self.tmpname,"w"),
                  string.format("Impossible to open %s", self.tmpname))
  return true
end

function file_builder_gc(self)
  utils.remove(self.tmpname)
end

function file_builder:__call()
  local tmpname = os.tmpname()
  local obj = {
    tmpname = tmpname,
    f = assert(io.open(tmpname,"w"),
               string.format("Impossible to open %s", tmpname))
  }
  setmetatable(obj, { __index=self, __gc=file_builder_gc })
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
  utils.remove(filename)
  return true
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
  match_tbl.filename["$regex"]:gsub(self.path,self.tmpname)
  return cursor(match_tbl)
end

function sshfs:remove_file(filename)
  local filename = filename:gsub(self.path,self.tmpname)
  utils.remove(filename)
  return true
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
    function() file_builder() end,
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

------------------------------------------------------------------------------

fs.router   = router
fs.sshfs    = sshfs
fs.sharedfs = sharedfs

return fs
