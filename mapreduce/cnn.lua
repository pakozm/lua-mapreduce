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
  if #self.pending_inserts[ns] > utils.MAX_PENDING_INSERTS then
    local db = self:connect()
    db:insert_batch(ns,self.pending_inserts[ns])
    for i,func in ipairs(self.pending_callbacks[ns]) do func() end
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
        for i,func in ipairs(self.pending_callbacks[ns]) do func() end
      end
    end
    self.pending_inserts   = nil
    self.pending_callbacks = nil
  end
end

function cnn:__call(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = dbname,
                gridfs_dbname = dbname .. "_fs",
                auth_table = auth_table }
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(cnn,cnn)

return cnn
