local mongo = require "mongo"
local utils = require "mapreduce.utils"

local cnn   = {
  _VERSION = "0.1",
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

function cnn:get_dbname()
  return self.dbname
end

function cnn:get_gridfs_dbname()
  return self.dbname
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