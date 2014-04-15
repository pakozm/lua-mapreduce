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

function cnn:get_dbname()
  return self.dbname
end

function cnn:__call(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = dbname,
                auth_table = auth_table }
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(cnn,cnn)

return cnn
