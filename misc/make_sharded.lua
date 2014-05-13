local connection_string = arg[1]
local dbname = arg[2]
local auth_user = arg[3]
local auth_dbname = arg[4] or "admin"
assert(connection_string and dbname,
       "At least connection string and dbname are needed as arguments")
--
function read_password(msg)
  io.write(msg)
  -- enter raw mode
  os.execute("stty -echo raw")
  local pass = {}
  repeat
    ch = io.read(1)
    if not ch then
      break
    elseif ch == '\r' or ch == '\n' then
      io.write('\r\n')
    elseif ch:byte(1) == 127 then
      io.write('\r')
      io.write(msg)
      io.write((' '):rep(#pass))
      table.remove(pass,#pass)
      io.write('\r')
      io.write(msg)
      io.write(('*'):rep(#pass))
    else
      table.insert(pass,ch)
      io.write('*')
    end
    io.flush()
  until ch == '\n' or ch == '\r' or not ch
  -- activate echo and exit raw mode
  os.execute("stty echo cooked")
  --
  return table.concat(pass)
end
--
local mongo = require "mongo"
local db = assert( mongo.Connection.New() )
assert( db:connect(connection_string) )
if auth_user then
  local auth_password = read_password("password: ")
  assert( db:auth{ username=auth_user,
                   password=auth_password,
                   dbname=auth_dbname },
          "Impossible to authenticate with the given credentials" )
  auth_password = nil
end
collectgarbage("collect")
local gridfs = assert( mongo.GridFS.New(db, dbname) )
db:run_command("admin", { enableSharding = dbname })
db:run_command("admin", { shardCollection = dbname .. ".fs.chunks",
                          key = { files_d = 1 } })
