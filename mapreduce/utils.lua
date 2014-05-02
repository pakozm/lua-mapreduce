local mongo = require "mongo"

local utils = {
  _VERSION = "0.1",
  _NAME = "mapreduce.utils",
  DEFAULT_RW_TIMEOUT = 300, -- seconds
  DEFAULT_SLEEP = 1, -- seconds
  DEFAULT_MICRO_SLEEP = 0.1, -- seconds
  DEFAULT_HOSTNAME = "<unknown>",
  DEFAULT_TMPNAME = "<NONE>",
  DEFAULT_DATE = 0,
  STATUS = {
    WAITING = 0,  -- default job status
    RUNNING = 1,  -- status when a worker is running the job
    BROKEN = 2,   -- a job which is detected as broken
    FINISHED = 3, -- a finished job
    GROUPED = 4,  -- a finished job which results has been grouped
  },
  TASK_STATUS = {
    WAIT     = "WAIT",
    MAP      = "MAP",
    REDUCE   = "REDUCE",
    FINISHED = "FINISHED",
  },
  MAX_PENDING_INSERTS = 50000,
  MAX_NUMBER_OF_TASKS = 14000,
  GRP_TMP_DIR = "/tmp/grouped",
}

local STATUS = utils.STATUS

-------------------------------------------------------------------------------

local function connect(cnn_string, auth_table)
  local db = assert( mongo.Connection.New{ auto_reconnect=true,
                                           rw_timeout=utils.DEFAULT_RW_TIMEOUT} )
  assert( db:connect(cnn_string) )
  if auth_table then db:auth(auth_table) end
  assert( not db:is_failed(), "Impossible to connect :S" )
  return db
end

local function iscallable(obj)
  local t = type(obj)
  return t == "function" or (t == "table" and (getmetatable(obj) or {}).__call)
end

local function get_hostname()
  local p = io.popen("hostname","r")
  local hostname = p:read("*l")
  p:close()
  return hostname
end

local function check_mapreduce_result(res)
  return res.ok==1,string.format("%s (code: %d)",
                                 res.errmsg or "", res.code or 0)
end

local function sleep(n)
  -- print("SLEEP ",n)
  os.execute("sleep " .. tonumber(n))
end

-- makes a map/reduce job document
local function make_job(key, value)
  assert(key~=nil and value~=nil, "Needs a key and a value")
  return {
    _id = tostring(key) or error("Key must be convertible to string"),
    value = value,
    worker = utils.DEFAULT_HOSTNAME,
    tmpname = utils.DEFAULT_TMPNAME,
    time = os.time(),
    status = utils.STATUS.WAITING,
  }
end

local function escape(str)
  if type(str) == "number" then
    return tostring(str)
  else
    str = assert(tostring(str),"Unable to convert to string map value")
    return ( string.format("%q",str):gsub("\\\n","\\n") )
  end
end

local function serialize_table_ipairs(t)
  local result = {}
  for _,v in ipairs(t) do
    table.insert(result, escape(v))
  end
  return string.format("{%s}",table.concat(result, ","))
end

local function serialize_sorted_by_lines(f,result)
  local keys = {} for k,_ in pairs(result) do table.insert(keys,k) end
  table.sort(keys)
  for _,key in ipairs(keys) do
    local key_str = escape(key)
    local value_str = serialize_table_ipairs(result[key])
    f:write(string.format("return %s,%s\n", key_str, value_str))
  end
end

--------------------------------------------------------------------------------

utils.iscallable = iscallable
utils.get_table_fields = get_table_fields
utils.get_table_fields_ipairs = get_table_fields_ipairs
utils.get_table_fields_recursive = get_table_fields_recursive
utils.get_hostname = get_hostname
utils.check_mapreduce_result = check_mapreduce_result
utils.sleep = sleep
utils.make_job = make_job
utils.connect = connect
utils.escape = escape
utils.serialize_table_ipairs = serialize_table_ipairs
utils.serialize_sorted_by_lines = serialize_sorted_by_lines

return utils
