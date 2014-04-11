local mongo = require "mongo"

local util = {
  _VERSION = "0.1",
  _NAME = "mapreduce.util",
  DEFAULT_RW_TIMEOUT = 120, -- seconds
  DEFAULT_SLEEP = 1, -- seconds
  DEFAULT_MICRO_SLEEP = 0.1, -- seconds
  DEFAULT_HOSTNAME = "<unknown>",
  DEFAULT_TMPNAME = "<NONE>",
  DEFAULT_DATE = 0,
  STATUS = { WAITING = 0, RUNNING = 1, BROKEN = 2, FINISHED = 3, },
  MAX_PENDING_INSERTS = 50000,
}

local STATUS = util.STATUS

-------------------------------------------------------------------------------

local function connect(cnn_string, auth_table)
  local db = assert( mongo.Connection.New{ auto_reconnect=true,
                                           rw_timeout=util.DEFAULT_RW_TIMEOUT} )
  assert( db:connect(cnn_string) )
  if auth_table then db:auth(auth_table) end
  assert( not db:is_failed(), "Impossible to connect :S" )
  return db
end

local function iscallable(obj)
  local t = type(obj)
  return t == "function" or (t == "table" and (getmetatable(obj) or {}).__call)
end

local valid_get_table_fields_params_attributes = { type_match = true,
                                                   mandatory  = true,
                                                   getter     = true,
                                                   default    = true }
local function get_table_fields(params, t, ignore_other_fields)
  local type   = type
  local pairs  = pairs
  local ipairs = ipairs
  --
  local params = params or {}
  local t      = t or {}
  local ret    = {}
  for key,value in pairs(t) do
    if not params[key] then
      if ignore_other_fields then
        ret[key] = value
      else
        error("Unknown field: " .. key)
      end
    end
  end
  for key,data in pairs(params) do
    if params[key] then
      local data = data or {}
      for k,_ in pairs(data) do
        if not valid_get_table_fields_params_attributes[k] then
          error("Incorrect parameter to function get_table_fields: " .. k)
        end
      end
      -- each param has type_match, mandatory, default, and getter
      local v = t[key]
      if v == nil then v = data.default end
      if v == nil and data.mandatory then
        error("Mandatory field not found: " .. key)
      end
      if v ~= nil and data.type_match and (type(v) ~= data.type_match) then
        if data.type_match ~= "function" or not iscallable(v) then
          error("Incorrect type '" .. type(v) .. "' for field '" .. key .. "'")
        end
      end
      if data.getter then v=(t[key]~=nil and data.getter(t[key])) or nil end
      ret[key] = v
    end  -- if params[key] then ...
  end -- for key,data in pairs(params) ...
  return ret
end

local function get_table_fields_ipairs(...)
  local arg = table.pack(...)
  return function(t)
    local table = table
    local t   = t or {}
    local ret = {}
    for i,v in ipairs(t) do
      table.insert(ret, get_table_fields(table.unpack(arg), v))
    end
    return ret
  end
end

local function get_table_fields_recursive(...)
  local arg = table.pack(...)
  return function(t)
    local t = t or {}
    return get_table_fields(table.unpack(arg), t)
  end
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
  print("SLEEP ",n)
  os.execute("sleep " .. tonumber(n))
end

-- makes a map/reduce task table
local function make_task(key, value)
  assert(key~=nil and value~=nil, "Needs a key and a value")
  return {
    key = tostring(key) or error("Key must be convertible to string"),
    value = value,
    worker = util.DEFAULT_HOSTNAME,
    tmpname = util.DEFAULT_TMPNAME,
    time = os.time(),
    status = util.STATUS.WAITING,
    groupped = false,
  }
end

--------------------------------------------------------------------------------

-- sets the task job for the cluster, which indicates the workers which kind of
-- work (MAP,REDUCE,...) must perform
local task = {}

function task.create_collection(server,dbname,name)
  local db = server:connect()
  assert( db:update(dbname, { key = "unique" },
                    { ["$set"] = { key         = "unique",
                                   job         = name,
                                   mapfn       = server.mapfn,
                                   reducefn    = server.reducefn,
                                   map_tasks   = server.map_dbname,
                                   map_results = server.map_result_dbname,
                                   red_tasks   = server.red_dbname,
                                   red_results = server.red_result_dbname,
                                   map_args    = server.map_args,
                                   reduce_args = server.reduce_args,
                    }, },
                    true, false) )
end

function task:__call(server,dbname)
  local db  = server:connect()
  local obj = { server=server,
                dbname = dbname,
                status = db:find_one(dbname) }
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(task,task)

function task:get_type()
  return self.status.job
end

function task:set_type(name)
  local db = self.server:connect()
  local dbname = self.dbname
  assert( db:update(dbname, { key = "unique" },
                    { ["$set"] = { job = name } },
                    true, false) )
end

function task:get_mapfn() return self.status.mapfn end
function task:get_map_args() return self.status.map_args end
function task:get_map_tasks() return self.status.map_tasks end
function task:get_map_results() return self.status.map_results end

function task:get_redfn() return self.status.reducefn end
function task:get_red_args() return self.status.reduce_args end
function task:get_red_tasks() return self.status.red_tasks end
function task:get_red_results() return self.status.red_results end

--------------------------------------------------------------------------------

local function tmpname_summary(tmpname)
  return tmpname:match("([^/]+)$")
end

--------------------------------------------------------------------------------

local job = {}

function job:__call()
  local obj = { }
  setmetatable(obj,self)
  return obj
end
setmetatable(job,job)

function job:take_one(worker)
  self.worker = worker
  local db = worker:connect()
  local dbname = self.dbname
  local t = os.time()
  local query = {
    ["$or"] = {
      { status = STATUS.WAITING, },
      { status = STATUS.BROKEN, },
    },
  }
  local set_query = {
    worker = util.get_hostname(),
    tmpname = tmpname_summary(worker.tmpname),
    time = t,
    status = STATUS.RUNNING,
  }
  -- FIXME: check the write concern
  assert( db:update(dbname, query,
                    {
                      ["$set"] = set_query,
                    },
                    false,    -- no create a new document if not exists
                    false) )  -- only update the first match
  -- FIXME: be careful, this call could fail if the secondary server don't has
  -- updated its data
  self.one_job = db:find_one(dbname, set_query)
  if self.one_job then return true end
end

function job:get_key()
  assert(self.one_job)
  return self.one_job.key
end

function job:get_id()
  assert(self.one_job)
  return self.one_job._id
end

function job:get_pair()
  assert(self.one_job)
  return self.one_job.key,self.one_job.value
end

function job:mark_as_finished()
  assert(self.one_job)
  local db = self.worker:connect()
  assert( db:update(dbname,
                    {
                      _id = self.one_job._id,
                    },
                    {
                      ["$set"] = {
                        status = STATUS.FINISHED,
                        time = os.time(),
                      },
                    },
                    false,
                    false) )
end

--------------------------------------------------------------------------------

util.iscallable = iscallable
util.get_table_fields = get_table_fields
util.get_table_fields_ipairs = get_table_fields_ipairs
util.get_table_fields_recursive = get_table_fields_recursive
util.get_hostname = get_hostname
util.check_mapreduce_result = check_mapreduce_result
util.sleep = sleep
util.make_task = make_task
util.task = task
util.connect = connect

return util
