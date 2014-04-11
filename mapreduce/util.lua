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

function task:__call(cnn)
  local dbname = self.cnn:dbname()
  local obj = {
    server = server,
    ns = cnn:dbname() .. ".task",
    map_tasks_ns   = dbname .. ".map_tasks",
    map_results_ns = dbname .. ".map_results",
    red_tasks_ns   = dbname .. ".red_tasks",
    red_results_ns = dbname .. ".red_results",                
  }
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(task,task)

function task:create_collection(job_type,params)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { key = "unique" },
                    { ["$set"] = {
                        key         = "unique",
                        job         = job_type,
                        --
                        mapfn       = params.mapfn,
                        reducefn    = params.reducefn,
                        map_args    = params.map_args,
                        reduce_args = params.reduce_args,
                        --
                        map_tasks   = self.map_tasks_ns,
                        map_results = self.map_results_ns,
                        red_tasks   = self.red_tasks_ns,
                        red_results = self.red_results_ns,
                    }, },
                    true, false) )
end

function task:update()
  local db = cnn:connect()
  self.status = db:find_one(self.ns)
  if self.status then
    if self.status.job == "MAP" then
      self.current_tasks_ns   = self.map_tasks_ns
      self.current_results_ns = self.map_results_ns
      self.current_fname = self.mapfn
      self.current_args  = self.map_args
    elseif self.status.job == "REDUCE" then
      self.current_tasks_ns   = self.red_tasks_ns
      self.current_results_ns = self.red_results_ns
      self.current_fname = self.reducefn
      self.current_args  = self.reduce_args
    end
  else
    self.current_results_ns = nil
    self.current_results_ns = nil
    self.current_fname = nil
    self.current_args  = nil
  end
end

function task:finished()
  return not self.status or self.status.job == "FINISHED"
end

function task:get_job_type()
  assert(self.status)
  return self.status.job
end

function task:set_job_type(job_type)
  local db = self.server:connect()
  assert( db:update(self.ns, { key = "unique" },
                    { ["$set"] = { job = job_type } },
                    true, false) )
end

function task:get_tasks_ns()
  return self.current_tasks_ns
end

function task:get_results_ns()
  return self.current_results_ns
end

function task:get_fname()
  return self.current_fname
end

function task:get_args()
  return self.current_args
end

--------------------------------------------------------------------------------

local function tmpname_summary(tmpname)
  return tmpname:match("([^/]+)$")
end

--------------------------------------------------------------------------------

function job:process_pending_inserts()
  local db = self.cnn:connect()
  assert( db:insert_batch(self.ns, self.pending_inserts) )
  self.pending_inserts = {}
  collectgarbage("collect")
end

function job:insert(key,value,dbname)
  local t = os.time()
  table.insert(self.pending_inserts, { key=key, value=value })
  if #self.pending_inserts > util.MAX_PENDING_INSERTS then
    process_pending_inserts(self,dbname)
  end
end

local function get_func(self, fname, args)
  local f = self.funcs[fname]
  if not f then
    f = require(fname)
    if f.init then f.init(args) end
    self.funcs[fname] = f
    local k,v
    repeat
      k,v = debug.getupvalue (f.func, 1)
    until not k or k == "_ENV"
    assert(k == "_ENV")
    -- emit function is inserted in the environment of the function
    v.emit = function(key,value)
      return self:insert(key,value,self.results_ns)
    end
  end
  return f.func
end

--

local job = {}

function job:__call()
  local obj = { }
  setmetatable(obj,self)
  return obj
end
setmetatable(job,job)

function job:take_next(task)
  local db = self.cnn:connect()
  local task_type = task:get_type()
  if task_type == "WAIT" then
    return false
  elseif task_type == "FINISHED" then
    return
  end
  self.tasks_ns   = task:get_tasks_ns()
  self.results_ns = task:get_results_ns()
  local t  = os.time()
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
  if self.one_job then
    local fn
    local g = get_func(self, task:get_fname(), task:get_args())
    if task_type == "MAP" then
      self.results_ns = self.results_ns .. "." .. self.one_job.key
      fn = function()
        g(self:pair())
        if #self.pending_inserts > 0 then
          self:process_pending_inserts()
        end
        self:mark_as_finished()
      end
    elseif task_type == "REDUCE" then
      dbname = task:get_red_tasks()
      result_dbname = job_status.red_results
      fn = function()
        local key,value = self:pair()
        local value = g(key,value)
        assert(value, "Reduce must return a value")
        self:insert(key, value, task:get_results_ns())
        if #self.pending_inserts > 0 then
          self:process_pending_inserts()
        end
      end
    end
    return fn
  end
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

local cnn = {}

function cnn:__call(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = dbname,
                auth_table = auth_table }
  setmetatable(obj,self)
  return obj
end
setmetatable(cnn,cnn)

-- performs the connection, allowing to retrive a lost connection, and returns a
-- dbclient object
function cnn:connect()
  if not self.db or self.db:is_failed() then
    self.db = util.connect(self.connection_string, self.auth_table)
  end
  return self.db
end

function cnn:dbname()
  return self.dbname
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
util.cnn = cnn

return util
