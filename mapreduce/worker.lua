local mongo  = require "mongo"
local util   = require "mapreduce.util"
local worker = {
  _VERSION = "0.1",
  _NAME = "mapreduce.worker",
}

local STATUS = util.STATUS

-- PRIVATE FUNCTIONS

local function process_pending_inserts(self,dbname)
  local db = self:connect()
  assert( db:insert_batch(dbname, self.pending_inserts) )
  self.pending_inserts = {}
  collectgarbage("collect")
end

local function insert(self,key,value,dbname)
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
      return self:emit(key,value)
    end
  end
  return f.func
end

local function take_next_job(self)
  local task_dbname = self.task_dbname
  local task = util.task(self,task_dbname)
  local task_type = task:get_type()
  -- 
  if task_type == "WAIT" then
    return false
  elseif task_type == "FINISHED" then
    return
  end
  --
  local job = util.job()
  if job:take_one() then
    local dbname,result_dbname,fn
    if task_type == "MAP" then
      dbname = task:get_map_tasks()
      result_dbname = task:get_map_results() .. "." .. job:get_key()
      fn = get_func(self, task:get_mapfn(), task:get_map_args())
    elseif job_status.job == "REDUCE" then
      dbname = task:get_red_tasks()
      result_dbname = job_status.red_results
      local g = get_func(self, task:get_redfn(), task:get_red_args())
      fn = function(key,value)
        local value = g(key,value)
        assert(value, "Reduce must return a value")
        self:emit(key,value)
      end
    end
    self:set_result(result_dbname)
    return dbname, job, fn, result_dbname, task:get_type()
  else
    self:set_result()
    return false
  end
end
                             
-- WORKER METHODS
local worker_methods = {}

-- performs the connection, allowing to retrive a lost connection, and returns a
-- dbclient object
function worker_methods:connect()
  if not self.db or self.db:is_failed() then
    self.db = util.connect(self.connection_string, self.auth_table)
  end
  return self.db
end

function worker_methods:emit(key,value)
  assert(self.result_dbname)
  insert(self,key,value,self.result_dbname)
end

function worker:set_result(dbname)
  self.result_dbname = dbname
end

function worker_methods:execute()
  local iter       = 0
  local ITER_SLEEP = util.DEFAULT_SLEEP
  local MAX_ITER   = self.max_iter
  local MAX_SLEEP  = self.max_sleep
  local MAX_TASKS  = self.max_tasks
  local ntasks     = 0
  local task_done
  while iter < MAX_ITER and ntasks < MAX_TASKS do
    repeat
      collectgarbage("collect")
      local dbname,job,fn,result_dbname,task_status = take_next_job(self)
      if dbname then
        task_done = false
        assert(dbname and job and fn and result_dbname)
        print("# EXECUTING TASK ", job:get_id(), dbname, result_dbname)
        local key,value = job:get_pair()
        fn(key,value) -- MAP or REDUCE
        print("# \t FINISHED")
        if #self.pending_inserts > 0 then
          process_pending_inserts(self,result_dbname)
        end
        job:mark_as_finished()
        task_done = true
      else -- if dbname then ... else
        util.sleep(util.DEFAULT_SLEEP)
      end -- if dbname then ... else ... end
    until dbname == nil -- repeat
    if task_done then
      print("# TASK DONE")
      iter       = 0
      ITER_SLEEP = util.DEFAULT_SLEEP
      ntasks      = ntasks + 1
    end
    if ntasks < MAX_TASKS then
      print(string.format("# WAITING...\tntasks: %d/%d\tit: %d/%d\tsleep: %.1f",
                          ntasks, MAX_TASKS, iter, MAX_ITER, ITER_SLEEP))
      util.sleep(ITER_SLEEP)
      ITER_SLEEP = math.min(MAX_SLEEP, ITER_SLEEP*1.5)
    end
    iter = iter + 1
  end
end

function worker_methods:configure(t)
  local inv = { max_iter=true, max_sleep=true, max_tasks=true }
  for k,v in pairs(t) do
    assert(inv[k], string.format("Unknown parameter: %s\n", k))
    self[k] = v
  end
end

-- WORKER METATABLE
local worker_metatable = { __index = worker_methods,
                           __gc = function(self) os.remove(self.tmpname) end }

worker.new = function(connection_string, dbname, auth_table)
  local obj = {
    connection_string = connection_string,
    dbname = assert(dbname, "Needs a dbname as 2nd argument"),
    task_dbname = string.format("%s.task", dbname),
    auth_table = auth_table,
    tmpname = os.tmpname(),
    funcs = {},
    max_iter  = 20,
    max_sleep = 20,
    max_tasks  = 1,
    pending_inserts = {},
  }
  setmetatable(obj, worker_metatable)
  return obj
end

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
worker.utest = function(connection_string, dbname, auth_table)
  local connection_string = connection_string or "localhost"
  local dbname = dbname or "tmp"
end

------------------------------------------------------------------------------

return worker
