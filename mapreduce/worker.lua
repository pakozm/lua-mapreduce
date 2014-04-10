local mongo  = require "mongo"
local util   = require "mapreduce.util"
local worker = {
  _VERSION = "0.1",
  _NAME = "mapreduce.worker",
}

local STATUS = util.STATUS

-- PRIVATE FUNCTIONS

local function mark_as_finished(self,dbname,job)
  local db = self:connect()
  assert( db:update(dbname,
                    {
                      _id = job._id,
                    },
                    {
                      ["$set"] = {
                        status = STATUS.FINISHED,
                        finished_at = os.time(),
                      },
                      ["$unset"] = {
                        started_at = "",
                      },
                    },
                    false,
                    false) )
end

local function group(self,key,value,dbname)
  local db = self:connect()
  assert( db:update(dbname,
                    { key = key },
                    {
                      ["$push"] = { values = value },
                    },
                    true,    -- insert if not exists
                    false) ) -- no multi-document
              
end

local function insert(self,key,value,dbname)
  local db = self:connect()
  assert( db:insert(dbname, { key = key, value = value }) )
end

local function get_func(self, fname, args)
  local f = self.funcs[fname]
  if not f then
    f = require(fname)
    if f.init then f.init(args) end
    self.funcs[fname] = f
  end
  return coroutine.wrap(f.func)
end

local function take_next_job(self)
  local db = self:connect()
  local job_dbname = self.job_dbname
  local tmpname = self.tmpname
  local t = os.time()
  local dbname,fn,result_dbname,need_group=false
  local job_status = db:find_one(job_dbname) or "FINISHED"
  if job_status.job == "MAP" then
    dbname = job_status.map_tasks
    fn = get_func(self, job_status.mapfn, job_status.map_args)
    result_dbname = job_status.map_results
    need_group=true
  elseif job_status.job == "REDUCE" then
    dbname = job_status.red_tasks
    fn = get_func(self, job_status.reducefn, job_status.reduce_args)
    result_dbname = job_status.red_results
  elseif job_status.job == "WAIT" then
    return false
  else
    return
  end
  local query = {
    ["$or"] = {
      { status = STATUS.WAITING, },
      { status = STATUS.BROKEN, },
    },
  }
  local hostname = util.get_hostname()
  local set_query = {
    worker = util.get_hostname(),
    tmpname = tmpname,
    started_at = t,
    status = STATUS.RUNNING,
  }
  -- FIXME: check the write concern
  assert( db:update(dbname, query,
                    {
                      ["$set"] = set_query,
                      ["$unset"] = {
                        enqued_at = "",
                      },
                    },
                    false,    -- no create a new document if not exists
                    false) )  -- only update the first match
  
  -- FIXME: be careful, this call could fail if the secondary server don't has
  -- updated its data
  local one_job = db:find_one(dbname, set_query)
  if one_job then
    return dbname,one_job,fn,result_dbname,need_group
  else
    return false
  end
end
                             
-- WORKER METHODS
local worker_methods = {}

-- performs the connection, allowing to retrive a lost connection, and returns a
-- dbclient object
function worker_methods:connect()
  if not self.db or self.db:is_failed() then
    local db = assert( mongo.Connection.New{ auto_reconnect=true,
                                             rw_timeout=util.DEFAULT_RW_TIMEOUT} )
    assert( db:connect(self.connection_string) )
    if self.auth_table then db:auth(auth_table) end
    assert( not db:is_failed(), "Impossible to connect :S" )
    self.db = db
  end
  return self.db
end

function worker_methods:execute()
  local iter       = 0
  local ITER_SLEEP = util.DEFAULT_SLEEP
  local MAX_ITER   = self.max_iter
  local MAX_SLEEP  = self.max_sleep
  local MAX_JOBS   = self.max_jobs
  local njobs      = 0
  local jobdone
  while iter < MAX_ITER and njobs < MAX_JOBS do
    jobdone = false
    repeat
      local dbname,job,fn,result_dbname,need_group = take_next_job(self)
      if dbname then
        assert(dbname and job and fn and result_dbname)
        print("# EXECUTING JOB ", job._id, dbname, result_dbname)
        repeat
          collectgarbage("collect")
          local key,value = fn(job.key,job.value)
          if key ~= nil then
            if need_group then
              group(self,key,value,result_dbname)
            else
              insert(self,key,value,result_dbname)
            end
          end
        until key==nil
        print("# \t FINISHED")
        mark_as_finished(self,dbname,job)
        jobdone = true
      else
        util.sleep(util.DEFAULT_SLEEP)
      end
    until dbname == nil
    if jobdone then
      print("# JOB DONE")
      iter       = 0
      ITER_SLEEP = util.DEFAULT_SLEEP
      njobs      = njobs + 1
    end
    if njobs < MAX_JOBS then
      print(string.format("# WAITING...\tnjobs: %d/%d\tit: %d/%d\tsleep: %.1f",
                          njobs, MAX_JOBS, iter, MAX_ITER, ITER_SLEEP))
      util.sleep(ITER_SLEEP)
      ITER_SLEEP = math.min(MAX_SLEEP, ITER_SLEEP*1.5)
    end
    iter = iter + 1
  end
end

function worker_methods:configure(t)
  local inv = { max_iter=true, max_sleep=true, max_jobs=true }
  for k,v in pairs(t) do
    assert(inv[k], string.format("Unknown parameter: %s\n", k))
    self[k] = v
  end
end

-- WORKER METATABLE
local worker_metatable = { __index = worker_methods,
                           __gc = function(self) os.remove(self.tmpname) end }

worker.new = function(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = assert(dbname, "Needs a dbname as 2nd argument"),
                job_dbname = string.format("%s.job", dbname),
                auth_table = auth_table,
                tmpname = os.tmpname(),
                funcs = {},
                max_iter  = 20,
                max_sleep = 20,
                max_jobs  = 1, }
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
