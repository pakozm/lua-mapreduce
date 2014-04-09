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
                      }
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
  end
  self.funcs[fname] = f
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
  if db:count(dbname, query) > 0 then
    local hostname = util.get_hostname()
    assert( db:update(dbname, query,
                      {
                        ["$set"] = {
                          worker  = hostname,
                          tmpname = tmpname,
                          started_at = t,
                          status = STATUS.RUNNING,
                        },
                      },
                      false,    -- no create a new document if not exists
                      false) )  -- only update the first match
    local one_job = assert( db:find_one(dbname,
                                        {
                                          status  = STATUS.RUNNING,
                                          worker  = hostname,
                                          tmpname = tmpname,
                                          started_at = t,
                                        }) )
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
  repeat
    local dbname,job,fn,result_dbname,need_group = take_next_job(self)
    if dbname then
      assert(dbname and job and fn and result_dbname)
      print("# EXECUTING JOB ", job.taskid, job._id, dbname, result_dbname)
      repeat
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
    else
      util.sleep(util.DEFAULT_SLEEP)
    end
  until dbname == nil
  print("# JOB DONE")
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
                funcs = {} }
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
