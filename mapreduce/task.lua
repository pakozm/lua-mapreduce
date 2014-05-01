-- sets the task job for the cluster, which indicates the workers which kind of
-- work (MAP,REDUCE,...) must perform
local task = {
  _VERSION = "0.1",
  _NAME = "task",
}

local utils       = require "mapreduce.utils"
local job         = require "mapreduce.job"
local STATUS      = utils.STATUS
local TASK_STATUS = utils.TASK_STATUS

-- PRIVATE FUNCIONS
local function tmpname_summary(tmpname)
  return tmpname:match("([^/]+)$")
end

local function task_set_task_status(self, status, tbl)
  self.tbl = tbl or {}
  self.tbl.status = status
  if self.tbl.status == TASK_STATUS.MAP then
    self.current_jobs_ns = self.map_jobs_ns
    self.current_results_ns = self.map_results_ns
    self.current_fname = self.tbl.mapfn
    self.current_args  = self.tbl.map_args
  elseif self.tbl.status == TASK_STATUS.REDUCE  then
    self.current_jobs_ns = self.red_jobs_ns
    self.current_results_ns = self.red_results_ns
    self.current_fname = self.tbl.reducefn
    self.current_args  = self.tbl.reduce_args
  end
end

-- PUBLIC METHODS

function task:map_results_iterator()
  local db = self.cnn:connect()
  local all_collections = db:get_collections(self.cnn:get_dbname())
  local i=0
  return function()
    while i < #all_collections do
      i=i+1
      local name = all_collections[i]
      if name:match("%.map_results%.") then
        return name
      end
    end
  end
end

function task:create_collection(task_status, params)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = {
                        status      = task_status,
                        --
                        mapfn       = params.mapfn,
                        reducefn    = params.reducefn,
                        map_args    = params.map_args,
                        reduce_args = params.reduce_args,
                    }, },
                    true, false) )
end

function task:update()
  local db = self.cnn:connect()
  local tbl = db:find_one(self.ns, { _id = "unique" })
  if tbl then
    task_set_task_status(self, tbl.status, tbl)
  else
    self.current_results_ns = nil
    self.current_results_ns = nil
    self.current_fname = nil
    self.current_args  = nil
  end
  self.tbl = tbl
end

function task:finished()
  return not self.tbl or self.tbl.status == TASK_STATUS.FINISHED
end

function task:get_task_status()
  if self.tbl then
    return self.tbl.status
  else
    return TASK_STATUS.FINISHED
  end
end

function task:set_task_status(status)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = { status = status } },
                    true, false) )
  task_set_task_status(self, status)
end

function task:get_task_ns()
  return self.ns
end

function task:get_map_jobs_ns()
  return self.map_jobs_ns
end

function task:get_red_jobs_ns()
  return self.red_jobs_ns
end

function task:get_map_results_ns()
  return self.map_results_ns
end

function task:get_red_results_ns()
  return self.red_results_ns
end

function task:get_jobs_ns()
  return self.current_jobs_ns
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

-- JOB INTERFACE

function task:finished_jobs_iterator()
  local db          = self.cnn:connect()
  local map_jobs_ns = self:get_map_jobs_ns()
  local cursor      = db:query(map_jobs_ns, { status = STATUS.FINISHED })
  local task_status = self:get_task_status()
  local jobs_ns     = self:get_jobs_ns()
  local results_ns  = self:get_results_ns()
  return function()
    local job_tbl = cursor:next()
    if job_tbl then
      local job_obj = job(self.cnn, job_tbl, task_status,
                          self:get_fname(), self:get_args(),
                          jobs_ns, results_ns,
                          true) -- not_executable=true
      return job_obj:get_results_ns(),job_obj
    end
  end
end

-- inserts a default job at the data base
function task:insert_default_job(key, value)
  assert(key~=nil and value~=nil, "Needs a key and a value")
  local job_tbl ={
    key = tostring(key) or error("Key must be convertible to string"),
    value = value,
    worker = utils.DEFAULT_HOSTNAME,
    tmpname = utils.DEFAULT_TMPNAME,
    time = os.time(),
    status = utils.STATUS.WAITING,
  }
  local db = self.cnn:connect()
  db:insert(self:get_jobs_ns(), job_tbl)
end

-- workers use this method to load a new job in the caller object
function task:take_next_job(tmpname)
  local db = self.cnn:connect()
  local task_status = self:get_task_status()
  if task_status == TASK_STATUS.WAIT then
    return TASK_STATUS.WAIT -- the worker needs to wait for jobs being ready
  elseif task_status == TASK_STATUS.FINISHED then
    return TASK_STATUS.FINISHED -- all jobs are done
  end
  -- take note of the name spaces for jobs and results
  local jobs_ns    = self:get_jobs_ns()
  local results_ns = self:get_results_ns()
  -- ask mongo to take a free job by setting its data
  local t = os.time()
  local query = {
    ["$or"] = {
      { status = STATUS.WAITING, },
      { status = STATUS.BROKEN, },
    },
  }
  local set_query = {
    worker = utils.get_hostname(),
    tmpname = tmpname_summary(tmpname),
    time = t,
    status = STATUS.RUNNING,
  }
  -- FIXME: check the write concern
  assert( db:update(jobs_ns, query,
                    {
                      ["$set"] = set_query,
                    },
                    false,    -- no create a new document if not exists
                    false) )  -- only update the first match
  -- FIXME: be careful, this call could fail if the secondary server don't has
  -- updated its data
  local job_tbl = db:find_one(jobs_ns, set_query)
  if job_tbl then
    return task_status,job(self.cnn, job_tbl, task_status,
                           self:get_fname(), self:get_args(),
                           jobs_ns, results_ns)
    
  else -- if self.one_job then ...
    -- the job (if taken) will be freed, making it available to other worker
    assert( db:update(jobs_ns, set_query,
                      {
                        ["$set"] = {
                          worker = utils.DEFAULT_HOSTNAME,
                          tmpname = utils.DEFAULT_TMPNAME,
                          status = STATUS.WAITING,
                        },
                      },
                      false,    -- no create a new document if not exists
                      false) )  -- only update the first match    
    return TASK_STATUS.WAIT -- the worker needs to wait
  end -- if self.one_job then ... else ...
end

function task:__call(cnn)
  local dbname = cnn:get_dbname()
  local obj = {
    cnn = cnn,
    ns = dbname .. ".task",
    map_jobs_ns    = dbname .. ".map_jobs",
    map_results_ns = dbname .. ".map_results",
    red_jobs_ns    = dbname .. ".red_jobs",
    red_results_ns = dbname .. ".red_results",                
  }
  setmetatable(obj, { __index=self })
  --
  local db = obj.cnn:connect()
  return obj
end
setmetatable(task,task)

return task
