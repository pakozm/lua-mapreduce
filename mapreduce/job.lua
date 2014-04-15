local utils = require "mapreduce.utils"

local job = {
  _VERSION = "0.1",
  _NAME = "job",
}

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

function job:__call()
  local obj = { }
  setmetatable(obj,self)
  return obj
end
setmetatable(job,job)

-- PRIVATE METHODS

local function job_process_pending_results(self)
  local db = self.cnn:connect()
  assert( db:insert_batch(self.ns, self.pending_inserts) )
  self.pending_inserts = {}
  collectgarbage("collect")
end

local function job_insert_result(self,key,value,dbname)
  local t = os.time()
  table.insert(self.pending_inserts, { key=key, value=value })
  if #self.pending_inserts > utils.MAX_PENDING_INSERTS then
    job_process_pending_inserts(self,dbname)
  end
end

-- PUBLIC METHODS

function job:make_job(task,key,value)
  assert(key~=nil and value~=nil, "Needs a key and a value")
  local job_tbl ={
    key = tostring(key) or error("Key must be convertible to string"),
    value = value,
    worker = utils.DEFAULT_HOSTNAME,
    tmpname = utils.DEFAULT_TMPNAME,
    time = os.time(),
    status = utils.STATUS.WAITING,
    groupped = false,
  }
  local db = self.cnn:connect()
  db:insert(task:get_jobs_ns(), job_tbl)
  self.one_job = nil
end

function job:take_next(task)
  local db = self.cnn:connect()
  local task_type = task:get_type()
  if task_type == "WAIT" then
    return false
  elseif task_type == "FINISHED" then
    return
  end
  self.jobs_ns    = task:get_jobs_ns()
  self.results_ns = task:get_results_ns()
  local t  = os.time()
  local query = {
    ["$or"] = {
      { status = STATUS.WAITING, },
      { status = STATUS.BROKEN, },
    },
  }
  local set_query = {
    worker = utils.get_hostname(),
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

return job
