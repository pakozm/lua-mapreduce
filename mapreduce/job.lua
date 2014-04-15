local utils = require "mapreduce.utils"

local job = {
  _VERSION = "0.1",
  _NAME = "job",
}


-- PRIVATE METHODS

-- the jobs are processed in batches, when a batch is ready, this function
-- inserts all the batch results in the database
local function job_process_pending_results(self)
  local db = self.cnn:connect()
  assert( db:insert_batch(self.results_ns, self.pending_inserts) )
  self.pending_inserts = {}
  collectgarbage("collect")
end

local function job_insert_result(self,key,value)
  local t = os.time()
  table.insert(self.pending_inserts, { key=key, value=value })
  if #self.pending_inserts > utils.MAX_PENDING_INSERTS then
    job_process_pending_inserts(self)
  end
end

-- loads the required Lua module, sets the upvalue for the "emit" function,
-- executes init function if needed, and returns the resulting function
local function job_get_func(self, fname, args)
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
      return job_insert_result(self, key, value)
    end
  end
  return f.func
end

local function job_mark_as_finished(self, job)
  assert(job.job_tbl)
  local db = self.cnn:connect()
  assert( db:update(job.jobs_ns,
                    {
                      _id = job:get_id(),
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

-- PUBLIC METHODS

-- constructor, receives a connection and a task instance
function job:__call(cnn, job_tbl, task_status, fname, args, jobs_ns, results_ns)
  local obj = {
    cnn = cnn,
    job_tbl = job_tbl,
    jobs_ns = jobs_ns,
    results_ns = results_ns,
    pending_inserts = {},
    funcs = {}
  }
  setmetatable(obj,self)
  --
  local fn
  local g = job_get_func(obj, fname, args)
  if task_status == "MAP" then
    obj.results_ns = obj.results_ns .. "." .. obj.job_tbl.key
    fn = function()
      g(obj:pair()) -- executes the MAP/REDUCE function
      if #obj.pending_inserts > 0 then
        job_process_pending_inserts(obj)
      end
      job_mark_as_finished(obj)
    end
  elseif task_status == "REDUCE" then
    fn = function()
      local key,value = obj:pair()
      local value = g(key,value) -- executes the MAP/REDUCE function
      assert(value, "Reduce must return a value")
      local db = obj.cnn:connect()
      db:insert(obj.results_ns, { key=key, value=value })
    end
  end
  obj.fn = fn
  return obj
end
setmetatable(job,job)

function job:execute()
  return self.fn()
end

function job:get_key()
  assert(self.job_tbl)
  return self.job_tbl.key
end

function job:get_id()
  assert(self.job_tbl)
  return self.job_tbl._id
end

function job:get_pair()
  assert(self.job_tbl)
  return self.job_tbl.key,self.job_tbl.value
end

function job:status_string()
  return job:get_id()
end

return job
