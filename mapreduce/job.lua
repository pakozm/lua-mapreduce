local utils = require "mapreduce.utils"

local job = {
  _VERSION = "0.1",
  _NAME = "job",
}

local STATUS = utils.STATUS

-- PRIVATE METHODS

-- the jobs are processed in batches, when a batch is ready, this function
-- inserts all the batch results in the database
local function job_process_pending_inserts(self)
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
local funcs = { }
local function job_get_func(self, fname, args)
  local f = funcs[fname]
  if not f then
    f = { m = require(fname) }
    if f.m.init then f.m.init(args) end
    funcs[fname] = f
    local k,v
    repeat
      k,v = debug.getupvalue (f.m.func, 1)
    until not k or k == "_ENV"
    assert(k == "_ENV")
    -- emit function is inserted in the environment of the function
    f.upvalue = v
  end
  f.upvalue.emit = function(key, value)
    return job_insert_result(self, key, value)
  end
  return f.m.func
end

local function job_mark_as_finished(self)
  assert(self.job_tbl)
  local db = self.cnn:connect()
  assert( db:update(self.jobs_ns,
                    {
                      _id = self:get_id(),
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

function job:execute()
  return self.fn()
end

function job:get_id()
  assert(self.job_tbl)
  return self.job_tbl._id
end

function job:get_pair()
  assert(self.job_tbl)
  return self.job_tbl._id,self.job_tbl.value
end

function job:status_string()
  return self:get_id()
end

-- constructor, receives a connection and a task instance
function job:__call(cnn, job_tbl, task_status, fname, args, jobs_ns, results_ns)
  local obj = {
    cnn = cnn,
    job_tbl = job_tbl,
    jobs_ns = jobs_ns,
    results_ns = results_ns,
    pending_inserts = {},
  }
  setmetatable(obj, { __index=self })
  --
  local fn
  local g = job_get_func(obj, fname, args)
  local key,value = obj:get_pair()
  if task_status == "MAP" then
    obj.results_ns = obj.results_ns .. ".K" .. key
    fn = function()
      g(key,value) -- executes the MAP/REDUCE function
      if #obj.pending_inserts > 0 then
        job_process_pending_inserts(obj)
      end
      job_mark_as_finished(obj)
    end
  elseif task_status == "REDUCE" then
    fn = function()
      local value = g(key,value) -- executes the MAP/REDUCE function
      assert(value, "Reduce must return a value")
      local db = obj.cnn:connect()
      print(obj.results_ns, key, value)
      db:insert(obj.results_ns, { _id=key, value=value })
      job_mark_as_finished(obj)
    end
  end
  obj.fn = fn
  return obj
end
setmetatable(job,job)

return job
