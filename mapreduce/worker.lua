local worker = {
  _VERSION = "0.1",
  _NAME = "mapreduce.worker",
}

local job    = require "mapreduce.job"
local utils  = require "mapreduce.utils"
local task   = require "mapreduce.task"
local cnn    = require "mapreduce.cnn"

-- PRIVATE FUNCTIONS

local worker_execute = function(self)
  print(" # HOSTNAME ", utils.get_hostname())
  local task       = self.task
  local iter       = 0
  local ITER_SLEEP = utils.DEFAULT_SLEEP
  local MAX_ITER   = self.max_iter
  local MAX_SLEEP  = self.max_sleep
  local MAX_TASKS  = self.max_tasks
  local ntasks     = 0
  local job_done
  while iter < MAX_ITER and ntasks < MAX_TASKS do
    collectgarbage("collect")
    local counter = 0
    repeat
      counter = counter + 1
      if counter % utils.MAX_IT_WO_CGARBAGE then collectgarbage("collect") end
      task:update()
      local task_status,job = task:take_next_job(self.tmpname)
      self.current_job = job
      if job then
        if not job_done then
          print("# New TASK ready")
        end
        print(string.format("# \t Executing %s job _id: %q",
                            task_status, job:status_string()))
        local t1 = utils.time()
        local elapsed_time = job:execute() -- MAP or REDUCE
        self.current_job = nil
        print(string.format("# \t\t Finished: %f elapsed user time, %f real time",
                            elapsed_time, utils.time() - t1))
        job_done = true
      else -- if dbname then ... else
        print("# \t Running, waiting for new jobs...")
        collectgarbage("collect")
        self.cnn:flush_pending_inserts(0)
        utils.sleep(utils.DEFAULT_SLEEP)
      end -- if dbname then ... else ... end
    until task:finished() -- repeat
    self.cnn:flush_pending_inserts()
    if job_done then
      print("# TASK done")
      iter       = 0
      ITER_SLEEP = utils.DEFAULT_SLEEP
      ntasks     = ntasks + 1
      job_done   = false
      job.reset_cache()
    end
    if ntasks < MAX_TASKS then
      print(string.format("# WAITING...\tntasks: %d/%d\tit: %d/%d\tsleep: %.1f",
                          ntasks, MAX_TASKS, iter, MAX_ITER, ITER_SLEEP))
      utils.sleep(ITER_SLEEP)
      ITER_SLEEP = math.min(MAX_SLEEP, ITER_SLEEP*1.5)
    end
    iter = iter + 1
  end
end

-- WORKER METHODS
local worker_methods = {}

function worker_methods:execute()
  local ok,msg = xpcall(worker_execute, debug.traceback, self)
  if not ok then
    if self.current_job then
      self.current_job:mark_as_broken()
    end
    error(msg)
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
  local cnn_obj = cnn(connection_string, dbname, auth_table)
  local obj = {
    cnn     = cnn_obj,
    task    = task(cnn_obj),
    tmpname = os.tmpname(),
    --
    max_iter   = 20,
    max_sleep  = 20,
    max_tasks  = 1,
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
