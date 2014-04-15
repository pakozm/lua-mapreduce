local worker = {
  _VERSION = "0.1",
  _NAME = "mapreduce.worker",
}

local utils  = require "mapreduce.utils"
local task   = require "mapreduce.task"
local cnn    = require "mapreduce.cnn"

-- PRIVATE FUNCTIONS

-- WORKER METHODS
local worker_methods = {}

function worker_methods:execute()
  local task       = self.task
  local iter       = 0
  local ITER_SLEEP = utils.DEFAULT_SLEEP
  local MAX_ITER   = self.max_iter
  local MAX_SLEEP  = self.max_sleep
  local MAX_TASKS  = self.max_tasks
  local ntasks     = 0
  local job_done
  while iter < MAX_ITER and ntasks < MAX_TASKS do
    repeat
      collectgarbage("collect")
      task:update()
      local task_status,job = task:take_next_job(self.tmpname)
      if job then
        if not job_done then
          print("# NEW TASK READY")
        end
        print(string.format("# \t EXECUTING %s JOB _id: %q",
                            task_status, job:status_string()))
        job:execute() -- MAP or REDUCE
        print("# \t\t FINISHED")
        job_done = true
      else -- if dbname then ... else
        utils.sleep(utils.DEFAULT_SLEEP)
      end -- if dbname then ... else ... end
    until task:finished() -- repeat
    if job_done then
      print("# TASK DONE")
      iter       = 0
      ITER_SLEEP = utils.DEFAULT_SLEEP
      ntasks     = ntasks + 1
      job_done   = false
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
