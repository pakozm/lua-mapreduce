local utils  = require "mapreduce.utils"
local task   = require "mapreduce.task"
local worker = {
  _VERSION = "0.1",
  _NAME = "mapreduce.worker",
}

-- PRIVATE FUNCTIONS

-- WORKER METHODS
local worker_methods = {}

function worker_methods:execute()
  local task       = self.task
  local iter       = 0
  local ITER_SLEEP = util.DEFAULT_SLEEP
  local MAX_ITER   = self.max_iter
  local MAX_SLEEP  = self.max_sleep
  local MAX_JOBS   = self.max_jobs
  local njobs      = 0
  local job_done
  while iter < MAX_ITER and njobs < MAX_JOBS do
    repeat
      collectgarbage("collect")
      task:update()
      local task_status,job = task:take_next_job()
      if job then
        print("# EXECUTING JOB ", job:status_string())
        job:execute() -- MAP or REDUCE
        print("# \t FINISHED")
        job_done = true
      else -- if dbname then ... else
        util.sleep(util.DEFAULT_SLEEP)
      end -- if dbname then ... else ... end
    until task:finished() -- repeat
    if job_done then
      print("# JOB DONE")
      iter       = 0
      ITER_SLEEP = util.DEFAULT_SLEEP
      njobs     = njobs + 1
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
  local cnn_obj = cnn(connection_string, dbname, auth_table),
  local obj = {
    cnn     = cnn_obj,
    task    = task(cnn),
    tmpname = os.tmpname(),
    --
    max_iter   = 20,
    max_sleep  = 20,
    max_jobs   = 1,
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
