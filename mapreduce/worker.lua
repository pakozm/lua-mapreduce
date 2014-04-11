local util   = require "mapreduce.util"
local worker = {
  _VERSION = "0.1",
  _NAME = "mapreduce.worker",
}

-- PRIVATE FUNCTIONS

-- WORKER METHODS
local worker_methods = {}

function worker_methods:execute()
  local task       = self.task
  local job        = self.job
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
      task:update()
      local fn = job:take_next(task)
      if fn then
        print("# EXECUTING TASK ", job:status_string())
        fn() -- MAP or REDUCE
        print("# \t FINISHED")
        task_done = true
      else -- if dbname then ... else
        util.sleep(util.DEFAULT_SLEEP)
      end -- if dbname then ... else ... end
    until task:finished() -- repeat
    if task_done then
      print("# TASK DONE")
      iter       = 0
      ITER_SLEEP = util.DEFAULT_SLEEP
      ntasks     = ntasks + 1
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
  local cnn = util.cnn(connection_string, dbname, auth_table),
  local obj = {
    cnn     = cnn,
    task    = util.task(cnn),
    job     = util.job(cnn)
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
