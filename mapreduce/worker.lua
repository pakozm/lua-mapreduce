--[[
  This file is part of Lua-MapReduce
  
  Copyright 2014, Francisco Zamora-Martinez
  
  The Lua-MapReduce toolkit is free software; you can redistribute it and/or modify it
  under the terms of the GNU General Public License version 3 as
  published by the Free Software Foundation
  
  This library is distributed in the hope that it will be useful, but WITHOUT
  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
  for more details.
  
  You should have received a copy of the GNU General Public License
  along with this library; if not, write to the Free Software Foundation,
  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
]]

-- The worker is a class which implements the basic procedure executed in a
-- client machine. Workers execute map/reduce jobs, until all task jobs are
-- being executed. They could be configured to process more than one task.  The
-- most important methods are "configure" and "execute". Execute method runs the
-- job in a protected environment, allowing to capture asserts and Lua errors.
-- Broken jobs will be tagged adequately in the Mongo database. Jobs are taken
-- from 'map_jobs' and 'red_jobs' collections. Intermediate data is stored in
-- gridfs or shared file system or locally and copied using scp.

local worker = {
  _VERSION = "0.2",
  _NAME = "mapreduce.worker",
}

local job    = require "mapreduce.job"
local utils  = require "mapreduce.utils"
local task   = require "mapreduce.task"
local cnn    = require "mapreduce.cnn"

-- PRIVATE FUNCTIONS AND PROPERTIES

-- executes the worker main loop; it runs querying the task object for new jobs
local worker_execute = function(self)
  print("# HOSTNAME ", utils.get_hostname())
  local task       = self.task
  local iter       = 0
  local ITER_SLEEP = utils.DEFAULT_SLEEP
  -- limits parameters, given at worker:configure(...) method
  local MAX_ITER   = self.max_iter
  local MAX_SLEEP  = self.max_sleep
  local MAX_TASKS  = self.max_tasks
  -----------------------------------------------------------
  local ntasks     = 0
  local job_done
  while iter < MAX_ITER and ntasks < MAX_TASKS do
    collectgarbage("collect")
    local counter = 0
    repeat
      counter = counter + 1
      if counter % utils.MAX_IT_WO_CGARBAGE then collectgarbage("collect") end
      -- synchronize and update task object and MongoDB, and process takes a job
      -- if possible
      task:update()
      local task_status,job = task:take_next_job(self.tmpname)
      self.current_job = job
      if job then
        if not job_done then print("# New TASK ready") end
        print(string.format("# \t Executing %s job _id: %q",
                            task_status, job:status_string()))
        local t1 = utils.time()
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
        -- job execution
        local elapsed_time = job:execute() -- map/reduce
        ----------------------------------------------------------------------
        ----------------------------------------------------------------------
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
      task.reset_cache()
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

-- execute wrapper, a public method which runs the jobs in a protected
-- environment (xpcall)
function worker_methods:execute()
  local num_failed_jobs = 0
  local failed_jobs = {}
  repeat
    local ok,msg = xpcall(worker_execute, debug.traceback, self)
    if not ok then
      -- in case of error, if a job is available, mark it as broken; additionally,
      -- insert the error message into 'errors' collection
      if self.current_job then
        self.current_job:mark_as_broken()
        local id = self.current_job:get_id()
        if not failed_jobs[id] then
          num_failed_jobs = num_failed_jobs + 1
        end
        failed_jobs[id] = true
      end
      self.cnn:flush_pending_inserts(0)
      self.cnn:insert_error(utils.get_hostname(), msg)
      print(string.format("Error executing a job: %s",msg))
      utils.sleep(utils.DEFAULT_SLEEP*4)
    end
  until ok or num_failed_jobs >= utils.MAX_WORKER_RETRIES
  print(string.format("# Worker retries: %d",num_failed_jobs))
  if num_failed_jobs >= utils.MAX_WORKER_RETRIES then
    error("Maximum number of retries achieved")
  end
end

-- configuration of worker, allows to change parameters 'max_iter', 'max_sleep'
-- and 'max_tasks'
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
    -- default values
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
worker.utest = function()
end

------------------------------------------------------------------------------

return worker
