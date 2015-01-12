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

-- Task class sets the task job for the server, which indicates the workers
-- which kind of work (MAP,REDUCE,...) must perform. Workers use task objects to
-- take a job from Mongo database, and to configure the job parameters. The task
-- is related with 'task' collection in MongoDB. This collection is a singleton
-- with data necessary to describe the MapReduce task. The following is an
-- example of the 'task' collection after the execution of wordcount example:

--[[
> db.task.find().pretty()
{
	"_id" : "unique",
	"init_args" : {
		"-1" : "lua",
		"0" : "execute_server.lua"
	},
	"status" : "FINISHED",
	"storage" : "gridfs:/tmp/lua_sQHr2G",
	"iteration" : 1,
	"mapfn" : "examples.WordCount.mapfn",
	"partitionfn" : "examples.WordCount.partitionfn",
	"reducefn" : "examples.WordCount.reducefn",
	"combinerfn" : "examples.WordCount.reducefn",
	"finished_time" : 1400225799.669539,
	"started_time" : 1400225797.659308,
	"stats" : {
  		"map_sum_cpu_time" : 0.039485000000000006,
		"map_sum_real_time" : 0.07356524467468262,
		"map_real_time" : 0.08571505546569824,
		"red_sum_cpu_time" : 0.05029200000000001,
		"red_sum_real_time" : 0.0801548957824707,
		"red_real_time" : 0.12635302543640137,
		"sum_sys_time" : 0.0639431404571533,
		"total_sum_cpu_time" : 0.08977700000000002,
		"total_sum_real_time" : 0.15372014045715332
		"total_real_time" : 0.2120680809020996,
		"iteration_time" : 2.0102310180664062,
	}
}
]]

local task = {
  _VERSION = "0.4",
  _NAME = "task",
}

local utils       = require "mapreduce.utils"
local job         = require "mapreduce.job"
local tuple       = require "mapreduce.tuple"
local STATUS      = utils.STATUS
local TASK_STATUS = utils.TASK_STATUS
local grp_tmp_dir = utils.GRP_TMP_DIR
local get_storage_from = utils.get_storage_from

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
    self.current_args  = self.tbl.init_args
  elseif self.tbl.status == TASK_STATUS.REDUCE  then
    self.current_jobs_ns = self.red_jobs_ns
    self.current_results_ns = self.red_results_ns
    self.current_fname = self.tbl.reducefn
    self.current_args  = self.tbl.init_args
  end
end

-- PUBLIC METHODS

function task:create_collection(task_status, params, iteration)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = {
                        status         = task_status,
                        --
                        mapfn          = params.mapfn,
                        reducefn       = params.reducefn,
                        partitionfn    = params.partitionfn,
                        combinerfn     = params.combinerfn,
                        init_args      = params.init_args,
                        --
                        storage        = params.storage,
                        --
                        iteration      = iteration,
                        started_time   = 0,
                        finished_time  = 0,
                    }, },
                    true, false) )
  self.tbl = params
end

function task:get_storage()
  assert(self.tbl)
  return get_storage_from(self.tbl.storage)
end

function task:insert_finished_time(t)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = {
                        finished_time = t,
                    }, },
                    false, false) )
end

function task:insert_started_time(t)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = {
                        started_time = t,
                    }, },
                    false, false) )
end

function task:insert(t)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = t },
                    false, false) )
end

function task:update()
  local db = self.cnn:connect()
  local tbl = db:find_one(self.ns, { _id = "unique" })
  if tbl then
    task_set_task_status(self, tbl.status, tbl, db)
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

function task:has_status()
  return self.tbl ~= nil
end

function task:get_iteration()
  return self.tbl.iteration
end

function task:set_task_status(status, extra)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { _id = "unique" },
                    { ["$set"] = { status = status } },
                    true, false) )
  if extra then
    assert( db:update(self.ns, { _id = "unique" },
                      { ["$set"] = extra },
                      true, false) )
  end
  task_set_task_status(self, status, self.tbl)
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

function task:get_reduce_fname()
  return self.tbl.reducefn
end

function task:get_reduce_args()
  return self.tbl.init_args
end

function task:get_partition_fname()
  return self.tbl.partitionfn
end

function task:get_partition_args()
  return self.tbl.init_args
end

-- TASK INTERFACE

local cache_map_ids     = {}
local cache_inv_map_ids = {}
function task.reset_cache()
  cache_map_ids     = {}
  cache_inv_map_ids = {}
end

-- workers use this method to load a new job in the caller object
local count_idle_iterations = 0
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
  local t = utils.time()
  local query = {
    ["$or"] = {
      { status = STATUS.WAITING, },
      { status = STATUS.BROKEN, },
    },
  }
  -- after first iteration, map jobs done previously will be taken if possible,
  -- reducing the overhead for loading data
  if self:get_iteration() > 1 and task_status == TASK_STATUS.MAP then
    query._id = { ["$in"] = cache_map_ids }
    -- check the count
    if db:count(jobs_ns, query) == 0 then
      -- if zero, count one more IDLE iteration, and check the counter
      count_idle_iterations = count_idle_iterations + 1
      query._id = nil
      -- in case the counter is within the limit, take only a broken job in case
      -- the counter exceeds its limit, take any job, broken or waiting
      if count_idle_iterations <= utils.MAX_IDLE_COUNT then
	query["$or"] = nil
	query.status = STATUS.BROKEN
      end
    end
  end
  local set_query = {
    worker       = utils.get_hostname(),
    tmpname      = tmpname_summary(tmpname),
    started_time = t,
    status       = STATUS.RUNNING,
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
    -- reset the IDLE counter
    count_idle_iterations = 0
    if task_status == TASK_STATUS.MAP then
      local _id = job_tbl._id
      if not cache_inv_map_ids[_id] then
        cache_inv_map_ids[_id] = true
        table.insert(cache_map_ids, _id)
      end
    end
    local storage,path = self:get_storage()
    return task_status,job(self.cnn, job_tbl, task_status,
                           self:get_fname(), self:get_args(),
                           jobs_ns, results_ns,
                           nil, -- not_executable = false
                           self:get_reduce_fname(),
                           self:get_partition_fname(),
                           storage, path)
    
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
    map_results_ns = "map_results",
    red_jobs_ns    = dbname .. ".red_jobs",
    red_results_ns = "red_results",
  }
  setmetatable(obj, { __index=self })
  --
  local db = obj.cnn:connect()
  return obj
end
setmetatable(task,task)

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
task.utest = function()
  assert(tmpname_summary("/tmp/blah") == "blah")
end

------------------------------------------------------------------------

return task
