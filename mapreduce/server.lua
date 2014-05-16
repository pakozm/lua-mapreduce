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

-- The server is one of the most important classes, together with job
-- class. Server allows to configure a (iterative) MapReduce task. It configures
-- 'map_jobs' and 'red_jobs' collections, where workers look to run jobs. The
-- programmer needs to execute server:configure(...) method in order to setup
-- the MapReduce task you want to execute. server:loop() method executes the
-- task and take note of statistics which will be written using a task object.

--[[ Example of map_jobs collection for wordcount
> db.map_jobs.find().limit(2).pretty()
{
	"_id" : "1",
	"value" : "mapreduce/server.lua",
	"creation_time" : 1400225797.659415,
	"started_time" : 1400225798.321171,
	"finished_time" : 1400225798.328765,
	"written_time" : 1400225798.346263,
	"cpu_time" : 0.017617999999999998,
	"real_time" : 0.022084951400756836,
	"status" : 4,
	"tmpname" : "lua_QcozTk",
	"worker" : "HOSTNAME"
}
{
	"_id" : "2",
	"value" : "mapreduce/worker.lua",
	"creation_time" : 1400225797.659444,
	"started_time" : 1400225798.346911,
	"finished_time" : 1400225798.350307,
	"written_time" : 1400225798.358262,
	"cpu_time" : 0.005623000000000003,
	"real_time" : 0.008959054946899414,
	"status" : 4,
	"tmpname" : "lua_QcozTk",
	"worker" : "HOSTNAME"
}
]]

--[[ Example of red_jobs collection for wordcount
> db.red_jobs.find().limit(2).pretty()
{
	"_id" : "1",
	"value" : {
		"result" : "result.P1",
		"mappers" : [
			"HOSTNAME",
			"HOSTNAME",
			"HOSTNAME",
			"HOSTNAME"
		],
		"file" : "/tmp/lua_sQHr2G/map_results.P1"
	},
	"creation_time" : 1400225798.665791,
	"started_time" : 1400225799.41176,
	"written_time" : 1400225799.423528,
	"cpu_time" : 0.006439999999999994,
	"real_time" : 0.008752822875976562,
	"status" : 4,
	"tmpname" : "lua_QcozTk",
	"worker" : "HOSTNAME",
}
{
	"_id" : "2",
	"value" : {
		"result" : "result.P2",
		"mappers" : [
			"HOSTNAME",
			"HOSTNAME",
			"HOSTNAME",
			"HOSTNAME"
		],
		"file" : "/tmp/lua_sQHr2G/map_results.P2"
	},
	"creation_time" : 1400225798.665803,
	"started_time" : 1400225799.426011,
	"written_time" : 1400225799.435997,
	"cpu_time" : 0.004669999999999994,
	"real_time" : 0.006902933120727539,
	"status" : 4,
	"tmpname" : "lua_QcozTk",
	"worker" : "HOSTNAME"
}
]]

local server = {
  _VERSION = "0.3",
  _NAME = "mapreduce.server",
}

local utils  = require "mapreduce.utils"
local task   = require "mapreduce.task"
local cnn    = require "mapreduce.cnn"
local fs     = require "mapreduce.fs"

local DEFAULT_HOSTNAME = utils.DEFAULT_HOSTNAME
local DEFAULT_IP = utils.DEFAULT_IP
local DEFAULT_DATE = utils.DEFAULT_DATE
local STATUS = utils.STATUS
local TASK_STATUS = utils.TASK_STATUS
local escape = utils.escape
local serialize_table_ipairs = utils.serialize_table_ipairs
local make_job = utils.make_job
local gridfs_lines_iterator = utils.gridfs_lines_iterator
local get_storage_from = utils.get_storage_from

-- PRIVATE FUNCTIONS AND METHODS

local function count_digits(n)
  -- sanity check
  assert(n >= 0, "Only valid for positive integers")
  if n == 0 then return 1 end
  local c = 0
  while n > 0 do
    n = math.floor(n/10)
    c = c + 1
  end
  return c
end

local function compute_real_time(db, ns)
  local out_min = assert( db:mapreduce(ns, [[
function() { emit(0, this.started_time) } ]],
                                       [[
function(k,v) {
  var min=v[0];
  for (var i=1; i<v.length; ++i)
    if (v[i]<min) min=v[i];
  return min;
}]]) )
  local out_max = assert( db:mapreduce(ns, [[
function() { emit(0, this.written_time) } ]],
                                       [[
function(k,v) {
  var max=v[0];
  for (var i=1; i<v.length; ++i)
    if (v[i]>max) max=v[i];
  return max;
}]]) )
  return out_max.results[1].value - out_min.results[1].value
end

local function compute_sum(db, ns, field)
  local result = assert( db:mapreduce(ns, string.format([[
function() { emit(0, this.%s) } ]], field),
                                      [[
function(k,v) { return Array.sum(v); }]]) )
  return result.results[1].value
end

-- returns a coroutine.wrap which returns true until all tasks are finished
local function make_task_coroutine_wrap(self,ns)
  local db = self.cnn:connect()
  local N = db:count(ns)
  return coroutine.wrap(function()
                          repeat
                            local db = self.cnn:connect()
                            local M = db:count(ns, { status = STATUS.WRITTEN })
                            if M then
                              io.stderr:write(string.format("\r\t %6.1f %% ",
                                                            M/N*100))
                              io.stderr:flush()
                              local ids = {}
                              local q = self.cnn:get_errors()
                              for v in q:results() do
                                table.insert(ids, v._id)
                                io.stderr:write(string.format("\nError from %s: %s\n",
                                                              v.worker, v.msg))
                                io.stderr:flush()
                              end
                              if #ids > 0 then
                                self.cnn:remove_errors(ids)
                              end
                            end
                            if not M or M < N then coroutine.yield(true) end
                          until M == N
                          io.stderr:write("\n")
                        end)
end

-- removes all the tasks which are not WRITTEN
local function remove_pending_tasks(db,ns)
  return db:remove(ns,
                   { ["$or"] = { { status = STATUS.BROKEN,  },
                                 { status = STATUS.WAITING  },
                                 { status = STATUS.FINISHED },
                                 { status = STATUS.RUNNING  }, } },
                   false)
end

-- insert jobs in mongo db and returns a coroutine ready to be executed as an
-- iterator
local function server_prepare_map(self)
  local count = 0
  local db = self.cnn:connect()
  local map_jobs_ns = self.task:get_map_jobs_ns()
  remove_pending_tasks(db, map_jobs_ns)
  -- create map tasks in mongo database
  local keys_check = {}
  self.taskfn.taskfn(function(key,value)
                       count = count + 1
                       assert(tostring(key),
                              "taskfn must return a convertible to string key")
                       assert(not keys_check[key],
                              string.format("Duplicate key: %s", key))
                       keys_check[key] = true
                       local tvalue = type(value)
                       if tvalue == "table" then
                         local json_value = utils.tojson(value)
                         assert(#json_value <= utils.MAX_TASKFN_VALUE_SIZE,
                                "Exceeded maximum taskfn value size")
                       end
                       -- FIXME: check how to process task keys which are
                       -- defined by a previously broken execution and didn't
                       -- belong to the current task execution
                       assert( db:insert(map_jobs_ns, make_job(key,value)) )
                     end)
  self.task:set_task_status(TASK_STATUS.MAP)
  -- this coroutine WAITS UNTIL ALL MAPS ARE DONE
  return make_task_coroutine_wrap(self, map_jobs_ns),count
end

-- insert the job in the mongo db and returns a coroutine
local function server_prepare_reduce(self)
  local db     = self.cnn:connect()
  local dbname = self.cnn:get_dbname()
  local map_results_ns = self.task:get_map_results_ns()
  local red_jobs_ns = self.task:get_red_jobs_ns()
  remove_pending_tasks(db, red_jobs_ns)
  -- list all the mappers
  local map_jobs_ns = self.task:get_map_jobs_ns()
  local query = db:query(map_jobs_ns,{})
  local map_hostnames = {}
  for q in query:results() do map_hostnames[tostring(q._id)] = q.worker end
  -- list the filenames generated by mappers in order to create the reduce jobs
  local storage,path = self.task:get_storage()
  local fs,make_builder,make_lines_iterator = fs.router(self.cnn,nil,
                                                        storage,path)
  local match_str = string.format("^%s/.*P.*M.*$",path):gsub("//","/")
  local filenames = {}
  local list = fs:list({ filename = { ["$regex"] = match_str } })
  local part_keys = {}
  local max_part_key = 0
  local mappers_by_part_key = {}
  for obj in list:results() do
    local filename = obj.filename
    -- sanity check
    assert(filename:match(match_str))
    -- create reduce jobs in mongo database, from partitioned space
    local part_key,mapper_key = filename:match("^.*.P([^%.]+)%.M([^%.]*)$")
    part_key = assert(tonumber(part_key))
    part_keys[part_key] = true
    max_part_key = math.max(max_part_key, part_key)
    -- annotate the mapper
    mappers_by_part_key[part_key] = mappers_by_part_key[part_key] or {}
    table.insert(mappers_by_part_key[part_key], map_hostnames[mapper_key])
  end
  local part_key_digits = count_digits(max_part_key)
  local result_str_format = "%s.P%0" .. tostring(part_key_digits) .. "d"
  local count=0
  for part_key,_ in pairs(part_keys) do
    count = count + 1
    local value = {
      mappers = mappers_by_part_key[part_key],
      file    = string.format("%s/%s.P%d", path, map_results_ns, part_key),
      result  = string.format(result_str_format, self.result_ns, part_key),
    }
    self.cnn:annotate_insert(red_jobs_ns, make_job(part_key, value))
  end
  self.cnn:flush_pending_inserts(0)
  self.task:set_task_status(TASK_STATUS.REDUCE)
  -- this coroutine WAITS UNTIL ALL REDUCES ARE DONE
  return make_task_coroutine_wrap(self, red_jobs_ns),count
end

local function server_drop_collections(self)
  local db = self.cnn:connect()
  local dbname = self.cnn:get_dbname()
  -- drop all the collections
  for _,name in ipairs(db:get_collections(dbname)) do
    db:drop_collection(name)
  end
  local gridfs = self.cnn:gridfs()
  local list = gridfs:list()
  for v in list:results() do
    gridfs:remove_file(v.filename)
  end
end

-- finalizer for the map-reduce process
local function server_final(self)
  -- FIXME: self.result_ns could contain especial characters, it will be
  -- necessary to escape them
  local match_str = string.format("^%s",self.result_ns)
  local gridfs = self.cnn:gridfs()
  local files = gridfs:list({ filename = { ["$regex"] = match_str } })
  local files_tbl
  local k=0
  local current_filename
  local lines_iterator
  -- iterator which is given to final function, allows to traverse all the
  -- results by pairs key,value
  local pair_iterator = function()
    if not files_tbl then
      files_tbl = {}
      for v in files:results() do
        assert(v.filename:match(match_str))
        table.insert(files_tbl, v.filename)
      end
      table.sort(files_tbl)
    end
    local line
    repeat
      if lines_iterator then
        line = lines_iterator()
      end
      if not line then
        k=k+1
        current_filename = files_tbl[k]
        if current_filename then
          lines_iterator = gridfs_lines_iterator(gridfs,current_filename)
        end
      end
    until current_filename == nil or line ~= nil
    if line then
      return load(line)()
    end
  end
  -- the reply could be: false/nil, true, "loop"
  local reply = self.finalfn.finalfn(pair_iterator)
  local remove_all = (reply == true) or (reply == "loop")
  if reply ~= "loop" and reply ~= true and reply ~= false and reply ~= nil then
    io.stderr:write("# WARNING!!! INCORRECT FINAL RETURN: " ..
                      tostring(reply) .. "\n")
  end
  -- drop collections, except reduce result and task status
  local db = self.cnn:connect()
  --
  local task = self.task
  if reply == "loop" then
    io.stderr:write("# LOOP again\n")
    db:drop_collection(task:get_map_jobs_ns())
    db:drop_collection(task:get_red_jobs_ns())
  else
    self.finished = true
    task:set_task_status(TASK_STATUS.FINISHED)
  end
  -- remove gridfs files
  local gridfs = self.cnn:gridfs()
  local list = gridfs:list()
  for v in list:results() do
    if not v.filename:match(match_str) or remove_all then
      gridfs:remove_file(v.filename)
    end
  end
end

-- SERVER METHODS
local server_methods = {}

-- configures the server with the script string
function server_methods:configure(params)
  params.storage = string.format("%s:%s",get_storage_from(params.storage,true))
  self.configured           = true
  self.configuration_params = params
  self.init_args            = params.init_args
  local dbname = self.dbname
  local scripts = {}
  self.result_ns = params.result_ns or "result"
  assert(params.taskfn and params.mapfn and params.partitionfn and params.reducefn,
         "Fields taskfn, mapfn, partitionfn and reducefn are mandatory")
  for _,name in ipairs{ "taskfn", "mapfn", "partitionfn", "reducefn", "finalfn",
                      "combinerfn" } do
    assert( (params[name] and type(params[name]) == "string") or
              (not params[name] and (name=="finalfn" or name == "combinerfn") ),
           string.format("Needs a %s module with %s function", name, name))
    if params[name] then
      local aux = require(params[name])
      assert(type(aux) == "table",
             string.format("Module %s must return a table",
                           name))
      assert(aux[name],
             string.format("Module %s must return a table with the field %s",
                           name, name))
      assert(aux.init, string.format("Init function is needed: %s", name))
      scripts[name] = params[name]
    end
  end
  local db = self.cnn:connect()
  --
  self.taskfn = require(scripts.taskfn)
  if scripts.finalfn then
    self.finalfn = require(scripts.finalfn)
  else
    self.finalfn = { finalfn = function() end, init = function() end }
  end
  local init_functions = {
    [self.taskfn.init] = self.taskfn.init,
    [self.finalfn.init] = self.finalfn.init,
  }
  for _,init in pairs(init_functions) do init(self.init_args) end
  self.mapfn = params.mapfn
  self.reducefn = params.reducefn
  self.partitionfn = params.partitionfn
end

-- makes all the map-reduce process, looping into the coroutines until all tasks
-- are done
function server_methods:loop()
  assert(self.configured, "Call to server:configure(...) method is mandatory")
  local it = 0
  repeat
    local skip_map,initialize=false,true
    if it == 0 then
      -- in the first iteration, we check if the task is a new fresh execution
      -- or if a previous broken task exists
      self.task:update()
      if self.task:has_status() then
        local status = self.task:get_task_status()
        if status == TASK_STATUS.REDUCE then
          -- if the task was in reduce state, skip map jobs and re-run reduce
          io.stderr:write("# WARNING: TRYING TO RESTORE A BROKEN TASK\n")
          skip_map   = true
          initialize = false
          self.configuration_params.storage =
            string.format("%s:%s", self.task:get_storage())
        elseif status == TASK_STATUS.FINISHED then
          -- if the task was finished, therefore it is a shit, drop old data
          server_drop_collections(self)
        else
          -- otherwise, the task is in WAIT or MAP states, try to restore from
          -- there
          initialize = false
        end
      end -- if task has status
    end -- if it == 0
    if initialize then
      -- count one iteration
      it = it+1
      -- create task object
      self.task:create_collection(TASK_STATUS.WAIT,
                                  self.configuration_params, it)
    else
      it = self.task:get_iteration()
      self.task:create_collection(self.task:get_task_status(),
                                  self.configuration_params, it)
    end
    io.stderr:write(string.format("# Iteration %d\n", it))
    local start_time = utils.time()
    self.task:insert_started_time(start_time)
    if not skip_map then
      -- MAP EXECUTION
      io.stderr:write("# \t Preparing Map\n")
      local do_map_step,map_count = server_prepare_map(self)
      collectgarbage("collect")
      io.stderr:write(string.format("# \t Map execution, size= %d\n",
                                    map_count))
      while do_map_step() do
        utils.sleep(utils.DEFAULT_SLEEP)
        collectgarbage("collect")
      end
    end
    local db = self.cnn:connect()
    local map_count = db:count(self.task:get_map_jobs_ns())
    -- REDUCE EXECUTION
    collectgarbage("collect")
    io.stderr:write("# \t Preparing Reduce\n")
    local do_reduce_step = server_prepare_reduce(self)
    local db = self.cnn:connect()
    local red_count = db:count(self.task:get_red_jobs_ns())
    collectgarbage("collect")
    io.stderr:write(string.format("# \t Reduce execution, num_files= %d  size= %d\n",
                                  red_count * map_count, red_count))
    while do_reduce_step() do
      utils.sleep(utils.DEFAULT_SLEEP)
      collectgarbage("collect")
    end
    -- TIME
    local end_time = utils.time()
    local total_time = end_time - start_time
    self.task:insert_finished_time(end_time)
    -- FINAL EXECUTION
    io.stderr:write("# \t Final execution\n")
    collectgarbage("collect")
    server_final(self)
    --
    -- STATISTICS
    local map_sum_cpu_time = compute_sum(db, self.task:get_map_jobs_ns(),
                                         "cpu_time")
    local red_sum_cpu_time = compute_sum(db, self.task:get_red_jobs_ns(),
                                         "cpu_time")
    local map_sum_real_time = compute_sum(db, self.task:get_map_jobs_ns(),
                                          "real_time")
    local red_sum_real_time = compute_sum(db, self.task:get_red_jobs_ns(),
                                          "real_time")
    local map_real_time    = compute_real_time(db, self.task:get_map_jobs_ns())
    local red_real_time    = compute_real_time(db, self.task:get_red_jobs_ns())

    io.stderr:write(string.format("#   Map sum(cpu_time)     %f\n",
                                  map_sum_cpu_time))
    io.stderr:write(string.format("#   Reduce sum(cpu_time)  %f\n",
                                  red_sum_cpu_time))
    io.stderr:write(string.format("# Sum(cpu_time)           %f\n",
                                  map_sum_cpu_time + red_sum_cpu_time))
    io.stderr:write(string.format("#   Map sum(real_time)    %f\n",
                                  map_sum_real_time))
    io.stderr:write(string.format("#   Reduce sum(real_time) %f\n",
                                  red_sum_real_time))
    io.stderr:write(string.format("# Sum(real_time)          %f\n",
                                  map_sum_real_time + red_sum_real_time))
    io.stderr:write(string.format("# Sum(sys_time)           %f\n",
                                  map_sum_real_time + red_sum_real_time -
                                    map_sum_cpu_time - red_sum_cpu_time))
    io.stderr:write(string.format("#   Map cluster time      %f\n", map_real_time))
    io.stderr:write(string.format("#   Reduce cluster time   %f\n", red_real_time))
    io.stderr:write(string.format("# Cluster time            %f\n",
                                  map_real_time + red_real_time))
    --
    self.task:insert{
      stats = {
        map_sum_cpu_time = map_sum_cpu_time,
        red_sum_cpu_time = red_sum_cpu_time,
        total_sum_cpu_time = map_sum_cpu_time + red_sum_cpu_time,
        map_sum_real_time = map_sum_real_time,
        red_sum_real_time = red_sum_real_time,
        total_sum_real_time = map_sum_real_time + red_sum_real_time,
        sum_sys_time = (map_sum_real_time + red_sum_real_time -
                          map_sum_cpu_time - red_sum_cpu_time),
        map_real_time = map_real_time,
        red_real_time = red_real_time,
        total_real_time = map_real_time + red_real_time,
        iteration_time = total_time,
      }
    }
    --
    io.stderr:write(string.format("# Server time %f\n", total_time))
  until self.finished
  local storage,path = self.configuration_params.storage:match("([^:]+):(/.*)")
  if storage == "shared" then utils.remove(path) end
end

-- SERVER METATABLE
local server_metatable = { __index = server_methods }

server.new = function(connection_string, dbname, auth_table)
  local cnn_obj = cnn(connection_string, dbname, auth_table)
  local obj = {
    cnn  = cnn_obj,
    task = task(cnn_obj),
  }
  setmetatable(obj, server_metatable)
  return obj
end

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
server.utest = function()
  assert(count_digits(0)    == 1)
  assert(count_digits(1)    == 1)
  assert(count_digits(9)    == 1)
  assert(count_digits(10)   == 2)
  assert(count_digits(99)   == 2)
  assert(count_digits(111)  == 3)
  assert(count_digits(1111) == 4)
  -- compute real time and compute sum
  local c  = cnn("localhost", "test")
  local db = c:connect()
  db:drop_collection("test.times")
  local min_started,max_written = 10,20
  db:insert("test.times",
            {
              started_time = min_started,
              written_time = 16
            })
  db:insert("test.times",
            {
              started_time = 14,
              written_time = max_written,
            })
  assert(compute_real_time(db, "test.times") == (max_written-min_started))
  assert(compute_sum(db, "test.times", "started_time") == min_started + 14)
  assert(compute_sum(db, "test.times", "written_time") == max_written + 16)
end

------------------------------------------------------------------------------

return server
