local server = {
  _VERSION = "0.1",
  _NAME = "mapreduce.server",
}

local utils  = require "mapreduce.utils"
local task   = require "mapreduce.task"
local cnn    = require "mapreduce.cnn"

local DEFAULT_HOSTNAME = utils.DEFAULT_HOSTNAME
local DEFAULT_IP = utils.DEFAULT_IP
local DEFAULT_DATE = utils.DEFAULT_DATE
local STATUS = utils.STATUS
local TASK_STATUS = utils.TASK_STATUS

local make_job = utils.make_job

-- PRIVATE FUNCTIONS

-- returns a coroutine.wrap which returns true until all tasks are finished
local function make_task_coroutine_wrap(self,ns)
  local db = self.cnn:connect()
  local N = db:count(ns)
  return coroutine.wrap(function()
                          repeat
                            local db = self.cnn:connect()
                            local M = db:count(ns, { status = STATUS.FINISHED })
                            if M then
                              io.stderr:write(string.format("\r%6.1f %% ",
                                                            M/N*100))
                              io.stderr:flush()
                            end
                            if not M or M < N then coroutine.yield(true) end
                          until M == N
                          io.stderr:write("\n")
                        end)
end

-- removes all the tasks which are in WAITING or BROKEN states
local function remove_pending_tasks(db,ns)
  return db:remove(ns,
                   { ["$or"] = { { status = STATUS.BROKEN, },
                                 { status = STATUS.WAITING } } },
                   false)
end

-- removes all the tasks at the given collection
local function remove_collection_data(db,ns)
  return db:remove(ns, {}, false)
end

-- SERVER METHODS
local server_methods = {}

-- configures the server with the script string
function server_methods:configure(params)
  self.configured = true
  self.task_args = params.task_args
  self.map_args = params.map_args
  self.reduce_args = params.reduce_args
  self.final_args = params.final_args
  local dbname = self.dbname
  local taskfn,mapfn,reducefn,finalfn
  local scripts = {}
  assert(params.taskfn and params.mapfn and params.reducefn,
         "Fields taskfn, mapfn and reducefn are mandatory")
  for _,name in ipairs{ "taskfn", "mapfn", "reducefn", "finalfn" } do
    assert(params[name] and type(params[name]) == "string",
           string.format("Needs a %s module", name))
    local aux = require(params[name])
    assert(type(aux) == "table",
           string.format("Module %s must return a table",
                         name))
    assert(aux.func,
           string.format("Module %s must return a table with the field func",
                         name))
    assert(aux.init or not params[ name:gsub("fn","_args") ],
           string.format("When args are given, a init function is needed: %s",
                         name))
    scripts[name] = params[name]
  end
  local db = self.cnn:connect()
  --
  self.taskfn = require(scripts.taskfn)
  if scripts.finalfn then
    self.finalfn = require(scripts.finalfn)
  else
    self.finalfn = { func = function() end }
  end
  if self.taskfn.init then self.taskfn.init(self.task_args) end
  if self.finalfn.init then self.finalfn.init(self.final_args) end
  self.mapfn = params.mapfn
  self.reducefn = params.reducefn
  -- create task object
  self.task:create_collection(TASK_STATUS.WAIT, params)
end

-- insert the job in the mongo db and returns a coroutine ready to be executed
-- as an iterator
function server_methods:prepare_map()
  local db = self.cnn:connect()
  local map_jobs_ns = self.task:get_map_jobs_ns()
  remove_pending_tasks(db, map_jobs_ns)
  -- create map tasks in mongo database
  local f = self.taskfn.func
  local count = 0
  for key,value in coroutine.wrap(f) do
    count = count + 1
    assert(count < utils.MAX_NUMBER_OF_TASKS,
           "Overflow maximum number of tasks: " .. utils.MAX_NUMBER_OF_TASKS)
    assert(tostring(key), "taskfn must return a string key")
    -- FIXME: check what happens when the insert is a duplicate of an existing
    -- key
    
    -- FIXME: check how to process task keys which are defined by a previously
    -- broken execution and didn't belong to the current task execution
    assert( db:insert(map_jobs_ns, make_job(key,value)) )
  end
  self.task:set_task_status(TASK_STATUS.MAP)
  -- this coroutine WAITS UNTIL ALL MAPS ARE DONE
  return make_task_coroutine_wrap(self, map_jobs_ns)
end

-- insert the job in the mongo db and returns a coroutine
function server_methods:prepare_reduce()
  local db = self.cnn:connect()
  local dbname = self.cnn:get_dbname()
  local map_jobs_ns = self.task:get_map_jobs_ns()
  local map_result_ns = self.task:get_map_results_ns()
  local red_jobs_ns = self.task:get_red_jobs_ns()
  remove_pending_tasks(db, red_jobs_ns)
  -- aggregation of map results
  
  -- FIXME: this aggregation procedure only allows to generate 16M document
  -- sizes
  local group_result = "group_result"
  local mongo_map_fn = [[
function() {
    emit(this.key, { v : [ this.value ] });
}]]
  local mongo_red_fn = [[
function(key,values){
    var result = [];
    values.forEach(function(d) {
        d.v.forEach(function(a) {
            result.push(a);
        });
    });
    return { v : result };
}]]
  for name in self.task:map_results_iterator() do
    local collection = name:match("^[^%.]+%.(.+)$")
    assert(db:eval(dbname,
                   string.format([[
function() {
  db.%s.mapReduce(%s, %s, { out : { reduce : "%s" } });
};
]], collection, mongo_map_fn, mongo_red_fn, group_result)))
    --local r = db:mapreduce(name, mongo_map_fn, mongo_red_fn,
    --                       {}, group_result)
    db:drop_collection(name)
  end
  -- create reduce tasks in mongo database, from aggregated map results
  local r = assert( db:query(dbname .. "." .. group_result) )
  for pair in r:results() do
    -- FIXME: check what happens when the insert is a duplicate of an existing
    -- key
    
    -- FIXME: check how to process task keys which are defined by a previously
    -- broken execution and didn't belong to the current task execution
    local key,value = pair._id, pair.value
    assert( type(value)  == "table" and value.v )
    assert( db:insert(red_jobs_ns, make_job(key,value.v)) )
  end
  db:drop_collection(dbname .. "." .. group_result)
  self.task:set_task_status(TASK_STATUS.REDUCE)
  -- this coroutine WAITS UNTIL ALL REDUCES ARE DONE
  return make_task_coroutine_wrap(self, red_jobs_ns)
end

function server_methods:drop_collections()
  local db = self:connect()
  local dbname = db:get_dbname()
  -- drop all the collections
  for _,name in ipairs(db:get_collections(dbname)) do
    db:drop_collection(name)
  end
end

-- finalizer for the map-reduce process
function server_methods:final()
  local db = self.cnn:connect()
  local task = self.task
  task:set_task_status(TASK_STATUS.FINISHED)
  local results_ns = task:get_red_results_ns()
  local q = db:query(results_ns, {})
  self.finalfn.func(q, db, results_ns)
  -- drop collections, except reduce result and task status
  db:drop_collection(task:get_map_jobs_ns())
  db:drop_collection(task:get_map_results_ns())
  db:drop_collection(task:get_red_jobs_ns())
  self.finished = true
end

-- makes all the map-reduce process, looping into the coroutines until all tasks
-- are done
function server_methods:loop()
  io.stderr:write("# Preparing MAP\n")
  local do_map_step = self:prepare_map()
  collectgarbage("collect")
  io.stderr:write("# MAP execution\n")
  while do_map_step() do
    utils.sleep(utils.DEFAULT_SLEEP)
    collectgarbage("collect")
  end
  io.stderr:write("# Preparing REDUCE\n")
  local do_reduce_step = self:prepare_reduce()
  collectgarbage("collect")
  io.stderr:write("# REDUCE execution\n")
  while do_reduce_step() do
    utils.sleep(utils.DEFAULT_SLEEP)
    collectgarbage("collect")
  end
  io.stderr:write("# FINAL execution\n")
  collectgarbage("collect")
  self:final()
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
server.utest = function(connection_string, dbname, auth_table)
  local connection_string = connection_string or "localhost"
  local dbname = dbname or "tmp"
  -- check server connection
  local s = server.new(connection_string, dbname, auth_table)
  s.configured = true
  s.mapfn      = "dummy"
  s.reducefn   = "dummy"
  utils.task.create_collection(s,s.task_ns,"WAIT")
  -- TODO: check the task_status
  -----------------------------
  assert(s.connection_string == connection_string)
  assert(s.dbname == dbname)
  assert(s.map_ns == dbname .. ".map_tasks")
  assert(s.map_result_ns == dbname .. ".map_results")
  assert(s.red_ns == dbname .. ".red_tasks")
  assert(s.red_result_ns == dbname .. ".red_results")
  assert(s.task_ns == dbname .. ".task")
  local db = assert(s:connect())
  assert(s.db)
  assert(db:find_one(s.task_ns).job == "WAIT")
  -- clean previous failed tests
  db:drop_collection(s.map_ns)
  db:drop_collection(s.map_result_ns)
  db:drop_collection(s.red_ns)
  -- check prepare_map
  s.taskfn = {
    func = function()
      for i=1,10 do coroutine.yield(i,{ file=i }) end
    end
  }
  local do_map_step = s:prepare_map()
  assert(db:find_one(s.task_ns).job == "MAP")
  assert(do_map_step)
  assert(db:count(s.map_ns, { status = STATUS.WAITING }) == 10)
  assert(do_map_step())
  assert(do_map_step())
  assert(db:update(s.map_ns, {},
                   { ["$set"] = { status = STATUS.FINISHED } },
                   false, true))
  assert(db:count(s.map_ns, { status = STATUS.FINISHED }) == 10)
  assert(not do_map_step())
  remove_collection_data(db,s.map_ns)
  -- check prepare_reduce
  db:insert(s.map_result_ns, { key="one", values={ 4, 5, 1, 3 } })
  db:insert(s.map_result_ns, { key="two", values={ 1, 2, 3 } })
  local do_red_step = s:prepare_reduce()
  assert(db:find_one(s.task_ns).job == "REDUCE")
  assert(do_red_step)
  assert(db:count(s.red_ns, { status = STATUS.WAITING }) == 2)
  assert(do_red_step())
  assert(do_red_step())
  assert(db:update(s.red_ns, {},
                   { ["$set"] = { status = STATUS.FINISHED } },
                   false, true))
  assert(db:count(s.red_ns, { status = STATUS.FINISHED }) == 2)
  assert(not do_red_step())
  remove_collection_data(db,s.red_ns)
  db:drop_collection(s.map_result_ns)
  -- finalize
  db:insert(s.red_result_ns, { key="one", value=13 })
  db:insert(s.red_result_ns, { key="two", value=6 })
  s.finalfn = {
    func = function(db,red_result_ns)
      local r = assert(db:query(red_result_ns, {}))
      assert(r:itcount() == 2)
      for pair in r:results() do
        if pair.key == "one" then
          assert(pair.value == 13)
        elseif pair.key == "two" then
          assert(pair.value == 6)
        else
          error("Incorrect key: " .. pair.key)
        end
      end
    end
  }
  s:finalize()
  assert(db:find_one(s.task_ns).job == "FINISHED")
  db:drop_collection(s.red_result_ns)
  db:drop_collection(s.task_ns)
end

------------------------------------------------------------------------------

return server
