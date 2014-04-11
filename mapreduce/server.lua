local mongo  = require "mongo"
local util   = require "mapreduce.util"
local server = {
  _VERSION = "0.1",
  _NAME = "mapreduce.server",
}

local DEFAULT_HOSTNAME = util.DEFAULT_HOSTNAME
local DEFAULT_IP = util.DEFAULT_IP
local DEFAULT_DATE = util.DEFAULT_DATE
local STATUS = util.STATUS

local make_task = util.make_task

-- PRIVATE FUNCTIONS

-- set key to be a unique index
local function ensure_unique_index(db,ns)
  assert(db:ensure_index(ns, { key = 1 }, true))
end

-- set key to be an index
local function ensure_index(db,ns)
  assert(db:ensure_index(ns, { key = 1 }, false))
end

-- returns a coroutine.wrap which returns true until all tasks are finished
local function make_task_coroutine_wrap(self,ns)
  local db = self:connect()
  local N = db:count(ns)
  return coroutine.wrap(function()
                          repeat
                            local db = self:connect()
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
  local r = db:mapreduce(ns,
                         [[
function() {
  emit(this.worker + "." + this.tmpname,0);
}]],
                         [[
function(key,values) {
  return 0;
}]],
                         { })
  for i,v in pairs(r) do print(i,v) end
  if r.results then
    for i,v in pairs(r.results) do print(i,v) end
  end
  return db:remove(ns,
                   { ["$or"] = { { status = STATUS.BROKEN, },
                                 { status = STATUS.WAITING } } },
                   false)
end

-- removes all the tasks at the given collection
local function remove_all_tasks(db,ns)
  return db:remove(ns, {}, false)
end

-- SERVER METHODS
local server_methods = {}

-- performs the connection, allowing to retrive a lost connection, and returns a
-- dbclient object
function server_methods:connect()
  if not self.db or self.db:is_failed() then
    assert(self.configured, "Call configure method")
    assert(not self.finished, "The task has finished")
    self.db = util.connect(self.connection_string, self.auth_table)
  end
  return self.db
end

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
  local db = self:connect()
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
  local task_dbname = self.task_dbname
  ensure_unique_index(db,task_dbname)
  ensure_unique_index(db,self.map_dbname)
  assert(db:ensure_index(self.map_dbname, { status = 1 }, false))
  ensure_unique_index(db,self.red_dbname)
  assert(db:ensure_index(self.red_dbname, { status = 1 }, false))
  ensure_unique_index(db,self.red_result_dbname)
  util.task.create_collection(self, task_dbname, "WAIT")
end

-- insert the job in the mongo db and returns a coroutine ready to be executed
-- as an iterator
function server_methods:prepare_map()
  local db = self:connect()
  local map_dbname = self.map_dbname
  remove_pending_tasks(db, map_dbname)
  -- create map tasks in mongo database
  local f = self.taskfn.func
  for key,value in coroutine.wrap(f) do
    assert(tostring(key), "taskfn must return a string key")
    -- FIXME: check what happens when the insert is a duplicate of an existing
    -- key
    
    -- FIXME: check how to process task keys which are defined by a previously
    -- broken execution and didn't belong to the current task execution
    assert( db:insert(map_dbname, make_task(key,value)) )
  end
  util.task(self,self.task_dbname):set_type("MAP")
  -- this coroutine WAITS UNTIL ALL MAPS ARE DONE
  return make_task_coroutine_wrap(self, map_dbname)
end

-- insert the job in the mongo db and returns a coroutine
function server_methods:prepare_reduce()
  local db = self:connect()
  local dbname = self.dbname
  local map_dbname = self.map_dbname
  local map_result_dbname = self.map_result_dbname
  local red_dbname = self.red_dbname
  remove_pending_tasks(db, red_dbname)
  -- aggregation of map results
  local group_result = "group_result"
  local all_collections = db:get_collections(dbname)
  for i,name in ipairs(all_collections) do
    if name:match("%.map_results%.") then
      local r = db:mapreduce(name,
                   [[
function(){
  emit(this.key,this.value);
};]],
               [[
function(key,values){
  var result = [];
  if (typeof(values) == "Array") {
    for (list in values) {
      for (v in list.v) {
        result.push(v);
      }
    }
  }
  else result.push(values);
  return { v:result };
};]],
               {}, group_result)
      db:drop_collection(name)
    end
  end
  -- create reduce tasks in mongo database, from aggregated map results
  local r = assert( db:query(dbname .. "." .. group_result) )
  for pair in r:results() do
    -- FIXME: check what happens when the insert is a duplicate of an existing
    -- key
    
    -- FIXME: check how to process task keys which are defined by a previously
    -- broken execution and didn't belong to the current task execution
    local key,value = pair._id, pair.value
    if type(value) ~= "table" then value = { v = { { value } } } end
    assert( type(value)  == "table" and value.v )
    assert( db:insert(red_dbname, make_task(key,value.v[1])) )
  end
  db:drop_collection(dbname .. "." .. group_result)
  util.task(self,self.task_dbname):set_type("REDUCE")
  -- this coroutine WAITS UNTIL ALL REDUCES ARE DONE
  return make_task_coroutine_wrap(self, red_dbname)
end

function server_methods:drop_collections()
  local db = self:connect()
  db:drop_collection(self.map_dbname)
  db:drop_collection(self.map_result_dbname)
  db:drop_collection(self.red_dbname)
  db:drop_collection(self.red_result_dbname)
  db:drop_collection(self.task_dbname)
end

-- finalizer for the map-reduce process
function server_methods:finalize()
  local db = self:connect()
  util.task(self,self.task_dbname):set_type("FINISHED")
  local f = self.finalfn.func
  f(db,self.red_result_dbname)
  -- drop collections, except reduce result and task status
  db:drop_collection(self.map_dbname)
  db:drop_collection(self.map_result_dbname)
  db:drop_collection(self.red_dbname)
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
    util.sleep(util.DEFAULT_SLEEP)
    collectgarbage("collect")
  end
  io.stderr:write("# Preparing REDUCE\n")
  local do_reduce_step = self:prepare_reduce()
  collectgarbage("collect")
  io.stderr:write("# REDUCE execution\n")
  while do_reduce_step() do
    util.sleep(util.DEFAULT_SLEEP)
    collectgarbage("collect")
  end
  io.stderr:write("# FINAL execution\n")
  collectgarbage("collect")
  self:finalize()
end

-- SERVER METATABLE
local server_metatable = { __index = server_methods }

server.new = function(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = assert(dbname, "Needs a dbname as 2nd argument"),
                task_dbname = string.format("%s.task", dbname),
                map_dbname = string.format("%s.map_tasks", dbname),
                red_dbname = string.format("%s.red_tasks", dbname),
                map_result_dbname = string.format("%s.map_results", dbname),
                red_result_dbname = string.format("%s.red_results", dbname),
                auth_table = auth_table, }
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
  util.task.create_collection(s,s.task_dbname,"WAIT")
  -- TODO: check the task_status
  -----------------------------
  assert(s.connection_string == connection_string)
  assert(s.dbname == dbname)
  assert(s.map_dbname == dbname .. ".map_tasks")
  assert(s.map_result_dbname == dbname .. ".map_results")
  assert(s.red_dbname == dbname .. ".red_tasks")
  assert(s.red_result_dbname == dbname .. ".red_results")
  assert(s.task_dbname == dbname .. ".task")
  local db = assert(s:connect())
  assert(s.db)
  assert(db:find_one(s.task_dbname).job == "WAIT")
  -- clean previous failed tests
  db:drop_collection(s.map_dbname)
  db:drop_collection(s.map_result_dbname)
  db:drop_collection(s.red_dbname)
  -- check prepare_map
  s.taskfn = {
    func = function()
      for i=1,10 do coroutine.yield(i,{ file=i }) end
    end
  }
  local do_map_step = s:prepare_map()
  assert(db:find_one(s.task_dbname).job == "MAP")
  assert(do_map_step)
  assert(db:count(s.map_dbname, { status = STATUS.WAITING }) == 10)
  assert(do_map_step())
  assert(do_map_step())
  assert(db:update(s.map_dbname, {},
                   { ["$set"] = { status = STATUS.FINISHED } },
                   false, true))
  assert(db:count(s.map_dbname, { status = STATUS.FINISHED }) == 10)
  assert(not do_map_step())
  remove_all_tasks(db,s.map_dbname)
  -- check prepare_reduce
  db:insert(s.map_result_dbname, { key="one", values={ 4, 5, 1, 3 } })
  db:insert(s.map_result_dbname, { key="two", values={ 1, 2, 3 } })
  local do_red_step = s:prepare_reduce()
  assert(db:find_one(s.task_dbname).job == "REDUCE")
  assert(do_red_step)
  assert(db:count(s.red_dbname, { status = STATUS.WAITING }) == 2)
  assert(do_red_step())
  assert(do_red_step())
  assert(db:update(s.red_dbname, {},
                   { ["$set"] = { status = STATUS.FINISHED } },
                   false, true))
  assert(db:count(s.red_dbname, { status = STATUS.FINISHED }) == 2)
  assert(not do_red_step())
  remove_all_tasks(db,s.red_dbname)
  db:drop_collection(s.map_result_dbname)
  -- finalize
  db:insert(s.red_result_dbname, { key="one", value=13 })
  db:insert(s.red_result_dbname, { key="two", value=6 })
  s.finalfn = {
    func = function(db,red_result_dbname)
      local r = assert(db:query(red_result_dbname, {}))
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
  assert(db:find_one(s.task_dbname).job == "FINISHED")
  db:drop_collection(s.red_result_dbname)
  db:drop_collection(s.task_dbname)
end

------------------------------------------------------------------------------

return server
