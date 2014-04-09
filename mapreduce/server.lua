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
local set_task_id,get_task_id,inc_task_id
do
  local taskid=0
  set_task_id = function(v) taskid = v end
  get_task_id = function() return taskid end
  inc_task_id = function() taskid = taskid + 1 return taskid end
end

-- set key to be a unique index
local function ensure_unique_index(db,ns)
  assert(db:ensure_index(ns, { key = 1 }, true))
end

-- returns a coroutine.wrap which returns true until all tasks are finished
local function make_task_coroutine_wrap(self,ns)
  local db = self:connect()
  local N = db:count(ns)
  return coroutine.wrap(function()
                          repeat
                            local db = self:connect()
                            local M = db:count(ns, { status = STATUS.FINISHED })
                            if M < N then coroutine.yield(true) end
                          until M == N
                        end)
end

-- removes all the tasks which are in WAITING or BROKEN states
local function remove_pending_tasks(db,ns)
  return db:remove(ns,
                   { ["$or"] = { status = STATUS.BROKEN,
                                 status = STATUS.WAITING } },
                   false)
end

-- removes all the tasks at the given collection
local function remove_all_tasks(db,ns)
  return db:remove(ns, {}, false)
end

local function look_for_last_task_id(db,ns)
  local res = db:mapreduce(ns,
                      -- inline javascript
                      [[
function() {
  emit(0, this.taskid)
}
]],
                      -- inline javascript
                      [[
function(key,values) {
  var res = 0;
  for ( var i=0; i<values.length; i++ )
    if ( values[i] > res ) res = values[i];
  return res;
}
]])
  if util.check_mapreduce_result(res) then
    if res.code then assert(util.check_mapreduce_result(res)) end
    return res.results[1].value
  else
    return 0
  end
end
                             
-- SERVER METHODS
local server_methods = {}

-- performs the connection, allowing to retrive a lost connection, and returns a
-- dbclient object
function server_methods:connect()
  if not self.db or self.db:is_failed() then
    local db = assert( mongo.Connection.New{ auto_reconnect=true,
                                             rw_timeout=util.DEFAULT_RW_TIMEOUT} )
    assert( db:connect(self.connection_string) )
    if self.auth_table then db:auth(auth_table) end
    assert( not db:is_failed(), "Impossible to connect :S" )
    self.db = db
  end
  return self.db
end

-- configures the server with the script string
function server_methods:configure(loadable_string)
  local dbname = self.dbname
  local result = load(loadable_string)()
  for _,name in ipairs{ "taskfn", "mapfn", "reducefn", "finalfn" } do
    assert(result[name] and type(result[name]) == "function",
           string.format("Needs a %s function/coroutine", name))
  end
  local db = self:connect()
  -- look for last task id if available
  set_task_id(math.max(look_for_last_task_id(db,map_dbname),
                       look_for_last_task_id(db,red_dbname)))
  --
  self.taskfn = result.taskfn
  self.finalfn = result.finalfn
end

-- insert the work in the mongo db and returns a coroutine ready to be executed
-- as an iterator
function server_methods:prepare_map()
  local db = self:connect()
  local map_dbname = self.map_dbname
  remove_pending_tasks(db, map_dbname)
  ensure_unique_index(db,map_dbname)
  -- create map tasks in mongo database
  for key,value in coroutine.wrap(self.taskfn) do
    assert(tostring(key), "taskfn must return a string key")
    -- FIXME: check what happens when the insert is a duplicate of an existing
    -- key
    
    -- FIXME: check how to process task keys which are defined by a previously
    -- broken execution and didn't belong to the current task execution
    assert( db:insert(map_dbname, make_task(key,value,inc_task_id())) )
  end
  -- this coroutine WAITS UNTIL ALL MAPS ARE DONE
  return make_task_coroutine_wrap(self, map_dbname)
end

-- insert the work in the mongo db and returns a coroutine
function server_methods:prepare_reduce()
  local db = self:connect()
  local map_result_dbname = self.map_result_dbname
  local red_dbname = self.red_dbname
  -- create reduce tasks in mongo database, from map results
  local r = assert( db:query(map_result_dbname) )
  for pair in r:results() do
    assert( db:insert(red_dbname, make_task(pair.key,pair.value,inc_task_id())) )
  end
  -- this coroutine WAITS UNTIL ALL REDUCES ARE DONE
  return make_task_coroutine_wrap(self, red_dbname)
end

-- finalizer for the map-reduce process
function server_methods:finalize()
  local db = self:connect()
  self.finalfn(self.red_result_dbname)
end

-- makes all the map-reduce process, looping into the coroutines until all tasks
-- are done
function server_methods:loop()
  local do_map_step = self:prepare_map()
  while do_map_step() do util.sleep(util.DEFAULT_SLEEP) end
  local do_reduce_step = self:prepare_reduce()
  while do_reduce_step() do util.sleep(util.DEFAULT_SLEEP) end
  self:finalize()
end

-- SERVER METATABLE
local server_metatable = { __index = server_methods }

server.new = function(connection_string, dbname, auth_table)
  local obj = { connection_string = connection_string,
                dbname = assert(dbname, "Needs a dbname as 2nd argument"),
                map_dbname = string.format("%s.map_tasks", dbname),
                red_dbname = string.format("%s.red_tasks", dbname),
                map_result_dbname = string.format("%s.map_results", dbname),
                red_result_dbname = string.format("%s.red_results", dbname),
                auth_table = auth_table }
  setmetatable(obj, server_metatable)
  return obj
end

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
server.utest = function(connection_string, dbname, auth_table)
  local connection_string = connection_string or "localhost"
  local dbname = dbname or "tmp"
  -- check task id counter
  set_task_id(5)
  assert(get_task_id()==5)
  assert(inc_task_id()==6)
  assert(get_task_id()==6)
  -- check server connection
  local s = server.new(connection_string, dbname, auth_table)
  assert(s.connection_string == connection_string)
  assert(s.dbname == dbname)
  assert(s.map_dbname == dbname .. ".map_tasks")
  assert(s.map_result_dbname == dbname .. ".map_results")
  assert(s.red_dbname == dbname .. ".red_tasks")
  local db = assert(s:connect())
  assert(s.db)
  -- check mapreduce for task id
  db:drop_collection("tmp.tasks")
  assert(look_for_last_task_id(db,"tmp.tasks") == 0)
  db:insert("tmp.tasks",{ taskid = 20 })
  db:insert("tmp.tasks",{ taskid = 10 })
  db:insert("tmp.tasks",{ taskid = 50 })
  assert(look_for_last_task_id(db, "tmp.tasks") == 50)
  db:drop_collection("tmp.tasks")
  -- clean previous failed tests
  db:drop_collection(s.map_dbname)
  db:drop_collection(s.map_result_dbname)
  db:drop_collection(s.red_dbname)
  -- check prepare_map
  s.taskfn = function()
    for i=1,10 do coroutine.yield(i,{ file=i }) end
  end
  local do_map_step = s:prepare_map()
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
  db:insert(s.map_result_dbname, { key="one", value={ 4, 5, 1, 3 } })
  db:insert(s.map_result_dbname, { key="two", value={ 1, 2, 3 } })
  local do_red_step = s:prepare_reduce()
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
  s.finalfn = function(red_result_dbname)
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
  s:finalize()
  db:drop_collection(s.red_result_dbname)
end

------------------------------------------------------------------------------

return server
