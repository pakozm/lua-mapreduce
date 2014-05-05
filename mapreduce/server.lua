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
local grp_tmp_dir = utils.GRP_TMP_DIR
local red_job_tmp_dir = utils.RED_JOB_TMP_DIR
local escape = utils.escape
local serialize_table_ipairs = utils.serialize_table_ipairs
local make_job = utils.make_job
local gridfs_lines_iterator = utils.gridfs_lines_iterator

-- PRIVATE FUNCTIONS AND METHODS

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
  local db = self.cnn:connect()
  local map_jobs_ns = self.task:get_map_jobs_ns()
  remove_pending_tasks(db, map_jobs_ns)
  -- create map tasks in mongo database
  local f = self.taskfn.func
  local count = 0
  local keys_check = {}
  for key,value in coroutine.wrap(f) do
    count = count + 1
    assert(tostring(key), "taskfn must return a string key")
    assert(not keys_check[key], string.format("Duplicate key: %s", key))
    keys_check[key] = true
    -- FIXME: check how to process task keys which are defined by a previously
    -- broken execution and didn't belong to the current task execution
    assert( db:insert(map_jobs_ns, make_job(key,value)) )
  end
  self.task:set_task_status(TASK_STATUS.MAP)
  -- this coroutine WAITS UNTIL ALL MAPS ARE DONE
  return make_task_coroutine_wrap(self, map_jobs_ns)
end

local function get_key_from_line(line)
  local key,value = load(line)()
  return key
end

-- this function merge all the map results, and applies the partitioning
-- function in order to balance the reduce execution
local function merge_gridfs_files(cnn, db, gridfs,
                                  filenames, part_func,
                                  result_ns)
  -- initializes all the line iterators (one for each file)
  local line_iterators = {}
  for _,name in ipairs(filenames) do
    table.insert(line_iterators, gridfs_lines_iterator(gridfs,name))
  end
  local finished = false
  local data = {}
  -- take the next data of a given file number
  local take_next = function(which)
    if line_iterators[which] then
      local line,_,_,_,_,pos,size = line_iterators[which]()
      if line then
        data[which] = data[which] or {}
        data[which][3],data[which][1],data[which][2] = line,load(line)()
        return pos,size
      else
        data[which] = nil
        line_iterators[which] = nil
      end
    else
      data[which] = nil
    end
  end
  -- we finished when all the data is nil
  local finished = function()
    local ret = true
    for i=1,#filenames do
      if data[i] ~= nil then ret = false break end
    end
    return ret
  end
  -- look for all the data which has the min key
  local search_min = function()
    local key
    local list = {}
    for i=1,#filenames do
      if data[i] then
        local current = data[i][1]
        if not key or current <= key then
          if not key or current < key then list = {} end
          table.insert(list,i)
          key = current
        end
      end
    end
    return list
  end
  -- initialize data with first line over all files, and count chunks for
  -- verbose output
  local current_pos = {}
  local total_size = 0
  for i=1,#filenames do
    local pos,size = take_next(i)
    if not pos then pos,size = 0,0 end
    current_pos[i] = pos
    total_size = total_size + size
  end
  --
  assert(not result_ns:match("^/tmp/"))
  --
  local red_job_files = {}
  local red_result_files = {}
  -- merge all the files until finished
  local counter = 0
  while not finished() do
    counter = counter + 1
    --
    local mins_list = search_min()
    assert(#mins_list > 0)
    local key = data[mins_list[1]][1]
    local part_key = assert(tonumber(part_func(key)),
                            "Partition key must be a number")
    assert(math.floor(part_key) == part_key,
           "Partition key must be an integer")
    if #mins_list == 1 then
      if #data[mins_list[1]][2] == 1 then
        -- put data in results file when not reduce is necessary
        red_result_files[part_key] = red_result_files[part_key] or
          cnn:grid_file_builder()
        red_result_files[part_key]:append(string.format("return %s,%s\n",
                                                        escape(data[mins_list[1]][1]),
                                                        escape(data[mins_list[1]][2][1])))
      else
        -- put data in job file when reduce is necessary
        red_job_files[part_key] = red_job_files[part_key] or
          cnn:grid_file_builder()
        red_job_files[part_key]:append(data[mins_list[1]][3])
        red_job_files[part_key]:append("\n")
      end
      local pos = take_next(mins_list[1])
      if pos then current_pos[mins_list[1]] = pos end
    else
      local key_str = escape(data[mins_list[1]][1])
      local result = {}
      for _,which in ipairs(mins_list) do
        for _,v in ipairs(data[which][2]) do
          table.insert(result, v)
        end
        local pos = take_next(which)
        if pos then current_pos[which] = pos end
      end
      local value_str = serialize_table_ipairs(result)
      red_job_files[part_key] = red_job_files[part_key] or
        cnn:grid_file_builder()
      red_job_files[part_key]:append(string.format("return %s,%s\n",
                                                   key_str,value_str))
    end
    -- verbose output
    if counter % utils.MAX_IT_WO_CGARBAGE == 0 then
      local pos = 0
      for i=1,#filenames do pos = pos + current_pos[i] end
      pos = math.min(pos,total_size)
      io.stderr:write(string.format("\r\t\t %6.1f %% ",
                                    pos/total_size*100))
      io.stderr:flush()
      collectgarbage("collect")
    end
  end
  io.stderr:write(string.format("\r\t\t %6.1f %% \n", 100))
  io.stderr:flush()
  -- close all the files and upload them to mongo gridfs
  for part_key,f in pairs(red_job_files) do
    local gridfs_name = string.format("%s.K%d",red_job_tmp_dir,part_key)
    gridfs:remove_file(gridfs_name)
    f:build(gridfs_name)
    -- remove crashed results
    local gridfs_name = string.format("%s.K%d",result_ns,part_key)
    gridfs:remove_file(gridfs_name)
  end
  for part_key,f in pairs(red_result_files) do
    local gridfs_name = string.format("%s.K%d",result_ns,part_key)
    gridfs:remove_file(gridfs_name)
    f:build(gridfs_name)
  end
  -- remove all map result gridfs files
  -- for _,name in ipairs(filenames) do gridfs:remove_file(name) end  
end

-- insert the job in the mongo db and returns a coroutine
local function server_prepare_reduce(self)
  local db     = self.cnn:connect()
  local gridfs = self.cnn:gridfs()
  local dbname = self.cnn:get_dbname()
  local red_jobs_ns = self.task:get_red_jobs_ns()
  remove_pending_tasks(db, red_jobs_ns)
  -- take all filenames which match with the grp_tmp_dir in gridfs (all map
  -- results are there)
  local filenames = {}
  local list = gridfs:list()
  for obj in list:results() do
    local filename = obj.filename
    if filename:match(string.format("^%s",grp_tmp_dir)) then
      table.insert(filenames, filename)
    end
  end
  -- if #filenames == 0 all data has been processed, avoid merge
  if #filenames > 0 then
    -- FIXME: check that #filenames == number of map jobs
    
    -- group map results depending in the partition function
    io.stderr:write("# \t\t Merge and partitioning\n")
    merge_gridfs_files(self.cnn, db, gridfs,
                       filenames, self.partitionfn.func,
                       self.result_ns)
    collectgarbage("collect")
  end
  io.stderr:write("# \t\t Creating jobs\n")
  -- create reduce jobs in mongo database, from partitioned space
  local keys_check = {}
  local list = gridfs:list()
  for v in list:results() do
    if v.filename:match(string.format("^%s",red_job_tmp_dir)) then
      local part_key = assert(tonumber(v.filename:match("^.*.K(%d+)$")))
      assert(not keys_check[part_key],
             string.format("Duplicate key: %s, file: %s",
                           part_key, v.filename))
      keys_check[part_key] = true
      local value = {
        file   = v.filename,
        result = string.format("%s.K%d",self.result_ns,part_key),
      }
      self.cnn:annotate_insert(red_jobs_ns, make_job(part_key, value))
    end
  end
  self.cnn:flush_pending_inserts(0)
  self.task:set_task_status(TASK_STATUS.REDUCE,
                            { last_chunk = max_chunk_value })
  -- this coroutine WAITS UNTIL ALL REDUCES ARE DONE
  return make_task_coroutine_wrap(self, red_jobs_ns)
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
  local files = gridfs:list()
  local current_file
  local lines_iterator
  -- iterator which is given to final function, allows to traverse all the
  -- results by pairs key,value
  local pair_iterator = function()
    local line
    repeat
      if lines_iterator then
        line = lines_iterator()
      end
      if not line then
        current_file = files:next()
        if current_file and current_file.filename:match(match_str) then
          lines_iterator = gridfs_lines_iterator(gridfs,current_file.filename)
        end
      end
    until current_file == nil or line ~= nil
    if line then
      return load(line)()
    end
  end
  -- the reply could be: false/nil, true, "loop"
  local reply = self.finalfn.func(pair_iterator)
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
  self.configured = true
  self.configuration_params = params
  self.task_args = params.task_args
  self.map_args = params.map_args
  self.partition_args = params.partition_args
  self.reduce_args = params.reduce_args
  self.final_args = params.final_args
  local dbname = self.dbname
  local taskfn,mapfn,partitionfn,reducefn,finalfn
  local scripts = {}
  self.result_ns = params.result_ns or "result"
  assert(params.taskfn and params.mapfn and params.partitionfn and params.reducefn,
         "Fields taskfn, mapfn, partitionfn and reducefn are mandatory")
  for _,name in ipairs{ "taskfn", "mapfn", "partitionfn", "reducefn", "finalfn" } do
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
  self.partitionfn = require(scripts.partitionfn)
  self.taskfn = require(scripts.taskfn)
  if scripts.finalfn then
    self.finalfn = require(scripts.finalfn)
  else
    self.finalfn = { func = function() end }
  end
  if self.partitionfn.init then self.partitionfn.init(self.partition_args) end
  if self.taskfn.init then self.taskfn.init(self.task_args) end
  if self.finalfn.init then self.finalfn.init(self.final_args) end
  self.mapfn = params.mapfn
  self.reducefn = params.reducefn
end

-- makes all the map-reduce process, looping into the coroutines until all tasks
-- are done
function server_methods:loop()
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
                                  self.configuration_params,
                                  it)
    end
    io.stderr:write(string.format("# Iteration %d\n", it))
    local time = os.time()
    self.task:insert_started_time(time)
    if not skip_map then
      -- MAP EXECUTION
      io.stderr:write("# \t Preparing MAP\n")
      local do_map_step = server_prepare_map(self)
      collectgarbage("collect")
      io.stderr:write("# \t MAP execution\n")
      while do_map_step() do
        utils.sleep(utils.DEFAULT_SLEEP)
        collectgarbage("collect")
      end
    end
    -- REDUCE EXECUTION
    collectgarbage("collect")
    io.stderr:write("# \t Preparing REDUCE\n")
    local do_reduce_step = server_prepare_reduce(self)
    collectgarbage("collect")
    io.stderr:write("# \t REDUCE execution\n")
    while do_reduce_step() do
      utils.sleep(utils.DEFAULT_SLEEP)
      collectgarbage("collect")
    end
    -- FINAL EXECUTION
    io.stderr:write("# \t FINAL execution\n")
    collectgarbage("collect")
    server_final(self)
    local end_time = os.time()
    local total_time = end_time - time
    self.task:insert_finished_time(end_time)
    --
    io.stderr:write("# " .. tostring(total_time) .. " seconds\n")
  until self.finished
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
  -- check serialization of map results
  local f = {
    write = function(self,str)
      self.tbl = self.tbl or {}
      table.insert(self.tbl,str)
    end,
    concat = function(self) return table.concat(self.tbl or {}) end,
  }
  -- FIXME: that is totally broken now
  utils.serialize_sorted_by_lines(f,{
                                    KEY1 = {1,1,1,1,1},
                                    KEY2 = {1,1,1},
                                    KEY3 = {1},
                                    KEY4 = { "hello\nworld" }
                                    })
  local result = [[return "KEY1",{1,1,1,1,1}
return "KEY2",{1,1,1}
return "KEY3",{1}
return "KEY4",{"hello\nworld"}
]]
  assert(f:concat() == result)
  -- check lines iterator over gridfs
  local cnn = cnn("localhost", "tmp")
  local db = cnn:connect()
  local gridfs = cnn:gridfs()
  local tmpname = os.tmpname()
  local f = io.open(tmpname, "w")
  f:write("first line\n")
  f:write("second line\n")
  f:write("third line\n")
  -- a large line, through multiple chunks
  for i=1,2^22 do
    f:write(string.format("a%d",i))
  end
  f:write("\n")
  f:close()
  gridfs:remove_file(tmpname)
  gridfs:store_file(tmpname,tmpname)
  local f = io.open(tmpname)
  for g_line in gridfs_lines_iterator(gridfs,tmpname) do
    local f_line = f:read("*l")
    assert(g_line == f_line)
  end
  os.remove(tmpname)
  -- check merge over several filenames
  local N=3
  local list_tmpnames = {} for i=1,N do list_tmpnames[i] = os.tmpname() end
  local list_files = {}
  for i,name in ipairs(list_tmpnames) do list_files[i]=io.open(name,"w") end
  -- FILE 1
  list_files[1]:write('return "a",{1,1,1}\n')
  list_files[1]:write('return "b",{1}\n')
  -- FILE 2
  list_files[2]:write('return "a",{1}\n')
  list_files[2]:write('return "c",{1,1,1,1,1,1,1,1,1}\n')
  -- FILE 3
  list_files[3]:write('return "a",{1,1}\n')
  list_files[3]:write('return "c",{1}\n')
  list_files[3]:write('return "d",{1,1,1,1,1,1}\n')
  --
  for i,f in ipairs(list_files) do
    f:close()
    gridfs:remove_file(list_tmpnames[i])
    gridfs:store_file(list_tmpnames[i], list_tmpnames[i])
    os.remove(list_tmpnames[i])
  end
  merge_gridfs_files(cnn, db, gridfs, list_tmpnames, 'result', 'tmp.result2')
  local lines = {
    'return "a",{1,1,1,1,1,1}',
    'return "c",{1,1,1,1,1,1,1,1,1,1}',
    'return "d",{1,1,1,1,1,1}',
  }
  for line in gridfs_lines_iterator(gridfs, "result") do
    assert(line == table.remove(lines,1))
  end
  for v in db:query('tmp.result2'):results() do
    assert(v._id == "b")
    assert(v.value == 1)
  end
end

------------------------------------------------------------------------------

return server
