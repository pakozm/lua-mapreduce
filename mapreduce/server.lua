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
local escape = utils.escape
local serialize_table_ipairs = utils.serialize_table_ipairs
local make_job = utils.make_job

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
                              io.stderr:write(string.format("\r%6.1f %% ",
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

-- removes all the tasks at the given collection
local function remove_collection_data(db,ns)
  return db:remove(ns, {}, false)
end

-- insert the job in the mongo db and returns a coroutine ready to be executed
-- as an iterator
local function server_prepare_map(self)
  local db = self.cnn:connect()
  local map_jobs_ns = self.task:get_map_jobs_ns()
  remove_pending_tasks(db, map_jobs_ns)
  -- create map tasks in mongo database
  local f = self.taskfn.func
  local count = 0
  for key,value in coroutine.wrap(f) do
    count = count + 1
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

-- iterates over all the lines of a given gridfs filename, and returns the first
-- and last chunks where the line is contained, and the first and last position
-- inside the corresponding chunks
local function gridfs_lines_iterator(gridfs, filename)
  local gridfile = gridfs:find_file(filename)
  local size          = #gridfile
  local abs_pos       = 0
  local current_chunk = 0
  local current_pos   = 1
  local num_chunks    = gridfile:num_chunks()
  local chunk,data
  return function()
    if current_chunk < num_chunks then
      chunk = chunk or gridfile:chunk(current_chunk)
      if current_pos < chunk:len() then
        local first_chunk = current_chunk
        local last_chunk  = current_chunk
        local first_chunk_pos = current_pos
        local last_chunk_pos = current_pos
        local tbl = {}
        local found_line = false
        repeat
          chunk = chunk or gridfile:chunk(current_chunk)
          data  = data  or chunk:data()
          local match = data:match("^([^\n]*)\n", current_pos)
          if match then
            table.insert(tbl, match)
            current_pos = #match + current_pos + 1 -- +1 because of the \n
            abs_pos     = #match + abs_pos + 1
            found_line = true
          else -- if match ... then
            -- inserts the whole chunk substring, no \n match found
            table.insert(tbl, data:sub(current_pos, chunk:len()))
            current_pos = chunk:len() + 1 -- forces to go next chunk
            abs_pos     = abs_pos + chunk:len() - current_pos + 1
          end -- if match ... then else ...
          last_chunk_pos = current_pos - 1
          last_chunk = current_chunk
          -- go to next chunk if we are at the end
          if current_pos > chunk:len() then
            current_chunk = current_chunk + 1
            current_pos   = 1
            chunk,data    = nil,nil
          end
          -- avoids to process empty lines
          if found_line and first_chunk==last_chunk and last_chunk_pos==first_chunk_pos then
            tbl             = {}
            found_line      = false
            first_chunk     = current_chunk
            last_chunk      = current_chunk
            first_chunk_pos = current_pos
            last_chunk_pos  = current_pos
          end
          --
        until found_line or current_chunk >= num_chunks
        return table.concat(tbl),first_chunk,last_chunk,first_chunk_pos,last_chunk_pos,abs_pos,size
      end -- if current_pos < chunk:len() ...
    end -- if current_chunk < gridfile:num_chunks() ...
  end -- return function()
end

local function get_key_from_line(line)
  local key,value = load(line)()
  return key
end

local function merge_gridfs_files(cnn, db, gridfs,
                                  filenames, result_filename,
                                  red_results_ns)
  local tmpname = os.tmpname()
  local f = io.open(tmpname, "w")
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
  -- look for all the data which has equal key
  local search_equals = function()
    local key
    local list = {}
    for i=1,#filenames do
      if data[i] then
        if not key or data[i][1] <= key then
          if not key or data[i][1] < key then list = {} end
          table.insert(list,i)
          key = data[i][1]
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
  -- merge all the files until finished
  local counter = 0
  while not finished() do
    counter = counter + 1
    --
    local equals_list = search_equals()
    if #equals_list == 1 then
      if #data[equals_list[1]][2] == 1 then
        -- take note in pending_red_results table when not reduce is necessary
        cnn:annotate_insert(red_results_ns,
                            { _id   = data[equals_list[1]][1],
                              value = data[equals_list[1]][2][1] })
      else
        -- insert in the gridfs file when reduce is necessary
        f:write(data[equals_list[1]][3])
        f:write("\n")
      end
      local pos = take_next(equals_list[1])
      if pos then current_pos[equals_list[1]] = pos end
    else
      local key_str = escape(data[equals_list[1]][1])
      local result = {}
      for _,which in ipairs(equals_list) do
        for _,v in ipairs(data[which][2]) do
          table.insert(result, v)
        end
        local pos = take_next(which)
        if pos then current_pos[which] = pos end
      end
      local value_str = serialize_table_ipairs(result)
      f:write(string.format("return %s,%s\n",key_str,value_str))
    end
    -- verbose output
    if counter % utils.MAX_IT_WO_CGARBAGE == 0 then
      local pos = 0
      for i=1,#filenames do pos = pos + current_pos[i] end
      pos = math.min(pos,total_size)
      io.stderr:write(string.format("\r\t%6.1f %% ",
                                    pos/total_size*100))
      io.stderr:flush()
      collectgarbage("collect")
    end
  end
  io.stderr:write(string.format("\r\t%6.1f %% \n", 100))
  io.stderr:flush()
  f:close()
  -- insert all the remaining pending results
  cnn:flush_pending_inserts(0)
  -- store the file in gridfs and remove temporal local file
  gridfs:remove_file(result_filename)
  gridfs:store_file(tmpname, result_filename)
  os.remove(tmpname)
  -- remove all map result gridfs files
  for _,name in ipairs(filenames) do
    gridfs:remove_file(name)
  end  
end

-- insert the job in the mongo db and returns a coroutine
local function server_prepare_reduce(self)
  local db     = self.cnn:connect()
  local gridfs = self.cnn:gridfs()
  local dbname = self.cnn:get_dbname()
  local red_jobs_ns = self.task:get_red_jobs_ns()
  remove_pending_tasks(db, red_jobs_ns)
  -- group all map results
  local group_result = "group_result"
  local filenames = {}
  for obj in gridfs:list():results() do
    local filename = obj.filename
    if filename:match(string.format("^%s",grp_tmp_dir)) then
      table.insert(filenames, filename)
    end
  end
  -- if #filenames == 0 all data has been processed, avoid merge
  if #filenames > 0 then
    io.stderr:write("# \t MERGE\n")
    merge_gridfs_files(self.cnn, db, gridfs,
                       filenames, group_result,
                       self.task:get_red_results_ns())
    collectgarbage("collect")
  end
  io.stderr:write("# \t CREATING JOBS\n")
  -- create reduce jobs in mongo database, from aggregated map results. reduce
  -- jobs are described as a position in a gridfs file
  local max_chunk_value = 0
  local counter = 0
  for line, first_chunk, last_chunk, first_chunk_pos, last_chunk_pos, pos, size in
  gridfs_lines_iterator(gridfs, group_result) do
    counter = counter + 1
    local key   = get_key_from_line(line)
    local value = {
      file = group_result,
      first_chunk = first_chunk,
      last_chunk = last_chunk,
      first_chunk_pos = first_chunk_pos,
      last_chunk_pos = last_chunk_pos,
    }
    self.cnn:annotate_insert(red_jobs_ns, make_job(key,value))
    if counter % utils.MAX_IT_WO_CGARBAGE == 0 then
      collectgarbage("collect")
      io.stderr:write(string.format("\r\t%6.1f %% ",
                                    pos/size*100))
      io.stderr:flush()
    end
    max_chunk_value= math.max(max_chunk_value, last_chunk)
  end
  io.stderr:write(string.format("\r\t%6.1f %% \n", 100))
  io.stderr:flush()
  self.cnn:flush_pending_inserts(0)
  io.stderr:write("# \t STARTING REDUCE\n")
  self.task:set_task_status(TASK_STATUS.REDUCE,
                            { last_chunk = max_chunk_value })
  -- this coroutine WAITS UNTIL ALL REDUCES ARE DONE
  return make_task_coroutine_wrap(self, red_jobs_ns)
end

local function server_drop_collections(self)
  local db = self:connect()
  local dbname = db:get_dbname()
  -- drop all the collections
  for _,name in ipairs(db:get_collections(dbname)) do
    db:drop_collection(name)
  end
end

-- finalizer for the map-reduce process
local function server_final(self)
  local db = self.cnn:connect()
  local task = self.task
  task:set_task_status(TASK_STATUS.FINISHED)
  local results_ns = task:get_red_results_ns()
  local q = db:query(results_ns, {})
  self.finalfn.func(q, db, results_ns)
  -- drop collections, except reduce result and task status
  db:drop_collection(task:get_map_jobs_ns())
  db:drop_collection(task:get_red_jobs_ns())
  db:drop_collection(task:get_red_results_ns())
  local gridfs = self.cnn:gridfs()
  for v in gridfs:list():results() do gridfs:remove_file(v.filename) end
  self.finished = true
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

-- makes all the map-reduce process, looping into the coroutines until all tasks
-- are done
function server_methods:loop()
  io.stderr:write("# Preparing MAP\n")
  local do_map_step = server_prepare_map(self)
  collectgarbage("collect")
  io.stderr:write("# MAP execution\n")
  while do_map_step() do
    utils.sleep(utils.DEFAULT_SLEEP)
    collectgarbage("collect")
  end
  collectgarbage("collect")
  io.stderr:write("# Preparing REDUCE\n")
  local do_reduce_step = server_prepare_reduce(self)
  collectgarbage("collect")
  io.stderr:write("# REDUCE execution\n")
  while do_reduce_step() do
    utils.sleep(utils.DEFAULT_SLEEP)
    collectgarbage("collect")
  end
  io.stderr:write("# FINAL execution\n")
  collectgarbage("collect")
  server_final(self)
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
