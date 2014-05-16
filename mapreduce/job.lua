local job = {
  _VERSION = "0.2",
  _NAME = "job",
}

local utils = require "mapreduce.utils"
local fs = require "mapreduce.fs"

local STATUS = utils.STATUS
local TASK_STATUS = utils.TASK_STATUS
local serialize_table_ipairs = utils.serialize_table_ipairs
local merge_iterator = utils.merge_iterator
local keys_sorted = utils.keys_sorted
local escape = utils.escape

--
local cache = {}
local function cached(func)
  cache[func] = cache[func] or {}
  local local_cache = cache[func]
  return function(key)
    if not local_cache[key] then
      local result = func(key)
      local_cache[key] = result
      return result
    else
      return local_cache[key]
    end
  end
end
local function reset_cache()
  cache = {}
end

-- PRIVATE FUNCTIONS AND METHODS

-- loads the required Lua module, sets the "emit" function, executes init
-- function if needed, and returns the resulting function
local initialized = {}
local funcs = { }
local function job_get_func(self, fname, func, args)
  local f = funcs[fname]
  if not f then
    f = { m = require(fname) }
    if f.m.init and not initialized[f.m.init] then
      f.m.init(args)
      initialized[f.m.init] = true
    end
    assert(f.m[func]) -- sanity check
    funcs[fname] = f
  end
  if func == "mapfn" then
    local MAX_MAP_RESULT = utils.MAX_MAP_RESULT
    local result = {}
    local copy_table_ipairs = utils.copy_table_ipairs
    local clear_table = utils.clear_table
    self.result  = result
    self.emit    = function(key,value)
      assert(tostring(key),
             "emit function must receive a convertible to string key")
      local result = result
      result[key]  = result[key] or {}
      local N      = #result[key]
      -- faster than table.insert
      result[key][ N+1 ] = value
      if self.combiner and N > MAX_MAP_RESULT then 
        self.combiner(key, result[key],self.combiner_emit)
        copy_table_ipairs(result[key], self.combiner_result)
        clear_table(self.combiner_result)
      end
    end
  elseif func == "reducefn" then
    self.result  = {}
    self.emit    = function(value)
      -- faster than table.insert
      self.result[ #self.result+1 ] = value
    end
    self.associative_reducer = f.m.associative_reducer
    self.commutative_reducer = f.m.commutative_reducer
    self.idempotent_reducer  = f.m.idempotent_reducer
  elseif func == "combinerfn" then
    self.combiner_result = {}
    self.combiner_emit = function(value)
      -- faster than table.insert
      self.combiner_result[ #self.combiner_result+1 ] = value
    end
  end
  return f.m[func]
end

local function job_mark_as_finished(self)
  assert(self.job_tbl)
  local db = self.cnn:connect()
  assert( db:update(self.jobs_ns,
                    {
                      _id = self:get_id(),
                    },
                    {
                      ["$set"] = {
                        status = STATUS.FINISHED,
                        finished_time = utils.time(),
                      },
                    },
                    false,
                    false) )
end

function job_mark_as_written(self,cpu_time)
  self.written = true
  assert(self.job_tbl)
  local db = self.cnn:connect()
  assert( db:update(self.jobs_ns,
                    {
                      _id = self:get_id(),
                    },
                    {
                      ["$set"] = {
                        status = STATUS.WRITTEN,
                        written_time = utils.time(),
                        cpu_time = cpu_time,
                        real_time = utils.time() - self.t,
                      },
                    },
                    false,
                    false) )
end

function job_prepare_map(self, g, combiner_fname, partitioner_fname, init_args,
                         storage, path)
  local partitioner = cached( job_get_func(self, partitioner_fname,
                                           "partitionfn", init_args) )
  -- combiner, apply the reduce function before put result to database
  local combiner
  if combiner_fname then
    combiner = job_get_func(self, combiner_fname, "combinerfn", init_args)
  end
  self.combiner = combiner
  local map_key,map_value = self:get_pair()
  -- this closure is the responsible for all the map job
  return function()
    -- take local variables
    local assert        = assert
    local tonumber      = tonumber
    local combiner      = combiner
    local partitioner   = partitioner
    local string_format = string.format
    local escape        = escape
    local math_floor    = math.floor
    local serialize_table_ipairs = serialize_table_ipairs
    local clock1 = os.clock()
    local combiner_emit = self.combiner_emit
    local copy_table_ipairs = utils.copy_table_ipairs
    local clear_table = utils.clear_table
    --------------------------------------------------------------------------
    --------------------------------------------------------------------------
    g(map_key,map_value,self.emit) -- executes the MAP function, the result is
    --------------------------------------------------------------------------
    --------------------------------------------------------------------------
    -- self.result the job is marked as finished, but not written
    job_mark_as_finished(self)
    --
    local results_ns = self.results_ns
    -- partition of the map result using partition function; additionally, the
    -- data is stored sorted by key; data is appended to builder
    local result     = self.result or {}
    local fs,make_builder,make_lines_iterator = fs.router(self.cnn,nil,
                                                          storage,path)
    local keys     = keys_sorted(result)
    local builders = {}
    for _,key in ipairs(keys) do
      local values = result[key]
      if #values > 1 then
        combiner(key,values,combiner_emit)
        copy_table_ipairs(values, self.combiner_result)
        clear_table(self.combiner_result)
      end
      local part_key  = partitioner(key)
      local part_key  = assert(tonumber(part_key),
                               "Partition key must be a number")
      assert(math_floor(part_key) == part_key,
             "Partition key must be an integer")
      local result_ns = string_format("%s.P%d.M%s", results_ns,
                                      part_key, map_key)
      local builder       = builders[result_ns] or make_builder()
      builders[result_ns] = builder
      local key_str       = escape(key)
      local values_str    = serialize_table_ipairs(values)
      builder:append(string.format("return %s,%s\n", key_str, values_str))
    end
    -- create all the files
    for result_ns,builder in pairs(builders) do
      local fs_filename = string.format("%s/%s",path,result_ns)
      fs:remove_file(fs_filename)
      assert( builder:build(fs_filename) )
    end
    local clock2 = os.clock()
    local elapsed_time = clock2 - clock1
    -- job is marked as written to the database
    job_mark_as_written(self, elapsed_time)
    return elapsed_time
  end
end

function job_prepare_reduce(self, g, storage, path)
  local key,value = self:get_pair()
  return function()
    -- take local variables
    local copy_table_ipairs = utils.copy_table_ipairs
    local clear_table   = utils.clear_table
    local associative   = self.associative_reducer
    local commutative   = self.commutative_reducer
    local idempotent    = self.idempotent_reducer
    local assert        = assert
    local string_format = string.format
    local escape        = escape
    local clock1 = os.clock()
    -- in reduce jobs, the value is a reference with the basename of the fs
    -- filenames related to the given reduce job
    local part_key = key
    local job_file = value.file
    local res_file = value.result
    local mappers  = value.mappers
    local gridfs   = self.cnn:gridfs()
    local builder  = self.cnn:grid_file_builder()
    gridfs:remove_file(res_file)
    -- take all the files which match the given job_file name
    local fs,make_builder,make_lines_iterator = fs.router(self.cnn,mappers,
                                                          storage,path)
    local filenames = {}
    local match_str = string.format("^%s.*", job_file)
    local list = fs:list({ filename = { ["$regex"] = match_str } })
    for v in list:results() do
      -- sanity check
      assert(v.filename:match(match_str))
      table.insert(filenames, v.filename)
    end
    --------------------------------------------------------------------------
    --------------------------------------------------------------------------
    -- iterate over a merge of all the input filenames
    if associative and commutative and idempotent then
      for k,v in merge_iterator(fs, filenames, make_lines_iterator) do
        if #v > 1 then
          g(k,v,self.emit) -- executes the REDUCE function
          copy_table_ipairs(v, self.result)
          clear_table(self.result)
        end
        -- write the result to mongo
        builder:append(string_format("return %s,%s\n", escape(k),
                                     serialize_table_ipairs(v)))
      end
    else
      for k,v in merge_iterator(fs, filenames, make_lines_iterator) do
        g(k,v,self.emit) -- executes the REDUCE function
        copy_table_ipairs(v, self.result)
        clear_table(self.result)
        -- write the result to mongo
        builder:append(string_format("return %s,%s\n", escape(k),
                                     serialize_table_ipairs(v)))
      end
    end
    --------------------------------------------------------------------------
    --------------------------------------------------------------------------
    assert( builder:build(res_file) )
    local clock2 = os.clock()
    local elapsed_time = clock2-clock1
    -- job is marked as as written directly
    job_mark_as_written(self, elapsed_time)
    -- remove all map result fs files
    for _,name in ipairs(filenames) do fs:remove_file(name) end  
    return elapsed_time
  end
end

-- PUBLIC METHODS

function job:execute()
  return self.fn()
end

function job:get_id()
  assert(self.job_tbl)
  return self.job_tbl._id
end

function job:get_pair()
  assert(self.job_tbl)
  return self.job_tbl._id,self.job_tbl.value
end

function job:status_string()
  return self:get_id()
end

function job:get_results_ns()
  return self.results_ns
end

function job:mark_as_broken()
  if not self.written then
    assert(self.job_tbl)
    local db = self.cnn:connect()
    assert( db:update(self.jobs_ns,
                      {
                        _id = self:get_id(),
                      },
                      {
                        ["$set"] = {
                          status = STATUS.BROKEN,
                          broken_time = utils.time(),
                        },
                      },
                      false,
                      false) )
  end
end

-- constructor, receives a connection and a task instance
function job:__call(cnn, job_tbl, task_status,
                    fname, init_args,
                    jobs_ns, results_ns,
                    not_executable,
                    combiner, partitioner,
                    storage, path)
  local obj = {
    cnn = cnn,
    job_tbl = job_tbl,
    jobs_ns = jobs_ns,
    results_ns = results_ns,
    t = utils.time(),
  }
  setmetatable(obj, { __index=self })
  --
  local fn,g,func
  if task_status == TASK_STATUS.MAP then
    func = "mapfn"
  elseif task_status == TASK_STATUS.REDUCE then
    func = "reducefn"
  else
    error("Incorrect task_status: " .. tostring(task_status) )
  end
  if not not_executable then
    g = job_get_func(obj, fname, func, args, task_status == TASK_STATUS.MAP)
  end
  if not not_executable then
    if task_status == TASK_STATUS.MAP then
      fn = job_prepare_map(obj, g, combiner, partitioner, init_args,
                           storage, path)
    elseif task_status == TASK_STATUS.REDUCE then
      fn = job_prepare_reduce(obj, g, storage, path)
    end
  end
  obj.fn = fn or function() error("Forbidden execution of jobs here") end
  return obj
end
setmetatable(job,job)

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------
job.utest = function()
  local f = cached(function(i) return i end)
  for j=1,2 do
    for i=1,10 do
      assert(f(i) == i)
    end
  end
end

------------------------------------------------------------------------

job.reset_cache = reset_cache

return job
