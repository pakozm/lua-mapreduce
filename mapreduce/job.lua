local utils = require "mapreduce.utils"

local job = {
  _VERSION = "0.1",
  _NAME = "job",
}

local STATUS = utils.STATUS
local grp_tmp_dir = utils.GRP_TMP_DIR
local serialize_sorted_by_lines = utils.serialize_sorted_by_lines
local gridfs_lines_iterator = utils.gridfs_lines_iterator
local keys_sorted = utils.keys_sorted

-- PRIVATE FUNCTIONS AND METHODS

-- loads the required Lua module, sets the upvalue for the "emit" function,
-- executes init function if needed, and returns the resulting function
local funcs = { }
local function job_get_func(self, fname, args)
  local f = funcs[fname]
  if not f then
    f = { m = require(fname) }
    if f.m.init then f.m.init(args) end
    funcs[fname] = f
    local k,v
    repeat
      k,v = debug.getupvalue (f.m.func, 1)
    until not k or k == "_ENV"
    assert(k == "_ENV")
    -- emit function is inserted in the environment of the function
    f.upvalue = v
  end
  f.upvalue.emit = function(key, value)
    self.result = self.result or {}
    self.result[key] = self.result[key] or {}
    table.insert(self.result[key], value)
  end
  return f.m.func
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
                        finished_time = os.time(),
                      },
                    },
                    false,
                    false) )
end

function job_mark_as_written(self)
  assert(self.job_tbl)
  local db = self.cnn:connect()
  assert( db:update(self.jobs_ns,
                    {
                      _id = self:get_id(),
                    },
                    {
                      ["$set"] = {
                        status = STATUS.WRITTEN,
                        written_time = os.time(),
                      },
                    },
                    false,
                    false) )
end

function job_prepare_map(self, g,
                         combiner_fname, combiner_args,
                         paritioner)
  local map_key,map_value = self:get_pair()
  -- this closure is the responsible for all the map job
  return function()
    g(map_key,map_value) -- executes the MAP function, the result is
    -- self.result the job is marked as finished, but not written
    job_mark_as_finished(self)
    --
    local results_ns = self.results_ns
    -- combiner, apply the reduce function before put result to database
    local combiner =  job_get_func(self, combiner_fname,
                                   combiner_args)
    -- partition of the map result using partition function; additionally, the
    -- data is stored sorted by key; data is appended to GridFileBuilders
    local result     = self.result or {}
    local db         = self.cnn:connect()
    local gridfs     = self.cnn:gridfs()
    local keys       = keys_sorted(result)
    local builders = {}
    for _,key in ipairs(keys) do
      local value     = result[key]
      local value     = ( (#value > 1) and { combiner(value) } ) or value
      local part_key  = partitioner(key)
      local part_key  = assert(tonumber(part_key),
                               "Partition key must be a number")
      assert(math.floor(part_key) == part_key,
             "Partition key must be an integer")
      local result_ns = string.format("%s.P%d.M%s", self.results_ns,
                                      part_key, map_key)
      local builder   = builders[result_ns] or self.cnn:grid_file_builder()
      local key_str   = escape(key)
      local value_str = serialize_table_ipairs(result[key])
      builder:append(string.format("return %s,%s\n", key_str, value_str))
    end
    -- create all the GridFS files
    for result_ns,builder in pairs(builders) do
      local gridfs_filename = string.format("%s/%s",grp_tmp_dir,results_ns)
      gridfs:remove_file(gridfs_filename)
      builder:build(gridfs_filename)
    end
    -- job is marked as written to the database
    job_mark_as_written(self)
  end
end

function job_prepare_reduce(self, g)
  local key,value = self:get_pair()
  return function()
    -- in reduce jobs, the value is a reference with the basename of the gridfs
    -- filenames related to the given reduce job
    local part_key = key
    local job_file = value.file
    local res_file = value.result
    local gridfs   = obj.cnn:gridfs()
    local builder  = obj.cnn:grid_file_builder()
    gridfs:remove_file(res_file)
    -- take all the files which match the given job_file name
    local filenames = {}
    local match_str = string.format("^%s", job_file)
    local list = gridfs:list({ filename = match_str })
    for _,v in list:results() do
      -- sanity check
      assert(v.filename:match(match_str))
      table.insert(filenames, v.filename)
    end
    local counter = 0
    for k,v in merge_iterator(gridfs, filenames) do
      counter = counter + 1
      local v = g(k,v) -- executes the REDUCE function
      assert(v, "Reduce must return a value")
      builder:append(string.format("return %s,%s\n",
                                   utils.escape(k), utils.escape(v)))
      if counter % utils.MAX_IT_WO_CGARBAGE then
        collectgarbage("collect")
      end
    end
    builder:build(res_file)
    -- job is marked as as written directly
    job_mark_as_written(obj)
  end
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

-- constructor, receives a connection and a task instance
function job:__call(cnn, job_tbl, task_status, fname, args, jobs_ns, results_ns,
                    not_executable, combiner_fname, combiner_args)
  local obj = {
    cnn = cnn,
    job_tbl = job_tbl,
    jobs_ns = jobs_ns,
    results_ns = results_ns,
  }
  setmetatable(obj, { __index=self })
  --
  local fn,g
  if not not_executable then g = job_get_func(obj, fname, args) end
  if not not_executable then
    if task_status == "MAP" then
      fn = job_prepare_map(self, g, not_executable, combiner_fname, combiner_args)
    elseif task_status == "REDUCE" then
      fn = job_prepare_reduce(self, g)
    end
  else
    fn = function() error("Forbidden execution of jobs here") end
  end
  obj.fn = fn
  return obj
end
setmetatable(job,job)

return job
