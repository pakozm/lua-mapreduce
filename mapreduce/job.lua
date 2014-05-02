local utils = require "mapreduce.utils"

local job = {
  _VERSION = "0.1",
  _NAME = "job",
}

local STATUS = utils.STATUS
local grp_tmp_dir = utils.GRP_TMP_DIR
local serialize_sorted_by_lines = utils.serialize_sorted_by_lines

-- PRIVATE FUNCTIONS AND METHODS

local function take_value_from_gridfs(gridfs, chunk_value)
  local file = chunk_value.file
  local first_chunk = chunk_value.first_chunk
  local last_chunk = chunk_value.last_chunk
  local first_chunk_pos = chunk_value.first_chunk_pos
  local last_chunk_pos = chunk_value.last_chunk_pos
  local gridfile = assert( gridfs:find_file(file) )
  local tbl = {}
  assert(first_chunk >= 0)
  assert(first_chunk <= last_chunk)
  assert(last_chunk  <  gridfile:num_chunks())
  for i=first_chunk,last_chunk do
    local chunk = assert( gridfile:chunk(i) )
    local data = chunk:data()
    local from,to = 1,#data
    if i == first_chunk then
      assert(first_chunk_pos > 0 and first_chunk_pos <= #data)
      from = first_chunk_pos
    end
    if i == last_chunk then
      assert(last_chunk_pos > 0 and last_chunk_pos <= #data)
      to = last_chunk_pos
    end
    table.insert(tbl, data:sub(from,to))
  end
  local str = table.concat(tbl)
  local key,value = assert(load(str)())
  return value
end


-- the jobs are processed in batches, when a batch is ready, this function
-- inserts all the batch results in the database
local function job_process_pending_inserts(self)
  local db = self.cnn:connect()
  assert( db:insert_batch(self.results_ns, self.pending_inserts) )
  self.pending_inserts = {}
  collectgarbage("collect")
end

local function job_insert_result(self,key,value)
  local t = os.time()
  table.insert(self.pending_inserts, { key=key, value=value })
  if #self.pending_inserts > utils.MAX_PENDING_INSERTS then
    job_process_pending_inserts(self)
  end
end

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
  self.result = {}
  f.upvalue.emit = function(key, value)
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
                        time = os.time(),
                      },
                    },
                    false,
                    false) )
end

function job_mark_as_grouped(self)
  assert(self.job_tbl)
  local db = self.cnn:connect()
  assert( db:update(self.jobs_ns,
                    {
                      _id = self:get_id(),
                    },
                    {
                      ["$set"] = {
                        status = STATUS.GROUPED,
                        time = os.time(),
                      },
                    },
                    false,
                    false) )
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
                    not_executable)
  local obj = {
    cnn = cnn,
    job_tbl = job_tbl,
    jobs_ns = jobs_ns,
    results_ns = results_ns,
    pending_inserts = {},
  }
  setmetatable(obj, { __index=self })
  --
  local fn,g
  if not not_executable then g = job_get_func(obj, fname, args) end
  local key,value = obj:get_pair()
  if task_status == "MAP" then
    obj.results_ns = obj.results_ns .. ".K" .. key
    if not not_executable then
      fn = function()
        g(key,value) -- executes the MAP function, the result is self.result
        -- the job is not marked as finished, but yes as grouped
        -- job_mark_as_finished(obj)
        --
        -- aggregates all the map job in a gridfs file
        local results_ns = obj.results_ns
        local result     = obj.result
        local db         = obj.cnn:connect()
        local gridfs     = obj.cnn:gridfs()
        local tmpname    = os.tmpname()
        local f = io.open(tmpname,"w")
        serialize_sorted_by_lines(f,result)
        f:close()
        gridfs:store_file(tmpname, string.format("%s/%s",grp_tmp_dir,results_ns))
        os.remove(tmpname)
        job_mark_as_grouped(obj)
      end
    end
  elseif task_status == "REDUCE" then
    if not not_executable then
      fn = function()
        -- in reduce jobs, the value is a reference to a gridfs filename
        local value = take_value_from_gridfs(obj.cnn:gridfs(), value)
        local value = g(key,value) -- executes the REDUCE function
        assert(value, "Reduce must return a value")
        local db = obj.cnn:connect()
        db:insert(obj.results_ns, { _id=key, value=value })
        job_mark_as_finished(obj)
      end
    end
  end
  if not_executable then
    fn = function() error("Forbidden execution of jobs here") end
  end
  obj.fn = fn
  return obj
end
setmetatable(job,job)

return job