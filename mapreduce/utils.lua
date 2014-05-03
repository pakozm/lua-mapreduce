local mongo = require "mongo"

local utils = {
  _VERSION = "0.1",
  _NAME = "mapreduce.utils",
  DEFAULT_RW_TIMEOUT = 300, -- seconds
  DEFAULT_SLEEP = 1, -- seconds
  DEFAULT_MICRO_SLEEP = 0.1, -- seconds
  DEFAULT_HOSTNAME = "<unknown>",
  DEFAULT_TMPNAME = "<NONE>",
  DEFAULT_DATE = 0,
  STATUS = {
    WAITING = 0,  -- default job status
    RUNNING = 1,  -- status when a worker is running the job
    BROKEN = 2,   -- a job which is detected as broken
    FINISHED = 3, -- a finished job
    WRITTEN = 4,  -- a finished job which results has been written
  },
  TASK_STATUS = {
    WAIT     = "WAIT",
    MAP      = "MAP",
    REDUCE   = "REDUCE",
    FINISHED = "FINISHED",
  },
  MAX_PENDING_INSERTS  = 50000,
  MAX_IT_WO_CGARBAGE   =  5000,
  MAX_TIME_WO_CGARBAGE =    60, -- 1 minute
  GRP_TMP_DIR = "/tmp/grouped",
  RED_JOB_TMP_DIR = "/tmp/red_job",
}

local STATUS = utils.STATUS

-------------------------------------------------------------------------------

local function connect(cnn_string, auth_table)
  local db = assert( mongo.Connection.New{ auto_reconnect=true,
                                           rw_timeout=utils.DEFAULT_RW_TIMEOUT} )
  assert( db:connect(cnn_string) )
  if auth_table then db:auth(auth_table) end
  assert( not db:is_failed(), "Impossible to connect :S" )
  return db
end

local function iscallable(obj)
  local t = type(obj)
  return t == "function" or (t == "table" and (getmetatable(obj) or {}).__call)
end

local function get_hostname()
  local p = io.popen("hostname","r")
  local hostname = p:read("*l")
  p:close()
  return hostname
end

local function check_mapreduce_result(res)
  return res.ok==1,string.format("%s (code: %d)",
                                 res.errmsg or "", res.code or 0)
end

local function sleep(n)
  -- print("SLEEP ",n)
  os.execute("sleep " .. tonumber(n))
end

-- makes a map/reduce job document
local function make_job(key, value)
  assert(key~=nil and value~=nil, "Needs a key and a value")
  return {
    _id = tostring(key) or error("Key must be convertible to string"),
    value = value,
    worker = utils.DEFAULT_HOSTNAME,
    tmpname = utils.DEFAULT_TMPNAME,
    creation_time = os.time(),
    status = utils.STATUS.WAITING,
  }
end

local function escape(str)
  if type(str) == "number" then
    return tostring(str)
  else
    str = assert(tostring(str),"Unable to convert to string map value")
    return ( string.format("%q",str):gsub("\\\n","\\n") )
  end
end

local function serialize_table_ipairs(t)
  local result = {}
  for _,v in ipairs(t) do
    table.insert(result, escape(v))
  end
  return string.format("{%s}",table.concat(result, ","))
end

local function serialize_sorted_by_lines(f,result,combiner)
  local keys = {}
  for k,v in pairs(result) do
    table.insert(keys,k)
    -- combine as soon as possible allows reduction of intermediate file sizes
    if combiner and #v > 1 then
      result[k] = { assert(combiner(k,v)) }
    end
  end
  table.sort(keys)
  for _,key in ipairs(keys) do
    local key_str = escape(key)
    local value_str = serialize_table_ipairs(result[key])
    f:write(string.format("return %s,%s\n", key_str, value_str))
  end
end

-- iterates over all the lines of a given gridfs filename, and returns the first
-- and last chunks where the line is contained, and the first and last position
-- inside the corresponding chunks
local function gridfs_lines_iterator(gridfs, filename)
  local gridfile      = gridfs:find_file(filename)
  local size          = #gridfile
  local current_chunk = 0
  local current_pos   = 1
  local abs_pos       = 0
  local num_chunks    = gridfile:num_chunks()
  local chunk,data
  return function()
    -- capture the variables to avoid garbage collection, and to improve
    -- performance
    local gridfs = gridfs
    local gridfile = gridfile
    if current_chunk < num_chunks then
      chunk = chunk or gridfile:chunk(current_chunk)
      if current_pos <= chunk:len() then
        local first_chunk = current_chunk
        local last_chunk  = current_chunk
        local first_chunk_pos = current_pos
        local last_chunk_pos = current_pos
        local tbl = {}
        local found_line = false
        repeat
          chunk = chunk or gridfile:chunk(current_chunk)
          data  = data  or chunk:data()
          local chunk_len = chunk:len()
          local match = data:match("^([^\n]*)\n", current_pos)
          if match then
            table.insert(tbl, match)
            current_pos = #match + current_pos + 1 -- +1 because of the \n
            abs_pos     = #match + abs_pos + 1
            found_line = true
          else -- if match ... then
            -- inserts the whole chunk substring, no \n match found
            table.insert(tbl, data:sub(current_pos, chunk_len))
            current_pos = chunk_len + 1 -- forces to go next chunk
            abs_pos     = abs_pos + #tbl[#tbl]
          end -- if match ... then else ...
          last_chunk_pos = current_pos - 1
          last_chunk = current_chunk
          -- go to next chunk if we are at the end
          if current_pos > chunk_len then
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
    assert(abs_pos == size,
           string.format("Unexpected end-of-file (found %d != expected %d): %s\n",
                         abs_pos, size, filename))
  end -- return function()
end

--------------------------------------------------------------------------------

utils.iscallable = iscallable
utils.get_table_fields = get_table_fields
utils.get_table_fields_ipairs = get_table_fields_ipairs
utils.get_table_fields_recursive = get_table_fields_recursive
utils.get_hostname = get_hostname
utils.check_mapreduce_result = check_mapreduce_result
utils.sleep = sleep
utils.make_job = make_job
utils.connect = connect
utils.escape = escape
utils.serialize_table_ipairs = serialize_table_ipairs
utils.serialize_sorted_by_lines = serialize_sorted_by_lines
utils.gridfs_lines_iterator = gridfs_lines_iterator

return utils
