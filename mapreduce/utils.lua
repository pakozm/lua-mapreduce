local mongo = require "mongo"

assert(mongo._VERSION == "0.4" or tonumber(mongo._VERSION > 0.4))

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
  MAX_MAP_RESULT       =  5000,
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

-- receives a dictionary and returns an array with the keys in order
local function keys_sorted(result)
  local keys = {}
  for k,v in pairs(result) do table.insert(keys,k) end
  table.sort(keys)
  return keys
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
  local tbl           = {}
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
        -- faster than tbl = {}
        for k,v in ipairs(tbl) do tbl[k] = nil end
        local found_line = false
        repeat
          chunk = chunk or gridfile:chunk(current_chunk)
          data  = data  or chunk:data()
          local chunk_len = chunk:len()
          local match = data:match("^([^\n]*)\n", current_pos)
          if match then
            tbl[ #tbl+1 ] =  match
            current_pos = #match + current_pos + 1 -- +1 because of the \n
            abs_pos     = #match + abs_pos + 1
            found_line = true
          else -- if match ... then
            -- inserts the whole chunk substring, no \n match found
            tbl[ #tbl+1 ] = data:sub(current_pos, chunk_len)
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
            -- faster than tbl = {}
            for k,v in ipairs(tbl) do tbl[k] = v end
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

-- receives a gridfs object and an array of filenames (remote filenames) and
-- performs a merge operation over all its lines; the files content is expected
-- to be something like "return k,v" in every line, so them could be loaded as
-- Lua strings
local function merge_iterator(gridfs, filenames)
  -- initializes all the line iterators (one for each file)
  local line_iterators = {}
  for _,name in ipairs(filenames) do
    line_iterators[ #line_iterators+1 ] = gridfs_lines_iterator(gridfs,name)
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
  -- initialize data with first line over all files
  for i=1,#filenames do take_next(i) end
  local counter = 0
  -- the following closure is the iterator
  return function()
    local MAX_IT_WO_CGARBAGE = utils.MAX_IT_WO_CGARBAGE
    local assert       = assert
    local data         = data
    local take_next    = take_next
    local finished     = finished
    -- merge all the files until finished
    while not finished() do
      counter = counter + 1
      --
      local mins_list = search_min()
      assert(#mins_list > 0)
      local key = data[mins_list[1]][1]
      local result
      if #mins_list == 1 then
        -- only one secuence of values, nothing to merge
        result = data[mins_list[1]][2]
        take_next(mins_list[1])
      else -- if #mins_list == 1 then ... else
        result = {}
        for i=1,#mins_list do
          local which = mins_list[i]
          -- sanity check
          assert(data[which][1] == key)
          local v = data[which][2]
          for j=1,#v do result[ #result+1 ] = v[j] end
          take_next(which)
        end
      end -- if #mins_list == 1 then ... else ... end
      -- verbose output
      if counter % MAX_IT_WO_CGARBAGE == 0 then
        collectgarbage("collect")
      end
      return key,result
    end -- while not finished()
    -- remove all map result gridfs files
    -- for _,name in ipairs(filenames) do gridfs:remove_file(name) end  
  end -- return function
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
utils.gridfs_lines_iterator = gridfs_lines_iterator
utils.keys_sorted = keys_sorted
utils.merge_iterator = merge_iterator

return utils
