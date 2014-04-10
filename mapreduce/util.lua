local util = {
  _VERSION = "0.1",
  _NAME = "mapreduce.util",
  DEFAULT_RW_TIMEOUT = 30, -- seconds
  DEFAULT_SLEEP = 1, -- seconds
  DEFAULT_MICRO_SLEEP = 0.1, -- seconds
  DEFAULT_HOSTNAME = "<unknown>",
  DEFAULT_TMPNAME = "<NONE>",
  DEFAULT_DATE = 0,
  STATUS = { WAITING = 0, RUNNING = 1, BROKEN = 2, FINISHED = 3, },
}

-------------------------------------------------------------------------------

local function iscallable(obj)
  local t = type(obj)
  return t == "function" or (t == "table" and (getmetatable(obj) or {}).__call)
end

local valid_get_table_fields_params_attributes = { type_match = true,
                                                   mandatory  = true,
                                                   getter     = true,
                                                   default    = true }
local function get_table_fields(params, t, ignore_other_fields)
  local type   = type
  local pairs  = pairs
  local ipairs = ipairs
  --
  local params = params or {}
  local t      = t or {}
  local ret    = {}
  for key,value in pairs(t) do
    if not params[key] then
      if ignore_other_fields then
        ret[key] = value
      else
        error("Unknown field: " .. key)
      end
    end
  end
  for key,data in pairs(params) do
    if params[key] then
      local data = data or {}
      for k,_ in pairs(data) do
        if not valid_get_table_fields_params_attributes[k] then
          error("Incorrect parameter to function get_table_fields: " .. k)
        end
      end
      -- each param has type_match, mandatory, default, and getter
      local v = t[key]
      if v == nil then v = data.default end
      if v == nil and data.mandatory then
        error("Mandatory field not found: " .. key)
      end
      if v ~= nil and data.type_match and (type(v) ~= data.type_match) then
        if data.type_match ~= "function" or not iscallable(v) then
          error("Incorrect type '" .. type(v) .. "' for field '" .. key .. "'")
        end
      end
      if data.getter then v=(t[key]~=nil and data.getter(t[key])) or nil end
      ret[key] = v
    end  -- if params[key] then ...
  end -- for key,data in pairs(params) ...
  return ret
end

local function get_table_fields_ipairs(...)
  local arg = table.pack(...)
  return function(t)
    local table = table
    local t   = t or {}
    local ret = {}
    for i,v in ipairs(t) do
      table.insert(ret, get_table_fields(table.unpack(arg), v))
    end
    return ret
  end
end

local function get_table_fields_recursive(...)
  local arg = table.pack(...)
  return function(t)
    local t = t or {}
    return get_table_fields(table.unpack(arg), t)
  end
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
  os.execute("sleep " .. tonumber(n))
end

-- makes a map/reduce task table
local function make_task(key, value)
  assert(key~=nil and value~=nil, "Needs a key and a value")
  return {
    key = tostring(key),
    value = value,
    worker = util.DEFAULT_HOSTNAME,
    tmpname = util.DEFAULT_TMPNAME,
    enqued_at = os.time(),
    status = util.STATUS.WAITING,
  }
end

--------------------------------------------------------------------------------

util.iscallable = iscallable
util.get_table_fields = get_table_fields
util.get_table_fields_ipairs = get_table_fields_ipairs
util.get_table_fields_recursive = get_table_fields_recursive
util.get_hostname = get_hostname
util.check_mapreduce_result = check_mapreduce_result
util.sleep = sleep
util.make_task = make_task

return util
