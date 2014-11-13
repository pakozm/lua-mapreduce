--[[
  This file is part of Lua-Tuple (https://github.com/pakozm/lua-tuple)
  This file is part of Lua-MapReduce (https://github.com/pakozm/lua-mapreduce)
  
  Copyright 2014, Francisco Zamora-Martinez
  
  The Lua-MapReduce toolkit is free software; you can redistribute it and/or modify it
  under the terms of the GNU General Public License version 3 as
  published by the Free Software Foundation
  
  This library is distributed in the hope that it will be useful, but WITHOUT
  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
  for more details.
  
  You should have received a copy of the GNU General Public License
  along with this library; if not, write to the Free Software Foundation,
  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
]]

local tuple = {
  _VERSION = "0.1",
  _NAME = "tuple",
}

local NUM_BUCKETS = 2^20
local list_of_tuples = setmetatable({}, { __mode="v" })

local function dump_number(n)
  return string.format("%c%c%c%c%c%c%c%c",
		       bit32.band(n,0xFF),
		       bit32.band(bit32.rshift(n,8),0x00000000000000FF),
		       bit32.band(bit32.rshift(n,16),0x00000000000000FF),
		       bit32.band(bit32.rshift(n,24),0x00000000000000FF),
		       bit32.band(bit32.rshift(n,32),0x00000000000000FF),
		       bit32.band(bit32.rshift(n,40),0x00000000000000FF),
		       bit32.band(bit32.rshift(n,48),0x00000000000000FF),
		       bit32.band(bit32.rshift(n,56),0x00000000000000FF))
end

local function compute_hash(t)
  local h = 0
  for i=1,#t do
    local v = t[i]
    local tt = type(v)
    if tt == "number" then v = dump_number(v)
    elseif tt == "table" then v = dump_number(compute_hash(v))
    end
    assert(type(v) == "string",
	   "Needs an array with numbers, tables or strings")
    for j=1,#v do
      h = h + string.byte(string.sub(v,j,j))
      h = h + bit32.lshift(h,10)
      h = bit32.bxor(h,  bit32.rshift(h,6))
      h = bit32.band(h, 0x00000000FFFFFFFF)
    end
  end
  h = h + bit32.rshift(h,3)
  h = bit32.bxor(h, bit32.lshift(h,11))
  h = h + bit32.lshift(h,15)
  h = bit32.band(h, 0x00000000FFFFFFFF)
  return h
end

local tuple_instance_mt = {
  __newindex = function(self) error("Unable to modify a tuple") end,
  __tostring = function(self)
    local result = {}
    for i=1,#self do result[#result+1] = tostring(self[i]) end
    return table.concat({"tuple(",table.concat(result, ", "),")"}, " ")
  end,
  __concat = function(self,other)
    local aux = {}
    for i=1,#self do aux[#aux+1] = self[i] end
    if type(other) == "table" then
      for i=1,#other do aux[#aux+1] = other[i] end
    else
      aux[#aux+1] = other
    end
    return tuple(aux)
  end,
}

local function proxy(t)
  setmetatable(t, tuple_instance_mt)
  return setmetatable({},{
      __newindex = function(self) error("Unable to modify a tuple") end,
      __index = function(self,k) if k == "is_tuple" then return true end return t[k] end,
      __len = function(self) return #t end,
      __tostring = function(self) return tostring(t) end,
      __lt = function(self,other)
	if type(other) ~= "table" then return false
        elseif #t < #other then return true
	elseif #t > #other then return false
	elseif t == other then return false
	else
	  for i=1,#t do
	    if t[i] > other[i] then return false end
	  end
	  return true
	end
      end,
      __le = function(self,other)
	-- equality is comparing references (tuples are in-mutable and interned)
	if self == other then return true end
	return self < other
      end,
      __pairs = function(self) return pairs(t) end,
      __ipairs = function(self) return ipairs(t) end,
      __concat = function(self,other) return t .. other end,
  })
end

local function tuple_constructor(t)
  local h = 0
  local new_tuple = {}
  for i,v in ipairs(t) do
    if type(v) == "table" then
      new_tuple[i] = tuple(v)
    else
      new_tuple[i] = v
    end
  end
  return proxy(new_tuple)
end

local tuple_mt = {
  -- tuple constructor doesn't allow table loops
  __call = function(self, ...)
    local t = { ... } if #t == 1 then t = t[1] end
    if type(t) ~= "table" then
      return t
    else
      if t.is_tuple then return t end
      local new_tuple = tuple_constructor(t)
      local p = compute_hash(new_tuple) % NUM_BUCKETS
      local bucket = (list_of_tuples[p] or setmetatable({}, { __mode="v" }))
      list_of_tuples[p] = bucket
      for i,vi in ipairs(bucket) do
	local equals = true
	for j,vj in ipairs(vi) do
	  if vj ~= new_tuple[j] then equals=false break end
	end
	if equals == true then return vi end
      end
      table.insert(bucket, new_tuple)
      return new_tuple
    end
  end,
}
setmetatable(tuple, tuple_mt)

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------

tuple.utest = function()
  local a = tuple(2,{4,5},3)
  local b = tuple(4,5)
  local c = tuple(2,a[2],3)
  assert(a == c)
  assert(b == a[2])
  assert(b == c[2])
end

return tuple
