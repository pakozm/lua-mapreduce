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

-- Linear implementation of in-mutable and interned tuples for Lua. It is linear
-- because tuples are stored into a linear table. A different approach would be
-- store tuples into an inverted prefix tree (trie). Major difference between
-- both approaches is that linear implementation needs more memory but has
-- better indexing time, while prefix tree implementation needs less memory but
-- has worst indexing time.

local tuple = {
  _VERSION = "0.1",
  _NAME = "tuple",
}

-- libraries import
local assert = assert
local getmetatable = getmetatable
local ipairs = ipairs
local pairs = pairs
local select = select
local tostring = tostring
local type = type
local bit32_band = bit32.band
local bit32_lshift = bit32.lshift
local bit32_rshift = bit32.rshift
local bit32_bxor = bit32.bxor
local math_max = math.max
local string_byte = string.byte
local string_format = string.format
local string_sub = string.sub
local table_concat = table.concat
local table_pack = table.pack

-- constants
local BYTE_MASK = 0x000000FF
local WORD_MASK = 0xFFFFFFFF
local MAX_NUMBER = 2^32
local MAX_BUCKET_HOLES_RATIO = 100
local NUM_BUCKETS = 2^20
local WEAK_MT = { __mode="v" }

-- the list of tuples is a hash table with a maximum of NUM_BUCKETS
local list_of_tuples = {}

-- converts a number into a binary string, for hash computation purposes
local function dump_number(n)
  assert(n < MAX_NUMBER, "Only valid for 32 bit numbers")
  return string_format("%c%c%c%c",
		       bit32_band(n,BYTE_MASK),
		       bit32_band(bit32_rshift(n,8),BYTE_MASK),
		       bit32_band(bit32_rshift(n,16),BYTE_MASK),
		       bit32_band(bit32_rshift(n,24),BYTE_MASK))
end

-- computes the hash of a given tuple candidate
local function compute_hash(t)
  local h = 0
  for i=1,#t do
    local v = t[i]
    local tt = type(v)
    -- dump the value if it is a number, another tuple or a nil value
    if tt == "number" then v = dump_number(v)
    elseif tt == "table" then v = dump_number(compute_hash(v))
    elseif tt == "nil" then v = "nil"
    end
    -- sanity check
    assert(type(v) == "string",
	   "Needs an array with numbers, tables or strings")
    -- hash computation for every char in the string v
    for j=1,#v do
      h = h + string_byte(string_sub(v,j,j))
      h = h + bit32_lshift(h,10)
      h = bit32_bxor(h,  bit32_rshift(h,6))
      -- compute hash modules 2^32
      h = bit32_band(h, WORD_MASK)
    end
  end
  h = h + bit32_rshift(h,3)
  h = bit32_bxor(h, bit32_lshift(h,11))
  h = h + bit32_lshift(h,15)
  -- compute hash modules 2^32
  h = bit32_band(h, WORD_MASK)
  return h
end

-- tuple instances has this metatable
local tuple_instance_mt = {
  -- disallow to change metatable
  __metatable = false,
  -- avoid to insert new elements
  __newindex = function(self) error("Unable to modify a tuple") end,
  -- convert it to a string like: tuple{ a, b, ... }
  __tostring = function(self)
    local result = {}
    for i=1,#self do
      local v = self[i]
      if type(v) == "string" then v = string_format("%q",v) end
      result[#result+1] = tostring(v)
    end
    return table_concat({"tuple{",table_concat(result, ", "),"}"}, " ")
  end,
  -- concatenates two tuples or a tuple with a number, string or another table
  __concat = function(a,b)
    if type(a) ~= "table" then a,b=b,a end
    local aux = {}
    for i=1,#a do aux[#aux+1] = a[i] end
    if type(b) == "table" then
      for i=1,#b do aux[#aux+1] = b[i] end
    else
      aux[#aux+1] = b
    end
    return tuple(aux)
  end,
}

-- returns a wrapper table (proxy) which shades the data table, allowing
-- in-mutability in Lua, it receives the table data and the number of elements
local function proxy(tpl,n)
  setmetatable(tpl, tuple_instance_mt)
  return setmetatable({}, {
      -- the proxy table has an in-mutable metatable, and stores in __metatable
      -- a string identifier, the real tuple data and the number of elements
      __metatable = { "is_tuple", tpl , n },
      __index = tpl,
      __newindex = function(self) error("Tuples are in-mutable data") end,
      __len = function(self) return getmetatable(self)[3] end,
      __tostring = function(self) return tostring(getmetatable(self)[2]) end,
      __lt = function(self,other)
	local t = getmetatable(self)[2]
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
	local t = getmetatable(self)[2]
	-- equality is comparing references (tuples are in-mutable and interned)
	if self == other then return true end
	return self < other
      end,
      __pairs = function(self) return pairs(getmetatable(self)[2]) end,
      __ipairs = function(self) return ipairs(getmetatable(self)[2]) end,
      __concat = function(self,other) return getmetatable(self)[2] .. other end,
      __mode = "v",
  })
end

-- builds a candidate tuple given a table, recursively converting tables in new
-- tuples
local function tuple_constructor(t)
  local new_tuple = {}
  for i,v in pairs(t) do
    -- ignore the field "n" introduced by variadic args
    if i~="n" then
      assert(type(i) == "number" and i>0, "Needs integer keys > 0")
      if type(v) == "table" then
	-- recursively converts tables in new tuples
	new_tuple[i] = tuple(v)
      else
	-- copies the value
	new_tuple[i] = v
      end
    end
  end
  -- returns a proxy to the new_tuple table with #t length
  return proxy(new_tuple,#t)
end

-- metatable of tuple "class" table
local tuple_mt = {
  -- tuple constructor doesn't allow table loops
  __call = function(self, ...)
    local n = select('#', ...)
    local t = table_pack(...) assert(#t == n) if #t == 1 then t = t[1] end
    if type(t) ~= "table" then
      -- non-table elements are unpacked when only one is given
      return t
    else
      -- check if the given table is a tuple, if it is the case, just return it
      local mt = getmetatable(t) if mt and mt[1]=="is_tuple" then return t end
      -- create a new tuple candidate
      local new_tuple = tuple_constructor(t)
      local p = compute_hash(new_tuple) % NUM_BUCKETS
      local bucket = (list_of_tuples[p] or setmetatable({}, WEAK_MT))
      list_of_tuples[p] = bucket
      -- Count the number of elements in the bucket and the maximum non-nil key.
      -- In case the relation between this two values was greater than
      -- MAX_BUCKET_HOLES_RATIO, the bucket will be rearranged to remove all nil
      -- holes.
      local max,n = 0,0
      for i,vi in pairs(bucket) do
	local equals = true
	-- check equality by comparing all the elements one-by-one
	for j,vj in ipairs(vi) do
	  if vj ~= new_tuple[j] then equals=false break end
	end
	-- BREAKS the execution flow in case the tuple exists in the bucket
	if equals == true then return vi end
	max = math_max(max,i)
	n = n+1
      end
      -- rearrange the bucket when the ratio achieves the threshold
      if max/n > MAX_BUCKET_HOLES_RATIO then
	local new_bucket = {}
	for i,vi in pairs(bucket) do new_bucket[#new_bucket+1] = vi end
	list_of_tuples[p], bucket = new_bucket, new_bucket
	max = #bucket
	collectgarbage("collect")
      end
      bucket[max+1] = new_tuple
      -- take note of the bucket into __metatable array, position 4
      getmetatable(new_tuple)[4] = p
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
  a,b,c = nil,nil,nil
  collectgarbage("collect")
  --
  local aux = {} for i=1,10000 do aux[tuple(i,i)] = i end
  assert(tuple.stats() == 10000)
  collectgarbage("collect")
  assert(tuple.stats() == 10000)
  aux = nil
  collectgarbage("collect")
  assert(tuple.stats() == 0)
end

-- returns the number of tuples "alive", the number of used buckets, and the
-- loading factor of the hash table
tuple.stats = function()
  local num_buckets = 0
  local size = 0
  for k1,v1 in pairs(list_of_tuples) do
    num_buckets = num_buckets + 1
    for k2,v2 in pairs(v1) do size=size+1 end
  end
  if num_buckets == 0 then num_buckets = 1 end
  return size,num_buckets,size/NUM_BUCKETS
end

return tuple
