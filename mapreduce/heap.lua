--[[
  This file is part of Lua-MapReduce
  
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

local heap = {
  _VERSION = "0.1",
  _NAME = "mapreduce.heap",
}

local function parent(p) return math.floor(p/2) end
local function left(p) return 2*p end
local function right(p) return 2*p+1 end

function heap:top()
  return self.data[1]
end

function heap:pop()
  local data, cmp = self.data, self.cmp
  local v = data[#data]
  data[#data] = nil
  if #data == 0 then return end
  -- heapify(1)
  local pos = 1
  while true do
    local l,r = left(pos), right(pos)
    local lv, rv = data[l], data[r]
    if not lv then break end
    local child = (rv and cmp(rv,lv) and r) or l
    if cmp(data[child],v) then
      data[pos] = data[child]
      pos = child
    else
      break
    end
  end
  data[pos] = v
end

function heap:push(v)
  local data, cmp = self.data, self.cmp
  local pos = #data + 1
  -- bubble up
  while pos > 1 do
    local p  = parent(pos)
    local pv = data[p]
    if cmp(v,pv) then
      data[pos] = data[p]
      pos = p
    else
      break
    end
  end
  data[pos] = v
end

function heap:clear()
  self.data = {}
end

function heap:size()
  return #self.data
end

function heap:empty()
  return #self.data == 0
end

function heap:__call(cmp)
  local obj = {
    cmp  = cmp or function(a,b) return a < b end,
    data = {},
  }
  setmetatable(obj, { __index = self,
                      __len = function(self) return #self.data end })
  return obj
end
setmetatable(heap, heap)

----------------------------------------------------------------------------
------------------------------ UNIT TEST -----------------------------------
----------------------------------------------------------------------------

heap.utest = function()
  local t = { 20, 10, 15, 1 }
  local h = heap()
  assert(h:empty())
  assert(h:top() == nil)
  assert(h:size() == 0)
  h:push(30)
  assert(h:top() == 30)
  h:clear()
  assert(h:empty())
  assert(h:top() == nil)
  assert(h:size() == 0)
  for i=1,#t do h:push(t[i]) end
  table.sort(t)
  for i=1,#t do
    assert(h:top() == t[i])
    h:pop()
  end
  assert(h:top() == nil)
end

return heap
