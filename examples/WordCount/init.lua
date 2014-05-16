-- string hash function: http://isthe.com/chongo/tech/comp/fnv/
local NUM_REDUCERS = 10
local FNV_prime    = 16777619
local offset_basis = 2166136261
local MAX          = 2^32

-- arg is for configuration purposes, it will be executed with init_args given
-- to the server
local init = function(arg) end

local taskfn = function(emit)
  emit(1,"mapreduce/server.lua")
  emit(2,"mapreduce/worker.lua")
  emit(3,"mapreduce/test.lua")
  emit(4,"mapreduce/utils.lua")
end

local mapfn = function(key,value,emit)
  for line in io.lines(value) do
    for w in line:gmatch("[^%s]+") do
      emit(w,1)
    end
  end
end

local partitionfn = function(key)
  -- compute hash
  local h = offset_basis
  for i=1,#key do
    h = (h * FNV_prime) % MAX
    h = bit32.bxor(h, key:byte(i))
  end
  return h % NUM_REDUCERS
end

local reducefn = function(key,values,emit)
  local count=0
  for _,v in ipairs(values) do count = count + v end
  emit(count)
end

local combinerfn = reducefn

local finalfn = function(pairs_iterator)
  for key,value in pairs_iterator do
    print(value[1],key)
  end
  return true -- indicates to remove mongo gridfs result files
end

return {
  init = init,
  taskfn = taskfn,
  mapfn = mapfn,
  partitionfn = partitionfn,
  reducefn = reducefn,
  combinerfn = combinerfn,
  finalfn = finalfn,
  -- This three properties are true for this reduce function.
  -- Combiners always must to fulfill this properties.
  associative_reducer = true,
  commutative_reducer = true,
  idempotent_reducer  = true,
}
