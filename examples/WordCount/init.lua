-- string hash function: http://isthe.com/chongo/tech/comp/fnv/
local NUM_REDUCERS = 10
local FNV_prime    = 16777619
local offset_basis = 2166136261
local MAX          = 2^32
return {
  -- arg is for configuration purposes, it will be executed with init_args given
  -- to the server
  init = function(arg) end,

  taskfn = function()
    coroutine.yield(1,"mapreduce/server.lua")
    coroutine.yield(2,"mapreduce/worker.lua")
    coroutine.yield(3,"mapreduce/test.lua")
    coroutine.yield(4,"mapreduce/utils.lua")
  end,

  mapfn = function(key,value,emit)
    for line in io.lines(value) do
      for w in line:gmatch("[^%s]+") do
        emit(w,1)
      end
    end
  end,

  partitionfn = function(key)
    -- compute hash
    local h = offset_basis
    for i=1,#key do
      h = (h * FNV_prime) % MAX
      h = bit32.bxor(h, key:byte(i))
    end
    return h % NUM_REDUCERS
  end,

  reducefn = function(key,values)
    local count=0
    for _,v in ipairs(values) do count = count + v end
    return count
  end,
  
  finalfn = function(pairs_iterator)
    for key,value in pairs_iterator do
      print(value,key)
    end
    return true -- indicates to remove mongo gridfs result files
  end,
}
