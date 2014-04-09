return function()
  coroutine.yield(1,"mapreduce/server.lua")
  coroutine.yield(2,"mapreduce/worker.lua")
  coroutine.yield(3,"mapreduce/test.lua")
  coroutine.yield(4,"mapreduce/util.lua")
end
