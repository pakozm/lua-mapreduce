-- arg is for configuration purposes, it is allowed in any of the scripts
local init = function(arg)
  -- do whatever you need for initialization parametrized by arg table
end
return {
  init = init,
  taskfn = function()
    coroutine.yield(1,"mapreduce/server.lua")
    coroutine.yield(2,"mapreduce/worker.lua")
    coroutine.yield(3,"mapreduce/test.lua")
    coroutine.yield(4,"mapreduce/utils.lua")
  end
}
