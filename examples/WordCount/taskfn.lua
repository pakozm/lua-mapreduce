-- arg is for configuration purposes, it is allowed in any of the scripts
local init = function(arg)
  for i,v in pairs(arg) do print(i,v) end
end
return {
  init = init,
  func = function()
    coroutine.yield(1,"mapreduce/server.lua")
    coroutine.yield(2,"mapreduce/worker.lua")
    coroutine.yield(3,"mapreduce/test.lua")
    coroutine.yield(4,"mapreduce/utils.lua")
  end
}
