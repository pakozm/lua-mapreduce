-- arg is for configuration purposes, it is allowed in any of the scripts
local init = function(arg)
  -- do whatever you need for initialization parametrized by arg table
end
return {
  init = init,
  taskfn = function(emit)
    emit(1,"mapreduce/server.lua")
    emit(2,"mapreduce/worker.lua")
    emit(3,"mapreduce/test.lua")
    emit(4,"mapreduce/utils.lua")
  end
}
