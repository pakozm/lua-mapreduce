return {
  -- init is for configuration purposes, it is allowed in any of the scripts
  init = function(arg)
    for i,v in pairs(arg) do print(i,v) end
  end,
  func = function()
    local f = io.popen("ls /home/experimentos/CORPORA/EUROPARL/en-splits/*","r")
    local i=0
    for filename in f:lines() do
      i=i+1
      coroutine.yield(i,filename)
    end
    f:close()
  end
}
