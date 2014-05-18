return {
  -- init is for configuration purposes, it is allowed in any of the scripts
  init = function(arg)
  end,
  taskfn = function(emit)
    local f = io.popen("ls /home/experimentos/CORPORA/EUROPARL/en-splits/*","r")
    local i=0
    for filename in f:lines() do
      i=i+1
      emit(i,filename)
    end
    f:close()
  end
}
