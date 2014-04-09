return {
  func = function(key,value)
    for line in io.lines(value) do
      for w in line:gmatch("[^%s]+") do
        coroutine.yield(w,1)
      end
    end
  end
}
