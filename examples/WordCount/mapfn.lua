return {
  init = function() print("Hello World") end,
  func = function(key,value)
    for line in io.lines(value) do
      for w in line:gmatch("[^%s]+") do
        emit(w,1)
      end
    end
  end
}
