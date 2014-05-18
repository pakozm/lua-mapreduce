return {
  init = function() end,
  mapfn = function(key,value,emit)
    for line in io.lines(value) do
      for w in line:gmatch("[^%s]+") do
        emit(w,1)
      end
    end
  end
}
