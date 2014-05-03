local it = 0
return {
  init = function() end,
  func = function(pairs_iterator)
    it = it + 1
    for key,value in pairs_iterator do
      print(value,key)
    end
    return true -- indicates to remove mongo gridfs result files
  end
}
