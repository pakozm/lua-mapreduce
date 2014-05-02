return {
  init = function() end,
  func = function(it)
    for key,value in it do
      print(value,key)
    end
    return true -- indicates to remove mongo gridfs result files
  end
}
