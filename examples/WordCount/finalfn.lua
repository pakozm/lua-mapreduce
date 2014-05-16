return {
  init = function() end,
  finalfn = function(pairs_iterator)
    for key,value in pairs_iterator do
      print(value[1],key)
    end
    return true -- indicates to remove mongo gridfs result files
  end
}
