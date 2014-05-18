return {
  init = function() end,
  finalfn = function(pairs_iterator)
    for key,values in pairs_iterator do
      print(values[1],key)
    end
    return true -- indicates to remove mongo gridfs result files
  end
}
