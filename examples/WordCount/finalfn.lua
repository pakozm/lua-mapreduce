return {
  init = function() end,
  func = function(query)
    for pair in query:results() do
      print(pair.value, pair._id)
    end
  end
}
