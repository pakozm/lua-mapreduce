return {
  func = function(db,ns)
    local q = db:query(ns,{})
    for pair in q:results() do
      print(pair.value, pair.key)
    end
  end
}

