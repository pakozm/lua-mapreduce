return {
  init = function() end,
  func = function(key,values)
    local count=0
    for _,v in ipairs(values) do count = count + v end
    return count
  end
}
