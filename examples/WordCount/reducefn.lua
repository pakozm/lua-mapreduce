return {
  init = function() end,
  reducefn = function(key,values)
    local count=0
    for _,v in ipairs(values) do count = count + v end
    return count
  end
}
