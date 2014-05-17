local reducefn = function(key,values,emit)
  local count=0
  for _,v in ipairs(values) do count = count + v end
  emit(count)
end
return {
  init = function() end,
  reducefn = reducefn,
  combinerfn = reducefn,
}
