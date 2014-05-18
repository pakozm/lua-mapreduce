local reducefn = function(key,values,emit)
  local count=0
  for _,v in ipairs(values) do count = count + v end
  emit(count)
end
return {
  init = function() end,
  reducefn = reducefn,
  combinerfn = reducefn,
  -- This three properties are true for this reduce function.
  -- Combiners always must to fulfill this properties.
  associative_reducer = true,
  commutative_reducer = true,
  idempotent_reducer  = true,
}
