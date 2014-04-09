return function(key,values)
  local count=0
  for _,v in ipairs(values) do count = count + v end
  coroutine.yield(key,count)
end
