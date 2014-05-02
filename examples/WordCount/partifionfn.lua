return {
  init = function() end,
  func = function(key)
    return key:byte(#key) -- last character (numeric byte)
  end
}
