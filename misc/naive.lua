local vocab = {}
for line in io.lines() do
  for w in line:gmatch("[^%s]+") do
    vocab[w] = (vocab[w] or 0) + 1
  end
end
for w,v in pairs(vocab) do print(v,w) end
