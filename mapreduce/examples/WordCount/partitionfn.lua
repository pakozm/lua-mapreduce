-- string hash function: http://isthe.com/chongo/tech/comp/fnv/
local NUM_REDUCERS = 15
local FNV_prime    = 16777619
local offset_basis = 2166136261
local MAX          = 2^32
return {
  init = function() end,
  partitionfn = function(key)
    -- compute hash
    local h = offset_basis
    for i=1,#key do
      h = (h * FNV_prime) % MAX
      h = bit32.bxor(h, key:byte(i))
    end
    return h % NUM_REDUCERS
  end
}
