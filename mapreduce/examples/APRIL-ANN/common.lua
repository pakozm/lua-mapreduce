-- Copyright 2014, Francisco Zamora-Martinez
--
-- Common functions for APRIL-ANN training scripts
-- https://github.com/pakozm/april-ann
local mongo    = require "mongo"
local mr_utils = require "mapreduce.utils"
local ptable   = require "mapreduce.persistent_table"

-----------------------------------------------------------------------------
local TR_LOSS_KEY  = "TR_LOSS"
local NUM_REDUCERS

local dbname,dbhost
local db,gridfs,conf,params
local user_init,user_taskfn,user_finalfn
local generate_new_trainer,compute_gradients_and_loss,compute_validation_loss

-----------------------------------------------------------------------------

util.omp_set_num_threads(1)

-----------------------------------------------------------------------------

local serialize_to_gridfs = function(gridfs, filename, obj)
  gridfs:remove_file(filename)
  local builder = mongo.GridFileBuilder.New(db, dbname)
  builder:append(util.serialize(obj))
  builder:build(filename)
end

local deserialize_from_gridfs = function(gridfs, filename)
  local file = assert( gridfs:find_file(filename) )
  local str_tbl = {}
  for i=1,file:num_chunks() do
    local chunk = file:chunk(i-1)
    table.insert(str_tbl, chunk:data())
  end
  return util.deserialize(table.concat(str_tbl))
end

local serialize_and_map_emit = function(key,value,emit)
  emit(key, util.serialize(value))
end

local serialize_and_red_emit = function(value,emit)
  emit(util.serialize(value))
end

local deserialize_emitted_value = function(str)
  return util.deserialize(str)
end

------------------------------------------------------------------------------

-- the first init is executed is at MapReduce server instance when it is
-- configured
local init = function(arg)
  conf:update()
  if not conf.train_func or conf.finished then
    if conf.finished then
      conf:drop()
      local list = gridfs:list()
      for r in list:results() do gridfs:remove_file(r.filename) end
    end
    conf.finished = false
    -- store current iteration and best model in a GridFS file (tmpname)
    conf.train_func = os.tmpname()
    --
    mr_utils.remove(conf.train_func)
    local trainer,train_func = generate_new_trainer_and_train_func()
    train_func:get_state_table().last = trainer
    serialize_to_gridfs(gridfs, conf.train_func, train_func)
    conf:update()
  end
  user_init(arg, conf)
  if conf:dirty() then conf:update() end
end

local taskfn = function(emit)
  conf:update()
  user_taskfn(emit,conf)
  if conf:dirty() then conf:update() end
end

local mapfn = function(key, value, emit)
  conf:update()
  local train_func = deserialize_from_gridfs(gridfs, assert(conf.train_func))
  local trainer = train_func:get_state_table().last
  conf:read_only(true)
  local weight_grads,loss_matrix,bunch_size =
    compute_gradients_and_loss(trainer, key, value, conf)
  conf:read_only(false)
  assert(weight_grads and loss_matrix and bunch_size,
         "compute_gradients_and_loss had to return gradients, loss_matrix and bunch_size")
  for name,grads in pairs(weight_grads) do
    serialize_and_map_emit(name,
                           {
                             grads,
                             trainer:weights(name):get_shared_count()*bunch_size
                           },
                           emit)
  end
  serialize_and_map_emit(TR_LOSS_KEY, loss_matrix, emit)
end

local partitionfn = function(key)
  local sum = 0 for i=1,#key do sum = sum + key:byte(i) end
  return sum % NUM_REDUCERS
end

local loss
local reducefn = function(key, values, emit)
  if not loss then
    local train_func = deserialize_from_gridfs(gridfs, assert(conf.train_func))
    local trainer    = train_func:get_state_table().last
    loss = trainer:get_loss_function()
  end
  if key == TR_LOSS_KEY then
    loss:reset()
    for i=1,#values do
      local v = deserialize_emitted_value(values[i])
      loss:accum_loss(v)
    end
    serialize_and_red_emit({ loss:get_accum_loss() }, emit)
  else
    -- accumulate gradients and shared count
    local t = deserialize_emitted_value(values[1])
    local gradient = t[1]
    local counts   = t[2]
    for i=2,#values do
      local t = deserialize_emitted_value(values[i])
      gradient:axpy(1.0, t[1])
      counts = counts + t[2]
    end
    serialize_and_red_emit({ gradient, counts }, emit)
  end
end

local train_func
local trainer
local optimizer
local thenet
local loss
local finalfn = function(pairs_iterator)
  local weight_grads = matrix.dict()
  conf:update()
  if not train_func then
    train_func = deserialize_from_gridfs(gridfs, assert(conf.train_func))
    trainer    = train_func:get_state_table().last
    optimizer  = trainer:get_optimizer()
    thenet     = trainer:get_component()
    loss       = trainer:get_loss_function()
  end
  --
  local tr_loss_mean,tr_loss_var
  for key,values in pairs_iterator do
    local value = deserialize_emitted_value(values[1])
    if key == TR_LOSS_KEY then
      tr_loss_mean = value[1]
      tr_loss_var  = value[2]
    else
      local N = value[2] if not N or N==0 then N=1 end
      if params.smooth_gradients then
        -- gradients smoothing
        value[1]:scal( 1.0/math.sqrt(N) )
      end
      weight_grads[key] = value[1]
    end
  end
  assert(tr_loss_mean)
  --
  conf:read_only(true)
  local weights_table = trainer:get_weights_table()
  local non_stop =
    train_func:execute(function()
	optimizer:execute(function(x,it)
	    if x ~= weights_table then
	      trainer:build{ weights = x }
	      weights_table = x
	    end
	    assert(not it or it == 0)
	    thenet:reset(it)
	    return tr_loss_mean,weight_grads
			  end,
	    weights_table)
	local va_loss_mean,va_loss_var =
	  compute_validation_loss(trainer, conf)
	return trainer, tr_loss_mean, va_loss_mean
    end)
  conf:read_only(false)
  serialize_to_gridfs(gridfs, assert(conf.train_func), train_func)
  --
  if non_stop then
    user_finalfn(train_func, conf)
    if conf:dirty() then conf:update() end
    return "loop"
  else
    conf.finished = true
    conf:update()
    return true
  end
end

------------------------------------------------------------------------------

local make_map_reduce_task_table = function(t)
  params = get_table_fields(
    {
      num_reducers               = { mandatory = true, type_match="number" },
      dbname                     = { mandatory = true, type_match="string" },
      dbhost                     = { mandatory = true, type_match="string" },
      compute_gradients_and_loss = { mandatory = true, type_match="function" },
      compute_validation_loss    = { mandatory = true, type_match="function" },
      user_init                  = { mandatory = true, type_match="function" },
      user_taskfn                = { mandatory = true, type_match="function" },
      user_finalfn               = { mandatory = true, type_match="function" },
      generate_new_trainer_and_train_func = { mandatory = true, type_match="function" },
      smooth_gradients = { mandatory = false, default = true },
    }, t)
  --
  dbname = params.dbname
  dbhost = params.dbhost
  db     = mr_utils.connect(dbhost)
  gridfs = mongo.GridFS.New(db, dbname)
  -- persistent table allows to store data which will be accessed in a
  -- distributed way
  conf      = ptable("conf", dbhost, dbname)
  compute_gradients_and_loss = params.compute_gradients_and_loss
  compute_validation_loss    = params.compute_validation_loss
  generate_new_trainer_and_train_func = params.generate_new_trainer_and_train_func
  user_init    = params.user_init
  user_taskfn  = params.user_taskfn
  user_finalfn = params.user_finalfn
  NUM_REDUCERS = params.num_reducers
  return {
    init        = init,
    --
    taskfn      = taskfn,
    mapfn       = mapfn,
    partitionfn = partitionfn,
    reducefn    = reducefn,
    finalfn     = finalfn,
  }
end

local aux_cache = {}
local cached = function(key,func,cache)
  local cache = cache or aux_cache
  if not cache[key] then
    cache[key] = func(key)
  end
  return cache[key]
end


------------------------------------------------------------------------------

return {
  make_map_reduce_task_table = make_map_reduce_task_table,
  cached = cached,
  serialize_to_gridfs = serialize_to_gridfs,
  deserialize_from_gridfs = deserialize_from_gridfs,
}
