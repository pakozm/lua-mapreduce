-- Copyright 2014, Francisco Zamora-Martinez
--
-- Example of iterative MapReduce to train an ANN using April-ANN toolkit:
-- https://github.com/pakozm/april-ann

local NUM_REDUCERS = 10
local EXP_DBHOST   = "localhost"
local EXP_DBNAME   = "exp_digits"

local bunch_size     = 32
local weights_random = random(1234)
local description    = "256 inputs 256 tanh 128 tanh 10 log_softmax"
local inf            = -1
local sup            =  1
local shuffle_random = random() -- TOTALLY RANDOM FOR EACH WORKER
local learning_rate  = 0.005
local momentum       = 0.02
local weight_decay   = 1e-04
local max_epochs     = 40
local min_epochs     = 20

local common = require "examples.April-ANN.common"
local cached = common.cached
--
local mat_cache = {}
local ds_cache  = {}

-- generates a new MLP with random weights
local generate_new_trainer_and_train_func = function()
  local thenet  = ann.mlp.all_all.generate(description)
  local trainer = trainable.supervised_trainer(thenet,
                                               ann.loss.multi_class_cross_entropy(10),
                                               bunch_size)
  trainer:build()
  trainer:randomize_weights{
    random      = weights_random,
    inf         = inf,
    sup         = sup,
    use_fanin   = true,
  }
  trainer:set_option("learning_rate", learning_rate)
  trainer:set_option("momentum",      momentum)
  trainer:set_option("weight_decay",  weight_decay)
  -- it is better to avoid BIAS regularization 
  trainer:set_layerwise_option("b.", "weight_decay", 0)
  --
  local stopping_criterion =
    trainable.stopping_criteria.make_max_epochs_wo_imp_relative(2)
  local train_func = trainable.train_holdout_validation{
    min_epochs = min_epochs,
    max_epochs = max_epochs,
    stopping_criterion = stopping_criterion
  }
  return trainer,train_func
end

-- the first init is executed is at MapReduce server instance when it is
-- configured; this function receives the arguments list given to server
-- configure method, and a persistent table where it is possible to store
-- persistent data which could be retrieved from map/reduce functions
local user_init = function(arg,conf)
end

-- receives the emit function and the persistent table in read-only mode
local user_taskfn = function(emit,conf)
  emit(1,"misc/digits.png")
  emit(2,"misc/digits.png")
  emit(3,"misc/digits.png")
  emit(4,"misc/digits.png")
end

local m2 = matrix(10,{1,0,0,0,0,0,0,0,0,0})

local make_load_matrix = function(value)
  return function()
    return ImageIO.read(value):to_grayscale():invert_colors():matrix()
  end
end

local make_load_dataset = function(mat)
  return function()
    local train_input = dataset.matrix(mat,
                                       {
                                         patternSize = {16,16},
                                         offset      = {0,0},
                                         numSteps    = {80,10},
                                         stepSize    = {16,16},
                                         orderStep   = {1,0}
                                       })
    -- a circular dataset which advances with step -1
    local train_output = dataset.matrix(m2,
                                        {
                                          patternSize = {10},
                                          offset      = {0},
                                          numSteps    = {800},
                                          stepSize    = {-1},
                                          circular    = {true}
                                        })
    -- VALIDATION --
    local val_input = dataset.matrix(mat,
                                     {
                                       patternSize = {16,16},
                                       offset      = {1280,0},
                                       numSteps    = {20,10},
                                       stepSize    = {16,16},
                                       orderStep   = {1,0}
                                     })
    local val_output   = dataset.matrix(m2,
                                        {
                                          patternSize = {10},
                                          offset      = {0},
                                          numSteps    = {200},
                                          stepSize    = {-1},
                                          circular    = {true}
                                        })
    return {
      train_input  = dataset.token.wrapper(train_input),
      train_output = dataset.token.wrapper(train_output),
      val_input    = dataset.token.wrapper(val_input),
      val_output   = dataset.token.wrapper(val_output),
    }
  end
end

-- receives the persistent table in read-only mode as last argument
local compute_gradients_and_loss = function(trainer, key, value, conf)
  local mat    = cached(value, make_load_matrix(value), mat_cache)
  local ds_tbl = cached(value, make_load_dataset(mat),  ds_cache)
  local in_ds  = ds_tbl.train_input
  local out_ds = ds_tbl.train_output
  local bunch_tbl = {}
  for i=1,bunch_size do
    table.insert(bunch_tbl, shuffle_random:randInt(1,in_ds:numPatterns()))
  end
  local input  = in_ds:getPatternBunch(bunch_tbl)
  local target = out_ds:getPatternBunch(bunch_tbl)
  local grads,tr_loss,tr_loss_matrix = trainer:compute_gradients_step(input,
                                                                      target)
  return grads,tr_loss_matrix
end

-- receives the persistent table in read-only mode as last argument
local compute_validation_loss = function(trainer, conf)
  util.omp_set_num_threads(4)
  local value  = "misc/digits.png"
  local mat    = cached(value, make_load_matrix(value), mat_cache)
  local ds_tbl = cached(value, make_load_dataset(mat),  ds_cache)
  local in_ds  = ds_tbl.val_input
  local out_ds = ds_tbl.val_output
  return trainer:validate_dataset{
    input_dataset  = in_ds,
    output_dataset = out_ds,
  }
end

-- the last argument is the persistent table (allows read/write operations)
local user_finalfn = function(train_func, conf)
  print(train_func:get_state_string())
  train_func:save("best_func.lua")
end

-----------------------------------------------------------------------------

return common.make_map_reduce_task_table {
  num_reducers = NUM_REDUCERS,
  dbname       = EXP_DBNAME,
  dbhost       = EXP_DBHOST,
  user_init    = user_init,
  user_taskfn  = user_taskfn,
  user_finalfn = user_finalfn,
  generate_new_trainer_and_train_func = generate_new_trainer_and_train_func,
  compute_gradients_and_loss = compute_gradients_and_loss,
  compute_validation_loss    = compute_validation_loss,
}
