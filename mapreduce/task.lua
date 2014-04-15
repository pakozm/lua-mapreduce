-- sets the task job for the cluster, which indicates the workers which kind of
-- work (MAP,REDUCE,...) must perform
local task = {
  _VERSION = "0.1",
  _NAME = "task",
}

function task:__call(cnn)
  local dbname = self.cnn:dbname()
  local obj = {
    server = server,
    ns = cnn:dbname() .. ".task",
    map_jobs_ns    = dbname .. ".map_jobs",
    map_results_ns = dbname .. ".map_results",
    red_jobs_ns    = dbname .. ".red_jobs",
    red_results_ns = dbname .. ".red_results",                
  }
  setmetatable(obj, { __index=self })
  return obj
end
setmetatable(task,task)

function task:create_collection(job_type,params)
  local db = self.cnn:connect()
  assert( db:update(self.ns, { key = "unique" },
                    { ["$set"] = {
                        key         = "unique",
                        job         = job_type,
                        --
                        mapfn       = params.mapfn,
                        reducefn    = params.reducefn,
                        map_args    = params.map_args,
                        reduce_args = params.reduce_args,
                        --
                        map_tasks   = self.map_jobs_ns,
                        map_results = self.map_results_ns,
                        red_tasks   = self.red_jobs_ns,
                        red_results = self.red_results_ns,
                    }, },
                    true, false) )
end

function task:update()
  local db = cnn:connect()
  self.status = db:find_one(self.ns)
  if self.status then
    if self.status.job == "MAP" then
      self.current_jobs_ns   = self.map_jobs_ns
      self.current_results_ns = self.map_results_ns
      self.current_fname = self.mapfn
      self.current_args  = self.map_args
    elseif self.status.job == "REDUCE" then
      self.current_jobs_ns   = self.red_jobs_ns
      self.current_results_ns = self.red_results_ns
      self.current_fname = self.reducefn
      self.current_args  = self.reduce_args
    end
  else
    self.current_results_ns = nil
    self.current_results_ns = nil
    self.current_fname = nil
    self.current_args  = nil
  end
end

function task:finished()
  return not self.status or self.status.job == "FINISHED"
end

function task:get_job_type()
  assert(self.status)
  return self.status.job
end

function task:set_job_type(job_type)
  local db = self.server:connect()
  assert( db:update(self.ns, { key = "unique" },
                    { ["$set"] = { job = job_type } },
                    true, false) )
end

function task:get_jobs_ns()
  return self.current_jobs_ns
end

function task:get_results_ns()
  return self.current_results_ns
end

function task:get_fname()
  return self.current_fname
end

function task:get_args()
  return self.current_args
end

return task
