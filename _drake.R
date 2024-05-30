source("R/packages.R")
source("R/workloads.R")
source("R/functions.R")
source("R/tables.R")
source("R/latex.R")
source("R/plots.R")
source("R/plan.R")
source("R/parallel.R")

# drake_config(plan, lock_envir = FALSE)
drake_config(parallel_plan, lock_envir = FALSE)

