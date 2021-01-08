db_file <- here("temporal-index-results.sqlite")
conn <- dbConnect(RSQLite::SQLite(), db_file)

install_symbolic_unit("records")

real_sizes <- tribble(
  ~dataset, ~dataset_n,
  "Flight", 684838,
  "Tourism", 835071,
  "Webkit", 1547419,
)

# TODO:
# - distribution of start times and durations for the real world datasets
# - best configurations, also as csv
# - best configuration latex table
# - scalability data and plots
# - parameter dependency plots
# - query focus plots

plan <- drake_plan(
  # The best configuration for each algorithm
  best_batch = target(
    table_best(),
    trigger = trigger(file = db_file)
  ),

  # Format the table to a latex file
  latex_batch = latex_best(best_batch) %>%
    write_file(file_out("paper/qps.tex")),
)