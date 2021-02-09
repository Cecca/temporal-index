install_symbolic_unit("records")

real_sizes <- tribble(
  ~dataset, ~dataset_n,
  "Flight", 684838,
  "Tourism", 835071,
  "Webkit", 1547419,
)

# TODO:
# - [x] distribution of start times and durations for the real world datasets
# - [x] best configurations, also as csv
# - [x] best configuration latex table
# - [x] scalability data and plots
# - [x] parameter dependency plots
# - [x] query focus plots

plan <- drake_plan(
  # The best configuration for each algorithm
  best_batch = target({
    file_in("temporal-index-results.sqlite")
    table_best()
  }),

  # Format the table to a latex file
  latex_batch = latex_best(best_batch) %>%
    write_file(file_out("paper/qps.tex")),

  # Export the table as a csv, to use it with D3
  csv_batch = best_batch %>%
    write_csv(file_out("docs/best.csv")),

  # Data for the scalability plot
  data_scalability = {
    file_in("temporal-index-results.sqlite")
    table_scalability()
  },

  # Scalability plot
  figure_scalability = data_scalability %>%
    plot_scalability() %>%
    save_png(file_out("paper/images/scalability.png"),
      width = 10, height = 3
    ),

  # Data for the parameter dependency plot
  data_parameter_dependency = {
    file_in("temporal-index-results.sqlite")
    table_parameter_dependency()
  },

  # Parameter dependency plot
  #
  # Compared to the old version (before removing the timing of individual
  # queries) this plot has the same behaviour (an numbers) for `both` and
  # `time` workloads. For `duration` workloads instead of the initial plateau
  # we have a peak, with much better throughputs!
  figure_parameter_dependency = data_parameter_dependency %>%
    plot_parameter_dependency() %>%
    save_png("paper/images/param_dependency.png",
      width = 5, height = 3
    ),

  # Data about the start times of real datasets
  data_start_times = table_start_times(),
  # Data about the durations of real datasets
  data_durations = table_durations(),

  # Plot with the distributions of such data
  figure_real_distribution = plot_real_distribution(
    data_start_times,
    data_durations
  ) %>%
    save_png(
      filename = "paper/images/real-distributions.png",
      width = 10,
      height = 3
    ),

  # Data for the query focus plot
  data_query_focus = {
    file_in("temporal-index-results.sqlite")
    table_query_focus() %>%
      filter(
          dataset_name == "random-uniform-zipf",
          str_detect(dataset_params, "n=10000000 ")
      )
  },

  # Figure for the query focus plot
  figure_query_focus = data_query_focus %>%
    plot_query_focus() %>%
    save_png(
      "paper/images/query-focus.png",
      width = 10,
      height = 3
    ),

  figure_query_focus_precision = data_query_focus %>%
    plot_query_focus_precision() %>%
    save_png(
      "paper/images/query-focus-precision.png",
      width = 10,
      height = 3
    ),

  figure_selectivity_dependency = data_query_focus %>%
    plot_selectivity_dependency() %>%
    save_png(
      "paper/images/selectivity-dep.png",
      width = 10,
      height = 3
    ),

  ######################################################################
  # Running example to be used in the paper
  query_range =
    interval(
      ymd_hms("2018-08-01T10:00:00"),
      ymd_hms("2018-08-01T14:00:00")
    ),
  query_duration = c(2, 4),

  data_running_example = table_running_example(
    query_range, query_duration
  ),

  figure_running_example = {
    tikzDevice::tikz(
      file = file_out("paper/example.tex"),
      width = 3.3, height = 1.4
    )
    print(plot_running_example(
      data_running_example, query_range, query_duration
    ))
    dev.off()
  },

  latex_running_example = data_running_example %>%
    latex_example() %>%
    write_file(file_out("paper/example-table.tex")),

  figure_running_example_plane = {
    tikzDevice::tikz(
      file = file_out("paper/example-plane.tex"),
      width = 3.3, height = 2.5
    )
    print(plot_running_example_plane(
      data_running_example, query_range, query_duration
    ))
    dev.off()
  },
  figure_running_example_grid = {
    tikzDevice::tikz(
      file = file_out("paper/example-grid.tex"),
      width = 3.3, height = 2.5
    )
    print(plot_running_example_plane(
      data_running_example, query_range, query_duration,
      grid = TRUE
    ))
    dev.off()
  },
)