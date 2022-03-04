install_symbolic_unit("records")
Sys.setlocale("LC_ALL", "en_US")

real_sizes <- tribble(
  ~dataset, ~dataset_n,
  "Flight", 684838,
  "Tourism", 835071,
  "Webkit", 1547419,
  "MimicIII", 4134909
)

plan <- drake_plan(
  # The best configuration for each algorithm
  best_batch = target({
    file_in("temporal-index-results.sqlite")
    table_best()
  }),

  # Same data than in best_batch, but in wide format
  best_wide = target({
    get_best_wide(best_batch)
  }),

  simulated_tradeoff_do_ro = get_simulated_tradeoff_do_ro(best_wide),
  simulated_tradeoff_do_rd = get_simulated_tradeoff_do_rd(best_wide),
  simulated_tradeoff_ro_rd = get_simulated_tradeoff_ro_rd(best_wide),
  simulated_tradeoff_tern = get_simulated_tradeoff_tern(best_wide),

  # Format the table to a latex file
  latex_batch = best_batch %>%
    filter(
      dataset_name %in% c("UZ", "Flight", "Webkit", "MimicIII"),
      time_constraint %in% c("UZ", "UU", "-"),
      duration_constraint %in% c("U", "-")
    ) %>%
    latex_best() %>%
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
      width = 5, height = 4
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
      height = 2
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
      height = 2.5
    ),

  figure_query_focus_inefficient_precision = table_query_focus_inefficient() %>%
    plot_query_focus_precision() %>%
    save_png(
      "paper/images/query-focus-inefficient-precision.png",
      width = 4,
      height = 4
    ),

  figure_selectivity_dependency_inefficient = table_query_focus_inefficient() %>%
    plot_selectivity_dependency(bare = FALSE, strip = FALSE, legend = FALSE) %>%
    save_png(
      "paper/images/selectivity-dep-inefficient.png",
      width = 4,
      height = 4
    ),

  figure_simulated_workload_do_ro = simulated_tradeoff_do_ro %>%
    plot_simulated_tradeoff() %>%
    save_png(
      "paper/images/simulated_tradeoff_do_ro.png",
      width = 8,
      height = 3
    ),
  figure_simulated_workload_do_rd = simulated_tradeoff_do_rd %>%
    plot_simulated_tradeoff() %>%
    save_png(
      "paper/images/simulated_tradeoff_do_rd.png",
      width = 8,
      height = 3
    ),
  figure_simulated_workload = {
    p1 <- simulated_tradeoff_do_ro %>% plot_simulated_tradeoff()
    p2 <- simulated_tradeoff_do_rd %>% plot_simulated_tradeoff()
    p3 <- simulated_tradeoff_ro_rd %>% plot_simulated_tradeoff(col = frac_ro, xlab = "Fraction of range queries")
    p <- (p1 / p2 / p3 / patchwork::guide_area()) + plot_layout(guides = "collect")
    save_png(
      p,
      "paper/images/simulated_tradeoff.png",
      width = 8,
      height = 5
    )
  },

  figure_tradeoff_tern = simulated_tradeoff_tern %>%
    plot_tradeoff_tern_all() %>%
    save_png(
      "paper/images/tradeoff_all.png",
      width = 8,
      height = 16
    ),
  # figure_tradeoff_tern_algo = simulated_tradeoff_tern %>%
  #   plot_tradeoff_tern_algo() %>%
  #   save_png(
  #     "paper/images/tradeoff_algo.png",
  #     width = 8,
  #     height = 16
  #   ),


  ######################################################################
  # Insertions performance
  data_insertions = {
    file_in("temporal-index-results.sqlite")
    table_insertions()
  },

  figure_insertions = data_insertions %>%
    plot_insertions() %>%
    save_png(
      "paper/images/insertions.png",
      width = 8,
      height = 4
    ),


  ######################################################################
  # Running example to be used in the paper
  query_range =
    interval(
      ymd("2016-06-15"),
      ymd("2016-07-15")
    ),
  query_duration = c(5, 13),

  data_running_example = table_running_mimic(
    query_range, query_duration
  ),

  figure_running_example = {
    tikzDevice::tikz(
      file = file_out("paper/example.tex"),
      width = 3.3, height = 1.0
    )
    print(plot_running_example(
      data_running_example, query_range, query_duration
    ))
    dev.off()
  },

  figure_running_example_plane = {
    plot_running_example_mimic(
      query_range, query_duration
    )
    ggsave(file_out("paper/images/example-plane.pdf"), width = 3.3, height = 2.5, dpi = 300)
  },
  figure_running_example_grid = {
    plot_running_example_mimic(
      query_range, query_duration,
      grid = TRUE
    )
    ggsave(file_out("paper/images/example-grid.pdf"), width = 3.3, height = 2.5, dpi = 300)
  },
)