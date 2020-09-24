db_file <- here("temporal-index-results.sqlite")
conn <- dbConnect(RSQLite::SQLite(), db_file)

install_symbolic_unit("records")

plan <- drake_plan(
  data = table_main(conn, file_in("temporal-index-results.sqlite")) %>% 
    filter(
      hostname == "ironmaiden",
      algorithm != "ebi-index",
      algorithm != "linear-scan",
    ) %>%
    collect() %>%
    mutate(date = parse_datetime(date)) %>%
    group_by(dataset, dataset_version, dataset_params, queryset, queryset_version, queryset_params, algorithm, algorithm_version, algorithm_params) %>%
    slice(which.max(date)) %>%
    ungroup() %>%
    mutate(
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[,2]),
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      total_time = time_index + time_queries,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, desc(time_queries)),
      qps = queryset_n / set_units(time_queries, "s"),
      output_throughput = output_throughput_ints_ns %>%
        set_units("records/ns") %>% set_units("records/s")
    ) %>%
    select(-time_query_ms, -time_index_ms)
    ,

  best_qps = data %>% 
    group_by(dataset, dataset_params, queryset, queryset_params, algorithm) %>% 
    slice(which.max(qps)) %>%
    ungroup(),

  d3overview = r2d3::r2d3(data=best_qps, script=file_in("js/overview.js")),
  
  historical_variations = table_history(conn, file_in("temporal-index-results.sqlite")) %>%
    group_by(algorithm, workload) %>% 
    arrange(algorithm_version, date) %>% 
    transmute(algorithm_version, date, variation = time_query_ms / lag(time_query_ms)) %>% 
    ungroup() %>%
    arrange(algorithm, workload, algorithm_version, date)
  ,

  regressions = historical_variations %>% filter(variation > 1),
  regressions_latest = historical_variations %>%
    group_by(algorithm) %>%
    filter(algorithm_version == max(algorithm_version)) %>%
    ungroup() %>%
    filter(variation > 1),

  latest_change = {
    mingroup <- historical_variations %>%
      group_by(algorithm) %>%
      filter(algorithm_version == max(algorithm_version)) %>%
      slice(which.min(abs(variation - 1))) %>%
      transmute(algorithm_version, min_variation = variation) %>%
      ungroup()

    maxgroup <- historical_variations %>%
      group_by(algorithm) %>%
      filter(algorithm_version == max(algorithm_version)) %>%
      slice(which.max(abs(variation - 1))) %>%
      transmute(algorithm_version, max_variation = variation) %>%
      ungroup()
    inner_join(mingroup, maxgroup)
  },

  query_stats_both = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-uniform-zipf",
                                       dataset_params_val = "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
                                       queryset_val = "random-uniform-zipf-uniform-uniform",
                                       queryset_params_val = "seed=23512 n=5000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 durmin_low=1 durmin_high=100 durmax_low=1 durmax_high=100"),
  query_stats_range = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-uniform-zipf",
                                       dataset_params_val = "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
                                       queryset_val = "random-uniform-zipf-None",
                                       queryset_params_val = "seed=23512 n=5000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 "),
  query_stats_duration = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-uniform-zipf",
                                       dataset_params_val = "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
                                       queryset_val = "random-None-uniform-uniform",
                                       queryset_params_val = "seed=23512 n=5000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 durmin_low=1 durmin_high=100 durmax_low=1 durmax_high=100"),

  query_stats_both_clustered = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-clustered-zipf",
                                       dataset_params_val = "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1",
                                       queryset_val = "random-uniform-zipf-uniform-uniform",
                                       queryset_params_val = "seed=23512 n=5000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 durmin_low=1 durmin_high=100 durmax_low=1 durmax_high=100"),
  query_stats_range_clustered = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-clustered-zipf",
                                       dataset_params_val = "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1",
                                       queryset_val = "random-uniform-zipf-None",
                                       queryset_params_val = "seed=23512 n=5000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 "),
  query_stats_duration_clustered = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-clustered-zipf",
                                       dataset_params_val = "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1",
                                       queryset_val = "random-None-uniform-uniform",
                                       queryset_params_val = "seed=23512 n=5000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 durmin_low=1 durmin_high=100 durmax_low=1 durmax_high=100"),

  plot_latency_both = query_stats_both %>% distribution_latency(),
  plot_normalized_latency_both = query_stats_both %>% distribution_normalized_latency(),
  plots_latencies_both = {
    p <- plot_grid(plot_latency_both, plot_normalized_latency_both)
    save_png(p, "imgs/latencies_both.png")
  },

  plot_latency_both_clustered = query_stats_both_clustered %>% distribution_latency(),
  plot_normalized_latency_both_clustered = query_stats_both_clustered %>% distribution_normalized_latency(),
  plots_latencies_both_clustered = {
    p <- plot_grid(plot_latency_both_clustered, plot_normalized_latency_both_clustered)
    save_png(p, "imgs/latencies_both_clustered.png")
  },

  # plot_latency_range = query_stats_range %>% distribution_latency(),
  # plot_normalized_latency_range = query_stats_range %>% distribution_normalized_latency(),
  # # plot_overhead_range = query_stats_range %>% distribution_overhead(),

  # plot_latency_duration = query_stats_duration %>% distribution_latency(),
  # plot_normalized_latency_duration = query_stats_duration %>% distribution_normalized_latency(),
  # # plot_overhead_duration = query_stats_duration %>% distribution_overhead(),

  # period_index_buckets = table_period_index_buckets(conn, file_in("temporal-index-results.sqlite"),
  #                                                   dataset_val = "random-uniform-zipf",
  #                                                   dataset_params_val = "123:10000000_1:10000000_10000000:1",
  #                                                   queryset_val = "random-uniform-zipf-uniform-uniform",
  #                                                   queryset_params_val = "23512:5000_1:10000000_10000000:1_1:100_1:100"),

  plot_both_uniform = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-uniform-zipf",
      dataset_params == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
      queryset == "random-uniform-zipf-uniform-uniform",
    ) %>%
    get_params(queryset_params, "q_") %>%
    mutate(workload = interaction(q_durmin_low, q_durmin_high, q_durmax_low, q_durmax_high)) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_both_uniform.png")),

  plot_range_only_uniform = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-uniform-zipf",
      dataset_params == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
      queryset == "random-uniform-zipf-None",
    ) %>%
    get_params(queryset_params, "q_") %>%
    inline_print() %>%
    mutate(workload = interaction(q_start_low, q_start_high, q_dur_n, q_dur_beta)) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_time_only_uniform.png")),

  plot_duration_only_uniform = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-uniform-zipf",
     dataset_params == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
      queryset == "random-None-uniform-uniform",
    ) %>%
    get_params(queryset_params, "q_") %>%
    mutate(workload = interaction(q_durmin_low, q_durmin_high, q_durmax_low, q_durmax_high)) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_duration_only_uniform.png")),

  plot_both_clustered = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-clustered-zipf",
      dataset_params == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1",
      queryset == "random-uniform-zipf-uniform-uniform",
    ) %>%
    get_params(queryset_params, "q_") %>%
    mutate(workload = interaction(q_durmin_low, q_durmin_high, q_durmax_low, q_durmax_high)) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_both_clustered.png")),

  plot_range_only_clustered = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-clustered-zipf",
      dataset_params == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1",
      queryset == "random-uniform-zipf-None",
    ) %>%
    get_params(queryset_params, "q_") %>%
    mutate(workload = interaction(q_start_low, q_start_high, q_dur_n, q_dur_beta)) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_time_only_clustered.png")),

  plot_duration_only_clustered = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-clustered-zipf",
      dataset_params == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1",
      queryset == "random-None-uniform-uniform",
    ) %>%
    get_params(queryset_params, "q_") %>%
    mutate(workload = interaction(q_durmin_low, q_durmin_high, q_durmax_low, q_durmax_high)) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_duration_only_clustered.png")),

  ## Uniform distribution of start times
  distribution_start_time = get_histograms("dataset-start-times", "experiments/all.yml") %>%
    filter(parameters == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1", name == "random-uniform-zipf"),
  distribution_duration = get_histograms("dataset-durations", "experiments/all.yml") %>%
    filter(parameters == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1", name == "random-uniform-zipf"),
  dataset_intervals = get_dump("dataset", "experiments/all.yml") %>%
    filter(parameters == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1", name == "random-uniform-zipf"),
  plot_dataset_intervals = draw_dataset(dataset_intervals),
  plot_distribution_start_time = plot_histogram(distribution_start_time, "start time"),
  plot_distribution_duration = plot_point_distribution(distribution_duration, "duration"),
  plot_distribution = cowplot::plot_grid(
      plot_distribution_start_time,
      plot_distribution_duration,
      plot_dataset_intervals,
      ncol=3
    ) %>%
    save_png("imgs/distribution_plots.png", width=10, height=4),

  ## Clustered distribution of start times
  distribution_start_time_clustered = get_histograms("dataset-start-times", "experiments/all.yml") %>%
    filter(parameters == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1"),
  distribution_duration_clustered = get_histograms("dataset-durations", "experiments/all.yml") %>%
    filter(parameters == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1"),
  dataset_intervals_clustered = get_dump("dataset", "experiments/all.yml") %>%
    filter(parameters == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1"),
  plot_dataset_intervals_clustered = draw_dataset(dataset_intervals_clustered),
  plot_distribution_start_time_clustered = plot_histogram(distribution_start_time_clustered, "start time"),
  plot_distribution_duration_clustered = plot_point_distribution(distribution_duration_clustered, "duration"),
  plot_distribution_clustered = cowplot::plot_grid(
      plot_distribution_start_time_clustered,
      plot_distribution_duration_clustered,
      plot_dataset_intervals_clustered,
      ncol=3
    ) %>%
    save_png("imgs/distribution_plots_clustered.png", width=10, height=4),

  overview_qps = {
    p <- plot_overview2(data, qps, n_bins=60, xlab="queries per second")
    save_png(p, file_out("imgs/overview-qps.png"))
    girafe(
      ggobj=p, 
      width_svg=10,
      height_svg=6,
      options = list(
        opts_hover(css = "r:4; stroke: black;"),
        opts_hover_inv(css = "opacity: 0.2;")
      ) 
    )
  },


  overview_output_throughput = {
    p <- plot_overview2(data, output_throughput, xlab="output throughput (records/s)")
    save_png(p, file_out("imgs/overview-output-thoughput.png"))
    girafe(
      ggobj=p, 
      width_svg=10,
      height_svg=6,
      options = list(
        opts_hover(css = "r:4; stroke: black;"),
        opts_hover_inv(css = "opacity: 0.2;")
      ) 
    )
  },

  report = rmarkdown::render(
    knitr_in("R/report.Rmd"),
    output_file = file_out("report.html"),
    output_dir = "."
  ),

)
