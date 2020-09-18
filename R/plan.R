db_file <- here("temporal-index-results.sqlite")
conn <- dbConnect(RSQLite::SQLite(), db_file)

install_symbolic_unit("records")

plan <- drake_plan(
  data = table_main(conn, file_in("temporal-index-results.sqlite")) %>% 
    filter(
      hostname == "ironmaiden",
      algorithm != "ebi-index",
      algorithm != "period-index-*",
      algorithm != "linear-scan",
    ) %>%
    collect() %>%
    mutate(date = parse_datetime(date)) %>%
    group_by(dataset, dataset_version, dataset_params, queryset, queryset_version, queryset_params, algorithm, algorithm_version, algorithm_params) %>%
    slice(which.max(date)) %>%
    ungroup() %>%
    separate(dataset_params, into=str_c("dataset_", c("seed", "n", "min_time", "max_time", "zipf_n", "exponent")), convert = T, remove=F) %>%
    separate(queryset_params, into=str_c("queryset_", c("seed", "n", "min_time", "max_time", "zipf_n", "exponent", "min_duration", "max_duration")), convert = T, remove=F) %>%
    mutate(
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

  query_stats_both = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-uniform-zipf",
                                       dataset_params_val = "123:10000000_1:10000000_10000000:1",
                                       queryset_val = "random-uniform-zipf-uniform-uniform",
                                       queryset_params_val = "23512:5000_1:10000000_10000000:1_1:100_1:100"),
  query_stats_range = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-uniform-zipf",
                                       dataset_params_val = "123:10000000_1:10000000_10000000:1",
                                       queryset_val = "random-uniform-zipf-None",
                                       queryset_params_val = "23512:5000_1:10000000_10000000:1_NA_NA"),
  query_stats_duration = table_query_stats(conn, file_in("temporal-index-results.sqlite"),
                                       dataset_val = "random-uniform-zipf",
                                       dataset_params_val = "123:10000000_1:10000000_10000000:1",
                                       queryset_val = "random-None-uniform-uniform",
                                       queryset_params_val = "23512:5000_NA_NA_1:100_1:100"),

  plot_latency_both = query_stats_both %>% distribution_latency(),
  plot_normalized_latency_both = query_stats_both %>% distribution_normalized_latency(),
  plot_overhead_both = query_stats_both %>% distribution_overhead(),

  plot_latency_range = query_stats_range %>% distribution_latency(),
  plot_normalized_latency_range = query_stats_range %>% distribution_normalized_latency(),
  plot_overhead_range = query_stats_range %>% distribution_overhead(),

  plot_latency_duration = query_stats_duration %>% distribution_latency(),
  plot_normalized_latency_duration = query_stats_duration %>% distribution_normalized_latency(),
  plot_overhead_duration = query_stats_duration %>% distribution_overhead(),

  period_index_buckets = table_period_index_buckets(conn, file_in("temporal-index-results.sqlite"),
                                                    dataset_val = "random-uniform-zipf",
                                                    dataset_params_val = "123:10000000_1:10000000_10000000:1",
                                                    queryset_val = "random-uniform-zipf-uniform-uniform",
                                                    queryset_params_val = "23512:5000_1:10000000_10000000:1_1:100_1:100"),

  plot_both = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-uniform-zipf",
      dataset_params == "123:10000000_1:10000000_10000000:1",
      queryset == "random-uniform-zipf-uniform-uniform",
    ) %>%
    inner_join(workloads) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_both.png")),

  plot_range_only = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-None",
      dataset_params == "123:10000000_1:10000000_10000000:1",
      queryset_params == "23512:5000_1:10000000_10000000:1_NA_NA"
    ) %>%
    inner_join(workloads) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_time_only.png")),

  plot_duration_only = data %>%
    filter(
      hostname == "ironmaiden",
      dataset == "random-uniform-zipf",
      queryset == "random-None-uniform-uniform",
      dataset_params == "123:10000000_1:10000000_10000000:1",
      # queryset_params == "23512:5000_NA_NA_1:100_1:100"
    ) %>%
    inner_join(workloads) %>%
    barchart_qps() %>%
    save_png(file_out("imgs/qps_duration_only.png")),

  ## Uniform distribution of start times
  distribution_start_time = get_histograms("dataset-start-times", "experiments/all.yml") %>%
    filter(parameters == "123:10000000_1:10000000_10000000:1", name == "random-uniform-zipf"),
  distribution_duration = get_histograms("dataset-durations", "experiments/all.yml") %>%
    filter(parameters == "123:10000000_1:10000000_10000000:1", name == "random-uniform-zipf"),
  queries = get_dump("queries", "experiments/all.yml") %>%
    filter(parameters == "23512:5000_1:10000000_10000000:1_1:10000_1:10000", name == "random-uniform-zipf-uniform-uniform"),
  dataset_intervals = get_dump("dataset", "experiments/all.yml") %>%
    filter(parameters == "123:10000000_1:10000000_10000000:1", name == "random-uniform-zipf"),
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
    filter(parameters == "123:10000000_10:10000000:100000_10000000:1", name == "random-clustered-zipf"),
  distribution_duration_clustered = get_histograms("dataset-durations", "experiments/all.yml") %>%
    filter(parameters == "123:10000000_10:10000000:100000_10000000:1", name == "random-clustered-zipf"),
  dataset_intervals_clustered = get_dump("dataset", "experiments/all.yml") %>%
    filter(parameters == "123:10000000_10:10000000:100000_10000000:1", name == "random-clustered-zipf"),
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
    p <- plot_overview(data, qps, xlab="queries per second")
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
    p <- plot_overview(data, output_throughput, xlab="output throughput (records/s)")
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
