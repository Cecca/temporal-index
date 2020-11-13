db_file <- here("temporal-index-results.sqlite")
conn <- dbConnect(RSQLite::SQLite(), db_file)

install_symbolic_unit("records")

real_sizes <- tribble(
  ~dataset, ~dataset_n,
  "Flight",        684838,
  "Tourism",       835071,
  "Webkit",       1547419,
)

plan <- drake_plan(
  data = table_main(conn, file_in("temporal-index-results.sqlite")) %>% 
    as_tibble() %>%
    filter(
      hostname == "ironmaiden",
      algorithm != "ebi-index",
      algorithm != "linear-scan",
      algorithm != "grid",
      algorithm != "grid3D",
      algorithm != "NestedVecs",
      algorithm != "NestedBTree",
      algorithm != "period-index-old-*",
      !str_detect(queryset, "Mixed"),
    ) %>%
    mutate(
      date = parse_datetime(date),
      is_estimate = is_estimate > 0
    ) %>%
    group_by(dataset, dataset_version, dataset_params, queryset, queryset_version, queryset_params, algorithm, algorithm_version, algorithm_params) %>%
    slice(which.max(date)) %>%
    ungroup() %>%
    mutate(
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[,2]),
      dataset_n = as.integer(str_match(dataset_params, "n=(\\d+)")[,2]),
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      total_time = time_index + time_queries,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, desc(time_queries)),
      qps = queryset_n / set_units(time_queries, "s")
      # output_throughput = output_throughput_ints_ns %>%
      #   set_units("records/ns") %>% set_units("records/s")
    ) %>%
    mutate(
      workload_type = case_when(
        queryset == "random-uniform-zipf-uniform-uniform" ~ "both",
        queryset == "random-clustered-zipf-uniform-uniform" ~ "both",
        queryset == "random-None-uniform-uniform" ~ "duration",
        queryset == "random-uniform-zipf-None" ~ "time",
        queryset == "random-clustered-zipf-None" ~ "time",
        queryset == "Mixed" ~ "mixed",
        TRUE ~ "Unknown"
      )
    ) %>%
    mutate(start_times_distribution = if_else(dataset == "random-uniform-zipf", "uniform", "clustered")) %>%
    filter(dataset_n == 10000000, queryset_n == 5000) %>%
    select(-time_query_ms, -time_index_ms)
    ,

  data_real = table_main(conn, file_in("temporal-index-results.sqlite")) %>% 
    as_tibble() %>%
    filter(
      hostname == "ironmaiden",
      dataset %in% c("Tourism", "Flight", "Webkit")
    ) %>%
    mutate(
      date = parse_datetime(date),
      is_estimate = is_estimate > 0
    ) %>%
    group_by(dataset, dataset_version, dataset_params, queryset, queryset_version, queryset_params, algorithm, algorithm_version, algorithm_params) %>%
    slice(which.max(date)) %>%
    ungroup() %>%
    inner_join(real_sizes) %>%
    mutate(
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[,2]),
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      total_time = time_index + time_queries,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, desc(time_queries)),
      qps = queryset_n / set_units(time_queries, "s")
    ) %>%
    mutate(
      workload_type = case_when(
        str_detect("random-granules-uniform-uniform-uniform", queryset) ~ "both",
        TRUE ~ "Unknown"
      )
    ) %>%
    select(-time_query_ms, -time_index_ms)
    ,
  
  queries = target(
    table_query_stats(conn, file_in("temporal-index-results.sqlite")) %>%
      mutate(precision = as.double(query_count) / as.double(query_examined)) %>%
      as.data.table(),
    format = "fst_dt"
  ),

  best = target(
    data %>%
      bind_rows(data_real) %>%
      lazy_dt() %>%
      group_by(dataset, dataset_params, queryset, queryset_params, algorithm) %>%
      slice(which.max(qps)) %>%
      ungroup() %>%
      as.data.table(),
    format = "fst_dt"
  ),

  best_csv = best %>%
    write_csv(file_out("docs/best.csv"))
  ,

  # query_stats = target(
  #   {
  #     base_data <- best %>%
  #       lazy_dt() %>%
  #       filter(dataset_params %in% c("seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1", "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1")) %>%
  #       inner_join(queries) 
      
  #     total_times <- base_data %>%
  #       group_by(start_times_distribution, algorithm, workload_type) %>%
  #       summarise(total_query_time = sum(query_time_ns))

  #     ranking <- base_data %>%
  #       group_by(start_times_distribution, algorithm, workload_type) %>%
  #       arrange(query_time_ns) %>%
  #       mutate(
  #         group_rank = ntile(query_time_ns, 50)
  #       ) %>%
  #       group_by(start_times_distribution, algorithm, workload_type, group_rank) %>%
  #       summarise(group_query_time = sum(query_time_ns))

  #     inner_join(ranking, total_times) %>%
  #       mutate(fraction_total_query_time = group_query_time / total_query_time) %>%
  #       as.data.table()
  #   },
  #   format = "fst_dt",
  # ),

  real_data_start_times = get_histograms("dataset-start-times", "./experiments/real-world.yml"),
  real_data_durations = get_histograms("dataset-durations", "./experiments/real-world.yml"),

  real_data_distributions = {
    p1 <- ggplot(real_data_start_times, aes(x=value, weight=count)) +
      geom_histogram() +
      facet_wrap(vars(name), scales="free") +
      labs(x="start time") +
      theme_tufte() +
      theme(axis.title.y=element_blank())
    p2 <- ggplot(real_data_durations, aes(x=value, weight=count)) +
      geom_histogram() +
      facet_wrap(vars(name), scales="free") +
      labs(x="duration") +
      theme_tufte() +
      theme(strip.text=element_blank(),
            axis.title.y=element_blank())
    save_png(
      plot_grid(p1, p2, ncol=1),
      filename="paper/images/real-distributions.png",
      width=10,
      height=3
    )
  },

  querystats_plot = {
    p <- data %>% 
      lazy_dt() %>%
      mutate(
        workload_type = case_when(
          queryset == "random-uniform-zipf-uniform-uniform" ~ "both",
          queryset == "random-clustered-zipf-uniform-uniform" ~ "both",
          queryset == "random-None-uniform-uniform" ~ "duration",
          queryset == "random-uniform-zipf-None" ~ "time",
          queryset == "random-clustered-zipf-None" ~ "time",
          queryset == "Mixed" ~ "mixed",
          TRUE ~ "Unknown"
        )
      ) %>%
      filter(dataset_params %in% c("seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1", "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1")) %>%
      group_by(dataset, dataset_params, queryset, queryset_params, algorithm) %>%
      slice(which.max(qps)) %>%
      ungroup() %>%
      mutate(start_times_distribution = if_else(dataset == "random-uniform-zipf", "uniform", "clustered")) %>%
      inner_join(queries) %>%
      plot_distribution_all()
    save_png(p, "paper/images/querytimes-distribution.png",
             width=9, height=4)
  },

  data_selectivity_vs_queries = lazy_dt(best) %>%
      mutate(
        workload_type = case_when(
          queryset == "random-uniform-zipf-uniform-uniform" ~ "both",
          queryset == "random-clustered-zipf-uniform-uniform" ~ "both",
          queryset == "random-None-uniform-uniform" ~ "duration",
          queryset == "random-uniform-zipf-None" ~ "time",
          queryset == "random-clustered-zipf-None" ~ "time",
          queryset == "Mixed" ~ "mixed",
          TRUE ~ "Unknown"
        )
      ) %>%
      mutate(start_times_distribution = if_else(dataset == "random-uniform-zipf", "uniform", "clustered")) %>%
      filter(dataset_params %in% c("seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1", "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1")) %>%
      inner_join(lazy_dt(queries)) %>%
      as_tibble() %>%
      mutate(
        query_time = set_units(as.double(query_time_ns), "ns") %>% set_units("ms"),
        selectivity = query_count / dataset_n
      ) %>%
      filter(if_else(algorithm == "interval-tree", workload_type != "duration", TRUE)) %>%
      filter(selectivity <= .2) %>%
      group_by(algorithm, start_times_distribution) %>%
      filter(row_number(query_time) < n() - 100),

  plot_selectivity_vs_query_time = {
    p <- data_selectivity_vs_queries %>%
      ggplot(aes(x=selectivity, y=drop_units(query_time), color=workload_type)) +
      geom_point(size=.1, alpha=0.5) +
      facet_grid(vars(start_times_distribution), vars(algorithm), scales="fixed") +
      scale_y_continuous() +
      scale_x_continuous(breaks=c(0,0.1,0.2), labels=scales::number_format(accuracy=0.1)) +
      scale_color_workload() +
      guides(colour = guide_legend(override.aes = list(size=5, alpha=1))) +
      labs(x="selectivity",
           y="query time (ms)",
           color="query type") +
      theme_bw()

    save_png(p, file_out("paper/images/selectivity_vs_time.png"),
             width=10, height=4)
  },

  scalability_data = table_main(conn, file_in("temporal-index-results.sqlite")) %>%
    filter(
      hostname == "ironmaiden",
      algorithm != "ebi-index",
      algorithm != "linear-scan",
      algorithm != "period-index-old-*",
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-uniform-uniform",
      queryset_params == "seed=23512 n=5000 start_low=1 start_high=1000000000 dur_n=1000000000 dur_beta=1 durmin_low=1 durmin_high=10000 durmax_low=1 durmax_high=10000"
    ) %>%
    collect() %>%
    mutate(
      date = parse_datetime(date),
      is_estimate = is_estimate > 0
    ) %>%
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
      qps = queryset_n / set_units(time_queries, "s")
      # output_throughput = output_throughput_ints_ns %>%
      #   set_units("records/ns") %>% set_units("records/s")
    ) %>%
    select(-time_query_ms, -time_index_ms) %>%
    group_by(dataset, dataset_params, queryset, queryset_params, algorithm) %>% 
    ungroup() %>%
    get_params(dataset_params, "d_")
    ,

  plot_scalability = {
    p <- scalability_data %>% 
      group_by(algorithm, dataset, dataset_params, queryset, queryset_params) %>%
      slice(which.max(qps)) %>%
      ggplot(aes(x=d_n, y=drop_units(qps), color=algorithm)) +
        geom_point() +
        geom_line() +
        geom_rangeframe(color="black") +
        scale_x_log10(labels=scales::number_format()) +
        scale_y_log10(labels=scales::number_format()) +
        scale_fill_algorithm() +
        theme_tufte()
    save_png(p, file_out("paper/images/scalability.png"),
             width=10, height=4)
  },

  best_qps = data %>% 
    group_by(dataset, dataset_params, queryset, queryset_params, algorithm) %>% 
    slice(which.max(qps)) %>%
    ungroup(),

  historical_variations = table_history(conn, file_in("temporal-index-results.sqlite")) %>%
    group_by(algorithm, workload) %>% 
    arrange(algorithm_version, date) %>% 
    transmute(algorithm_version, date, time_query_ms, diff = time_query_ms - lag(time_query_ms), variation = time_query_ms / lag(time_query_ms)) %>% 
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

  overview_qps = {
    p <- plot_overview2(data, qps, n_bins=60, xlab="queries per second")
    save_png(p, file_out("paper/images/overview-qps.png"),
             width=8, height=5)
    girafe(
      ggobj=p, 
      width_svg=10,
      height_svg=6,
      options = list(
        opts_hover(css = "r:4; opacity: 1.0;"),
        opts_hover_inv(css = "opacity: 0.2;")
      ) 
    )
  },

  data_algo_param_dep = table_main(conn, file_in("temporal-index-results.sqlite")) %>%
    as_tibble() %>%
    filter(
      algorithm == "period-index++",
      !(dataset %in% c("Flight", "Webkit", "Tourism"))
    ) %>%
    mutate(
      date = parse_datetime(date),
      is_estimate = is_estimate > 0
    ) %>%
    mutate(
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[,2]),
      dataset_n = as.integer(str_match(dataset_params, "n=(\\d+)")[,2]),
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      total_time = time_index + time_queries,
      qps = queryset_n / set_units(time_queries, "s")
    ) %>%
    get_params(queryset_params, "q_") %>%
    filter(
      queryset_n == 20000, 
      dataset_n %in% c(10000000),
      (q_durmin_high == 10000 | is.na(q_durmin_high)),
      (q_durmax_high == 10000 | is.na(q_durmax_high)),
      (q_start_high == 10000000 | is.na(q_start_high))
    ) %>%
    mutate(
      workload_type = case_when(
        queryset == "random-uniform-zipf-uniform-uniform" ~ "both",
        queryset == "random-None-uniform-uniform" ~ "duration",
        queryset == "random-uniform-zipf-None" ~ "time",
        TRUE ~ "Unknown"
      )
    ) %>%
    mutate(start_times_distribution = if_else(dataset == "random-uniform-zipf", "uniform", "clustered")) %>%
    get_params(algorithm_params, ""),

  
  plot_algo_param_dep = (ggplot(data_algo_param_dep, 
        aes(x=page_size, 
            y=drop_units(qps),
            color=workload_type)) +
      geom_point() +
      geom_line() +
      scale_x_continuous(trans="log10", limits=c(10,NA)) +
      scale_y_continuous(trans="log10", limits=c(NA,NA)) +
      scale_color_workload() +
      facet_wrap(vars(start_times_distribution), scales="fixed") +
      labs(x="page size",
           y="queries per second",
           color="workload") +
      theme_bw() +
      theme(legend.position='bottom',
            legend.direction='horizontal')) %>%
      save_png("paper/images/param_dependency.png",
               width=5, height=3),

  best_latex = best %>% 
    as_tibble() %>% 
    inner_join(queryset_labels) %>%
    inner_join(dataset_labels) %>%
    get_params(queryset_params, "q_") %>%
    select(qps, algorithm, dataset_label, query_interval_label, query_duration_label) %>%
    mutate(algorithm = case_when(
             algorithm == "period-index++" ~ "PI++",
             algorithm == "BTree" ~ "BT",
             algorithm == "grid-file" ~ "GF",
             algorithm == "period-index-*" ~ "PI*",
             algorithm == "interval-tree" ~ "IT",
             TRUE ~ algorithm
           ),
           algorithm = fct_reorder(algorithm, qps)) %>%
    arrange(desc(algorithm)) %>%
    group_by(dataset_label, query_interval_label, query_duration_label) %>%
    mutate(query_str = str_c("$(", query_interval_label, ", ", query_duration_label, ")$"),
           dataset_label = str_c("$", dataset_label, "$"),
           qps_num = qps,
           qps = scales::number(qps, big.mark="\\,"),
           qps = if_else(qps_num == max(qps_num), str_c("\\underline{",qps,"}"), qps)) %>%
    ungroup() %>%
    select(-qps_num, -query_interval_label, -query_duration_label) %>%
    pivot_wider(names_from="algorithm", values_from="qps") %>%
    arrange(dataset_label, query_str) %>%
    rename(dataset = dataset_label, queries = query_str) %>%
    (function(d) {print(d, n=100); d}) %>%
    kable("latex", escape=F, booktabs=T, align="r") %>%
    kable_styling(position = "center",
                  font_size = 8) %>%
    collapse_rows(columns=1, latex_hline = "major") %>%
    write_file("paper/qps.tex")
  ,

  dataset_labels = best %>%
    as_tibble() %>%
    distinct(dataset, dataset_params) %>%
    mutate(dataset_label = case_when(
      (dataset_params == "seed=123 n=10000000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1") ~
        "R_1",
      (dataset_params == "seed=123 n=10000000 start_n=10 start_high=10000000 start_stddev=100000 dur_n=10000000 dur_beta=1") ~
        "R_2",
      TRUE ~ dataset
    )) %>%
    drop_na() %>%
    select(dataset_label, dataset_params)
    ,

  queryset_labels = best %>%
    as_tibble() %>%
    distinct(queryset, queryset_params) %>%
    get_params(queryset_params, "") %>%
    mutate(query_duration_label = case_when(
      str_detect(queryset, "granule") ~ "real",
      (durmin_low==1 & durmin_high==100 & durmax_low==1 & durmax_high==100) ~ "D_1",
      (durmin_low==1 & durmin_high==10000 & durmax_low==1 & durmax_high==100) ~ "D_2",
      (durmin_low==1 & durmin_high==100 & durmax_low==1 & durmax_high==10000) ~ "D_3",
      (durmin_low==1 & durmin_high==10000 & durmax_low==1 & durmax_high==10000) ~ "D_4",
      (is.na(durmin_low)) ~ "nil"
    )) %>%
    mutate(query_interval_label = case_when(
      (is.na(start_n) & start_low==1 & start_high==10000000 & dur_n==10000000 & dur_beta==1) ~ "R_1",
      (start_n==10 & start_stddev==100000 & start_high==10000000 & dur_n==10000000 & dur_beta==1) ~ "R_2",
      (str_detect(queryset, "-None-")) ~ "nil"
    )) %>%
    select(queryset_params, query_interval_label, query_duration_label) %>%
    drop_na()
  ,

)
