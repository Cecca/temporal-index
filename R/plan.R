db_file <- here("temporal-index-results.sqlite")
conn <- dbConnect(RSQLite::SQLite(), db_file)

table_main <- function(connection, path) {
  tbl(connection, "main")
}

plan <- drake_plan(
  data = table_main(conn, file_in("temporal-index-results.sqlite")) %>% 
    filter(
      hostname == "ironmaiden"
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
    ) %>%
    select(-time_query_ms, -time_index_ms)
    ,

  query_stats = tbl(conn, "query_stats") %>%
    collect() %>%
    inner_join(data %>% select(sha, dataset_n, dataset_max_time, algorithm, algorithm_wpar)) %>%
    mutate(query_time = set_units(query_time_ms, "ms")) %>%
    select(-query_time_ms)
  ,

  plot_one_million = data %>%
    filter(
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-uniform-uniform"
    ) %>%
    filter(dataset_n == 1000000,
           dataset_max_time == 100000,
           queryset_max_time == 100000) %>%
    mutate(algorithm_wpar = fct_reorder(algorithm_wpar, qps)) %>%
    barchart_qps(),

  plot_conference = data %>%
    filter(
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-uniform-uniform"
    ) %>%
    filter(dataset_n == 1000000,
          dataset_max_time == 1000,
          queryset_max_time == 1000) %>%
    mutate(algorithm_wpar = fct_reorder(algorithm_wpar, qps)) %>%
    barchart_qps(),

  plot_range_only = data %>%
    filter(
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-None",
      hostname == "ironmaiden"
    ) %>%
    barchart_qps(),

  plot_duration_only = data %>%
    filter(
      dataset == "random-uniform-zipf",
      queryset == "random-None-uniform-uniform",
      hostname == "ironmaiden"
    ) %>%
    barchart_qps(),

  query_plot = query_stats %>%
    mutate(query_time = set_units(query_time, "us")) %>%
    filter(query_count > 0) %>%
    ggplot(aes(x=algorithm_wpar, y=query_time / query_count)) +
    facet_grid(vars(dataset_max_time), 
               vars(dataset_n), 
               scales="free") +
    # geom_point() +
    geom_tufteboxplot() +
    scale_y_unit() +
    coord_flip() +
    theme_bw()
  ,

  report = rmarkdown::render(
    knitr_in("R/report.Rmd"),
    output_file = file_out("report.html"),
    output_dir = "."
  ),

)
