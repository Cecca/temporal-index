conn <- dbConnect(RSQLite::SQLite(), here("temporal-index-results.sqlite"))

plan <- drake_plan(
  data = tbl(conn, "main") %>% 
    filter(
      dataset == "zipf-and-uniform",
      queryset == "zipf-and-uniform",
    ) %>%
    collect() %>%
    separate(dataset_params, into=str_c("dataset_", c("seed", "n", "exponent", "max_time")), convert = T) %>%
    filter(dataset_max_time == 10000000) %>%
    mutate(
      total_time = time_index_ms + time_query_ms,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, total_time)
    ),
    # separate(queryset_params, into=str_c("queryset_", c("seed", "n", "exponent", "max_time", "max_duration_factor")))

  query_stats = tbl(conn, "query_stats") %>%
    collect() %>%
    inner_join(data %>% select(sha, dataset_n, dataset_max_time, algorithm, algorithm_wpar)) %>%
    mutate(query_time = set_units(query_time_ms, "ms")) %>%
    select(-query_time_ms)
  ,

  plot = ggplot(data, aes(x=algorithm_wpar, 
                          y=time_query_ms,
                          fill=algorithm)) +
    geom_col() +
    geom_text(aes(label=time_query_ms),
              hjust=0,
              nudge_y=100) +
    coord_flip() +
    facet_grid(vars(dataset_max_time), 
               vars(dataset_n), 
               scales="free") +
    theme_bw()
   ,

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

)