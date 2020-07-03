conn <- dbConnect(RSQLite::SQLite(), here("temporal-index-results.sqlite"))

plan <- drake_plan(
  data = tbl(conn, "main") %>% 
    filter(
      dataset == "zipf-and-uniform",
      queryset == "zipf-and-uniform",
      dataset_params == "123,10000000,1,1000"
    ) %>%
    collect() %>%
    mutate(
      total_time = time_index_ms + time_query_ms,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, total_time)
    ),
    # separate(dataset_params, into=str_c("dataset_", c("seed", "n", "exponent", "max_time"))) %>%
    # separate(queryset_params, into=str_c("queryset_", c("seed", "n", "exponent", "max_time", "max_duration_factor")))

  plot = ggplot(data, aes(x=algorithm_wpar, 
                          y=time_index_ms + time_query_ms,
                          fill=algorithm)) +
    geom_col() +
    coord_flip() +
    theme_bw(),

)