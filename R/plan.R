db_file <- here("temporal-index-results.sqlite")
conn <- dbConnect(RSQLite::SQLite(), db_file)

table_main <- function(path) {
  tbl(conn, "main")
}

plan <- drake_plan(
  data = table_main(file_in("temporal-index-results.sqlite")) %>% 
    filter(
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-uniform-uniform",
    ) %>%
    collect() %>%
    separate(dataset_params, into=str_c("dataset_", c("seed", "n", "min_time", "max_time", "zipf_n", "exponent")), convert = T) %>%
    separate(queryset_params, into=str_c("queryset_", c("seed", "n", "min_time", "max_time", "zipf_n", "exponent", "min_duration", "max_duration")), convert = T) %>%
    mutate(
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      total_time = time_index + time_queries,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, time_queries),
      qps = queryset_n / set_units(time_queries, "s"),
    ) %>%
    select(-time_query_ms, -time_index_ms)
    ,

  data_range_only = table_main(file_in("temporal-index-results.sqlite")) %>% 
    filter(
      dataset == "random-uniform-zipf",
      queryset == "random-uniform-zipf-None",
    ) %>%
    collect() %>%
    separate(dataset_params, into=str_c("dataset_", c("seed", "n", "min_time", "max_time", "zipf_n", "exponent")), convert = T) %>%
    separate(queryset_params, into=str_c("queryset_", c("seed", "n", "min_time", "max_time", "zipf_n", "exponent", "min_duration", "max_duration")), convert = T) %>%
    mutate(
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      total_time = time_index + time_queries,
      algorithm_wpar = interaction(algorithm, algorithm_params),
      algorithm_wpar = fct_reorder(algorithm_wpar, time_queries),
      qps = queryset_n / set_units(time_queries, "s"),
    ) %>%
    filter(dataset_n == 1000000,
           dataset_max_time == 100000,
           queryset_max_time == 100000) %>%
    select(-time_query_ms, -time_index_ms)
    ,

  query_stats = tbl(conn, "query_stats") %>%
    collect() %>%
    inner_join(data %>% select(sha, dataset_n, dataset_max_time, algorithm, algorithm_wpar)) %>%
    mutate(query_time = set_units(query_time_ms, "ms")) %>%
    select(-query_time_ms)
  ,

  data_one_million = data %>%
    filter(dataset_n == 1000000,
           dataset_max_time == 100000,
           queryset_max_time == 100000) %>%
    mutate(algorithm_wpar = fct_reorder(algorithm_wpar, desc(qps)))
  ,

  data_conference = data %>%
    filter(dataset_n == 1000000,
           dataset_max_time == 1000,
           queryset_max_time == 1000) %>%
    mutate(algorithm_wpar = fct_reorder(algorithm_wpar, desc(qps)))
  ,

  plot_one_million = ggplot(data_one_million, 
                            aes(x=algorithm_wpar, 
                                y=qps,
                                fill=algorithm)) +
    geom_col() +
    geom_hline(yintercept=pretty(pull(data_one_million, qps)), col="white", lwd=.5) +
    geom_text(aes(label=scales::number(drop_units(qps), 
                                       accuracy=1)),
              size=3,
              hjust=0,
              vjust=0.5,
              nudge_y=200) +
    scale_y_unit(breaks=pretty(pull(data_one_million, qps))) +
    scale_fill_discrete_qualitative() +
    coord_flip() +
    # facet_grid(vars(dataset_max_time), 
    #            vars(dataset_n), 
    #            scales="free") +
    theme_tufte() +
    theme(
      panel.ontop = FALSE,
      # panel.grid.major.x = element_line(color="white", size=.5),
      panel.grid.minor.x = element_blank(),
      panel.grid.major.y = element_blank(),
      panel.grid.minor.y = element_blank(),
      legend.position = "none",
      axis.title.y = element_blank()
    )
   ,

  # Replicate the workload of the conference
  plot_conference = ggplot(data_conference, 
                           aes(x=algorithm_wpar, 
                               y=qps,
                               fill=algorithm)) +
    geom_col() +
    geom_hline(yintercept=seq(0, 8000, 2000), col="white", lwd=.5) +
    geom_text(aes(label=scales::number(drop_units(qps), 
                                       accuracy=1)),
              size=3,
              hjust=0,
              vjust=0.5,
              nudge_y=200) +
    scale_fill_discrete_qualitative() +
    coord_flip() +
    # facet_grid(vars(dataset_max_time), 
    #            vars(dataset_n), 
    #            scales="free") +
    theme_tufte() +
    theme(
      panel.ontop = FALSE,
      # panel.grid.major.x = element_line(color="white", size=.5),
      panel.grid.minor.x = element_blank(),
      panel.grid.major.y = element_blank(),
      panel.grid.minor.y = element_blank(),
      legend.position = "none",
      axis.title.y = element_blank()
    )
   ,

  plot_range_only = ggplot(data_range_only, 
                           aes(x=algorithm_wpar, 
                               y=qps,
                               fill=algorithm)) +
    geom_col() +
    geom_hline(yintercept=pretty(pull(data_range_only, qps)), col="white", lwd=.5) +
    geom_text(aes(label=scales::number(drop_units(qps), 
                                       accuracy=1)),
              size=3,
              hjust=0,
              vjust=0.5,
              nudge_y=10) +
    scale_y_unit(breaks=pretty(pull(data_range_only, qps))) +
    scale_fill_discrete_qualitative() +
    coord_flip() +
    theme_tufte() +
    theme(
      panel.ontop = FALSE,
      panel.grid.minor.x = element_blank(),
      panel.grid.major.y = element_blank(),
      panel.grid.minor.y = element_blank(),
      legend.position = "none",
      axis.title.y = element_blank()
    )
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
  ,

  report = rmarkdown::render(
    knitr_in("R/report.Rmd"),
    output_file = file_out("report.html"),
    output_dir = "."
  ),

)