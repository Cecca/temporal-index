library(tidyverse)
library(RSQLite)
library(DBI)
library(drake)


parallel_plan <- drake_plan(

  data = target({
    con <- dbConnect(SQLite(), "temporal-index-results-parallel.sqlite")
    res <- tbl(con, "batch") |>
      collect() |>
      filter(
        conf_file == "experiments/parallel.yml",
        !str_detect(dataset_name, "Tourism"),
        !str_starts(dataset_name, "reiterated"),
        !str_detect(dataset_params, "n=100000000"),
        algorithm_name == "rd-index"
      ) |>
      mutate(
        dataset_name = case_when(
          dataset_name == "random-uniform-zipf" ~ "Random",
          T ~ dataset_name
        ),
        dataset_name = factor(dataset_name, levels=c("Random", "Flight", "Webkit", "MimicIII"), ordered=T)
      ) |>
      # select(dataset_name, algorithm_params, num_threads, time_index_ms) |>
      arrange(dataset_name, algorithm_params, num_threads)

    selector <- res |>
      filter(num_threads == 16) |>
      distinct(dataset_name, algorithm_params)
    
    res <- semi_join(res, selector) |>
      filter(num_threads <= 16)

    res |>
      distinct(dataset_name, dataset_params) |>
      print()

    dbDisconnect(con)
    res
  }),

  data_pquery = target({
    con <- dbConnect(SQLite(), "temporal-index-results-parallel.sqlite")
    res <- tbl(con, "parallel") |>
      collect() |>
      filter(
        !str_detect(dataset_name, "Tourism"),
        !str_starts(dataset_name, "reiterated"),
        !str_detect(dataset_params, "n=100000000"),
        algorithm_name == "rd-index"
      ) |>
      mutate(
        dataset_name = case_when(
          dataset_name == "random-uniform-zipf" ~ "Random",
          T ~ dataset_name
        ),
        dataset_name = factor(dataset_name, levels=c("Random", "Flight", "Webkit", "MimicIII"), ordered=T)
      ) |>
      filter(num_threads <= 16) |>
      arrange(dataset_name, algorithm_params, num_threads)

    best <- res |>
      filter(num_threads == 1) |>
      group_by(dataset_name) |>
      slice_max(qps) |>
      select(dataset_name, algorithm_name, algorithm_params) |>
      print()

    res <- semi_join(res, best)
    res |>
      select(dataset_name, num_threads, qps) |>
      arrange(dataset_name, num_threads) |>
      group_by(dataset_name) |>
      mutate(speedup = qps / lag(qps)) |>
      print()

    dbDisconnect(con)
    res
  }),

  parallel_plots = target({
    data |>
      filter(algorithm_params=="dimension_order=TimeDuration page_size=100") |>
      ggplot(aes(num_threads, time_index_ms, group=algorithm_params)) +
      geom_point() +
      geom_line() +
      geom_rangeframe() +
      scale_y_continuous(limits=c(NA, NA)) +
      scale_x_continuous(trans=scales::transform_log2()) +
      facet_wrap(vars(dataset_name), scales="free_y", nrow=1) +
      labs(
        x = "threads",
        y = "index time (ms)"
      ) +
      theme_paper()
  }),

  parallel_queries_plot = target({
    data_pquery |>
      ggplot(aes(num_threads, qps, group=algorithm_params)) +
      geom_point() +
      geom_line() +
      geom_rangeframe() +
      scale_y_continuous(limits=c(NA, NA)) +
      scale_x_continuous(trans=scales::transform_log2()) +
      facet_wrap(vars(dataset_name), scales="free_y", nrow=1) +
      labs(
        x = "threads",
        y = "queries per second"
      ) +
      theme_paper()
  }),

  parallel_plots_file = target(ggsave(
    "paper/images/parallel-indexing.png",
    parallel_plots,
    width=7,
    height=1.8,
    dpi=300)
  ),

  parallel_queries_plots_file = target(ggsave(
    "paper/images/parallel-queries.png",
    parallel_queries_plot,
    width=7,
    height=1.8,
    dpi=300)
  )

)
