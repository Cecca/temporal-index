library(tidyverse)
library(RSQLite)
library(DBI)
library(drake)

plot_parallel_query_focus <- function(data_focus) {
    datasets <- data_focus |> pull(dataset_name)
    assertthat::are_equal(length(datasets), 1)
    stops <- seq(0, 1.0, by = 1 / 32)
    plotdata <- data_focus |>
        filter(num_threads > 1) |>
        filter(matches > 0, dataset_name == "random-uniform-zipf") |>
        mutate(
            algorithm_name = case_when(
                algorithm_name == "interval-tree" ~ "Interval-Tree",
                algorithm_name == "BTree" ~ "B-Tree",
                algorithm_name == "RTree" ~ "R-Tree",
                algorithm_name == "grid-file" ~ "Grid-File",
                algorithm_name == "period-index-*" ~ "Period-Index*",
                algorithm_name == "rd-index-dt" ~ "RD-index-dt",
                algorithm_name == "rd-index-td" ~ "RD-index-td",
                algorithm_name == "hint" ~ "HINT",
                T ~ algorithm_name
            ),
            algorithm_name = factor(algorithm_name, levels = c(
                "RD-index-td",
                "RD-index-dt",
                "Grid-File",
                "HINT",
                "Period-Index*",
                "R-Tree",
                "Interval-Tree",
                "B-Tree"
            ), ordered = T)
        ) |>
        mutate(
            selectivity_time_group = cut(
                selectivity_time,
                breaks = stops,
                labels = stops[-1],
                right = T,
                ordered_result = T
            ),
            selectivity_duration_group = cut(
                selectivity_duration,
                breaks = stops,
                labels = stops[-1],
                right = T,
                ordered_result = T
            )
        ) |>
        group_by(
            num_threads,
            algorithm_name,
            selectivity_time_group,
            selectivity_duration_group
        ) |>
        summarise(
          query_time = mean(query_time),
          sequential_query_time = mean(sequential_query_time),
        ) |>
        ungroup() |>
        mutate(
            query_time = drop_units(sequential_query_time / query_time),
                # set_units(query_time, "milliseconds"),
            query_time_tile =
                query_time |> ntile(20)
        )
    
    labels_data <- plotdata |>
        mutate(query_time = query_time) |>
        group_by(as.integer((query_time_tile - 1) / 4)) |>
        summarise(
            query_time_tile = min(query_time_tile),
            max_tile = max(query_time_tile),
            max_time = max(query_time),
            min_time = min(query_time)
        ) |>
        mutate(
            label = str_c(
                scales::number(min_time, accuracy = 0.1)
            )
        )
    # breaks <- pull(labels_data, query_time_tile)
    # labels <- pull(labels_data, label)
    breaks <- c(
        pull(labels_data, query_time_tile),
        summarise(labels_data, max(max_tile) + 3) |> pull()
    )
    labels <- c(
        pull(labels_data, label),
        summarise(labels_data, max(max_time) |>
            scales::number(accuracy = 0.1, suffix=" x")) |> pull()
    )

    ggplot(plotdata, aes(
        x = selectivity_time_group,
        y = selectivity_duration_group,
        color = query_time,
        fill = query_time,
        tooltip = query_time
    )) +
        # geom_point_interactive() +
        geom_tile() +
        # scale_fill_continuous_diverging(mid=1, aesthetics=c("fill", "color")) +
        scale_fill_gradient2(low="red", high="blue", mid = "white", midpoint = 1, aesthetics=c("fill", "color")) +
        # scale_fill_viridis_c(
        #     # breaks = breaks,
        #     # labels = labels,
        #     option = "viridis",
        #     direction = 1,
        #     aesthetics = c("fill", "color")
        # ) +
        scale_y_discrete(breaks = c(.25, .5, .75, 1)) +
        scale_x_discrete(breaks = c(.25, .5, .75, 1)) +
        facet_grid(vars(algorithm_name), vars(num_threads)) +
        labs(
            x = "time selectivity",
            y = "duration selectivity"
        ) +
        theme_paper() +
        theme(
            legend.title = element_blank(),
            legend.position = "right",
            # legend.key.width = unit(30, "mm")
        )
}


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

  data_pquery_focus = target({
    db_file <- here::here("temporal-index-results-parallel.sqlite")
    conn <- dbConnect(RSQLite::SQLite(), db_file)

    res <- as_tibble(dbGetQuery(
      conn,
      "select * from query_focus_parallel_w_stats natural join focus_configuration_parallel"
    )) |>
      mutate(query_time = set_units(query_time_ns, "ns")) |>
      select(-query_time_ns) |>
      filter(algorithm_name != "period-index++") |>
      filter(algorithm_params != "dimension_order=TimeDuration page_size=10") |>
      mutate(
        algorithm_name = case_when(
          str_detect(algorithm_params, "TimeDuration") ~ str_c(algorithm_name, "-td"),
          str_detect(algorithm_params, "DurationTime") ~ str_c(algorithm_name, "-dt"),
          TRUE ~ algorithm_name
        )
      )
  
    dbDisconnect(conn)

    single_thread <- res |>
      filter(num_threads == 1) |>
      select(algorithm_name, algorithm_params, dataset_name, dataset_params, queryset_name, queryset_params, query_index, sequential_query_time = query_time)

    res <- inner_join(res, single_thread)

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
  ),
  
  parallel_query_focus_plot = target(plot_parallel_query_focus(data_pquery_focus)),
  parallel_query_focus_plot_file = target(ggsave(
    "paper/images/parallel-focus.png",
    parallel_query_focus_plot,
    width=7,
    height=2.2,
    dpi=300
  ))

)
