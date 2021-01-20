## Define utility functions to load tables of results from the DB

# Extract parameters related to a column
get_params <- function(data, column, prefix) {
  out <- data %>%
    separate({{ column }}, into = paste0("p", 1:20), sep = " ", remove = F) %>%
    suppressWarnings() %>%
    pivot_longer(p1:p20, names_to = "dummy__", values_to = "pair__") %>%
    select(-dummy__) %>%
    drop_na(pair__) %>%
    separate(pair__,
      into = c("param__", "param_val__"),
      sep = "=", remove = T, convert = T
    ) %>%
    mutate(param__ = str_c(prefix, param__)) %>%
    filter(str_length(param__) > 0) %>%
    pivot_wider(names_from = param__, values_from = param_val__)

  assert_that(nrow(out) == nrow(data))
  out
}

# Loads data from the batch table
table_batch_db <- function(connection, path) {
  main <- dplyr::tbl(connection, "batch")
  main
}

# Loads data from the batch table, cleans it, and returns it as a tibble
table_batch <- function() {
  db_file <- here("temporal-index-results.sqlite")
  conn <- dbConnect(RSQLite::SQLite(), db_file)

  res <- dplyr::tbl(conn, "batch") %>%
    collect() %>%
    # Filter some no longer considered configurations that might still be
    # lingering around
    filter(
      hostname == "ironmaiden",
      algorithm_name != "ebi-index",
      algorithm_name != "linear-scan",
      algorithm_name != "grid",
      algorithm_name != "grid3D",
      algorithm_name != "NestedVecs",
      algorithm_name != "NestedBTree",
      algorithm_name != "period-index-old-*",
      !str_detect(queryset_name, "Mixed"),
    ) %>%
    # Convert to dates
    mutate(date = parse_datetime(date)) %>%
    # Select the most recent runs among the ones with the same version
    # and parameterization, in case of reruns
    group_by(dataset_id, queryset_id, algorithm_id) %>%
    slice(which.max(date)) %>%
    ungroup() %>%
    # Turn times to milliseconds, and remove the original columns
    mutate(
      time_queries = set_units(time_query_ms, "ms"),
      time_index = set_units(time_index_ms, "ms"),
      qps = set_units(qps, "1/s")
    ) %>%
    select(-time_query_ms, -time_index_ms) %>%
    # Tag workloads and distributions
    mutate(
      workload_type = case_when(
        queryset_name == "random-uniform-zipf-uniform" ~ "both",
        queryset_name == "random-clustered-zipf-uniform" ~ "both",
        queryset_name == "random-None-uniform" ~ "duration",
        queryset_name == "random-uniform-zipf-None" ~ "time",
        queryset_name == "random-clustered-zipf-None" ~ "time",
        queryset_name == "Mixed" ~ "mixed",
        TRUE ~ "Unknown"
      )
    ) %>%
    mutate(
      start_times_distribution =
        if_else(dataset_name == "random-uniform-zipf",
          "uniform",
          "clustered"
        )
    )

  dbDisconnect(conn)
  res
}

filter_synthetic <- function(data_batch) {
  data_batch %>%
    filter(
      str_detect(dataset_name, "random"),
      # Focus on experiments on 10 million intervals, without mixing
      # in experiments about scalability
      str_detect(dataset_params, "n=10000000 "),
      # Filter out datasets used in scalability experiments
      !str_detect(dataset_params, "start_high=1000000000 "),
      !(queryset_params %in% c(
        "start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 dur_dist_low=1 dur_dist_high=10000",
        "seed=23512 n=20000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
        "seed=23512 n=20000  dur_dist_low=1 dur_dist_high=10000"
      )),
      !(queryset_id %in% c(389, 408))
    )
}

filter_real <- function(data_batch) {
  data_batch %>%
    filter(dataset_name %in% c("Flight", "Webkit", "Tourism"))
}

table_best <- function() {
  data <- table_batch()
  synth <- filter_synthetic(data)
  real <- filter_real(data)
  bind_rows(synth, real) %>%
    group_by(algorithm_name, queryset_id, dataset_id) %>%
    slice_max(qps) %>%
    ungroup() %>%
    mutate(dataset_name = case_when(
      dataset_name == "random-uniform-zipf" ~ "UZ",
      dataset_name == "random-clustered-zipf" ~ "CZ",
      T ~ dataset_name
    )) %>%
    mutate(time_constraint = case_when(
      queryset_name == "random-uniform-zipf-uniform" ~ "UZ",
      queryset_name == "random-uniform-zipf-None" ~ "UZ",
      queryset_name == "random-clustered-zipf-uniform" ~ "CZ",
      queryset_name == "random-clustered-zipf-None" ~ "CZ",
      queryset_name == "random-None-uniform" ~ "-",
      queryset_name == "random-None-uniform-scaled" ~ "-",
      queryset_name == "random-uniform-scaled-uniform-scaled-uniform-scaled" ~ "UU",
      queryset_name == "random-uniform-uniform-uniform" ~ "UU",
      queryset_name == "random-uniform-scaled-uniform-scaled-uniform" ~ "UU",
      queryset_name == "random-uniform-scaled-uniform-scaled-None" ~ "UU",
      queryset_name == "random-uniform-uniform-None" ~ "UU",
      TRUE ~ "undefined"
    )) %>%
    mutate(duration_constraint = case_when(
      queryset_name == "random-uniform-zipf-uniform" ~ "U",
      queryset_name == "random-uniform-zipf-None" ~ "-",
      queryset_name == "random-clustered-zipf-uniform" ~ "U",
      queryset_name == "random-clustered-zipf-None" ~ "-",
      queryset_name == "random-None-uniform" ~ "U",
      queryset_name == "random-None-uniform-scaled" ~ "U",
      queryset_name == "random-uniform-scaled-uniform-scaled-uniform-scaled" ~ "U",
      queryset_name == "random-uniform-uniform-uniform" ~ "U",
      queryset_name == "random-uniform-scaled-uniform-scaled-uniform" ~ "U",
      queryset_name == "random-uniform-scaled-uniform-scaled-None" ~ "-",
      queryset_name == "random-uniform-uniform-None" ~ "-",
      TRUE ~ "undefined"
    ))
}


# Extracts, out of all the batch experiments, the scalability ones
table_scalability <- function() {
  batch_data <- table_batch() %>%
    mutate(
      # For synthetic datasets
      dataset_n = as.integer(str_match(dataset_params, " n=(\\d+)")[, 2]),
      # For real, scaled datasets
      scale = as.integer(str_match(dataset_params, "copies=(\\d+)")[, 2]),
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[, 2]),
      scale = case_when(
        !is.na(dataset_n) ~ as.integer(dataset_n / 1000000),
        dataset_name %in% c("Flight", "Webkit", "Tourism") ~ as.integer(1),
        TRUE ~ scale
      )
    ) %>%
    filter(
      # Filter synthetic datasets
      (queryset_n == 5000 &
        str_detect(queryset_params, "start_high=1000000000")) |
        # Filter real datasets
        str_detect(dataset_name, "reiterated") |
        (dataset_name %in% c("Flight", "Webkit", "Tourism") &
          queryset_name %in% c(
            "random-uniform-scaled-uniform-scaled-uniform-scaled",
            "random-uniform-scaled-uniform-scaled-uniform"
          ))
    ) %>%
    mutate(
      dataset_name = str_remove(dataset_name, "reiterated-")
    )

  batch_data %>%
    group_by(dataset_id, queryset_id, algorithm_name) %>%
    slice_max(qps) %>%
    ungroup()
}

# Table about the dependency between period-index++ performance
# and page size
table_parameter_dependency <- function() {
  table_batch() %>%
    filter(algorithm_name == "period-index++") %>%
    mutate(
      dataset_n = as.integer(str_match(dataset_params, "n=(\\d+)")[, 2]),
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[, 2])
    ) %>%
    get_params(queryset_params, "q_") %>%
    filter(
      dataset_n == 10000000,
      queryset_n == 20000,
      na_or_in(q_dur_dist_high, c(10000)),
      na_or_in(q_start_high, c(10000000))
    ) %>%
    get_params(algorithm_params, "")
}

# A table of start times of real datasets
table_start_times <- function() {
  get_histograms("dataset-start-times", "./experiments/real-world.yml") %>%
    select(name, start_time = value, count)
}

# A table of durations of real datasets
table_durations <- function() {
  get_histograms("dataset-durations", "./experiments/real-world.yml") %>%
    select(name, duration = value, count)
}

table_query_focus <- function() {
  db_file <- here("temporal-index-results.sqlite")
  conn <- dbConnect(RSQLite::SQLite(), db_file)

  as_tibble(dbGetQuery(
    conn,
    "select * from query_focus_w_stats natural join focus_configuration"
  )) %>%
    mutate(query_time = set_units(query_time_ns, "ns")) %>%
    select(-query_time_ns)
}

table_running_example <- function(query_range, query_duration) {
  tribble(
    ~flight, ~departure, ~arrival, ~pos,
    "f1", hm("8:00"), hm("20:00"), 0,
    "f2", hm("9:00"), hm("12:00"), 1,
    "f3", hm("13:00"), hm("14:00"), 1,
    "f4", hm("18:00"), hm("21:00"), 2,
    "f5", hm("20:15"), hm("21:15"), 1
  ) %>%
    mutate(
      midnight = ymd_hms("2021-01-01T00:00:00"),
      departure = midnight + departure,
      arrival = midnight + arrival,
      duration = arrival - departure,
      matches =
        (query_duration[1] <= duration & duration < query_duration[2]) &
          int_overlaps(interval(departure, arrival), query_range)
    )
}