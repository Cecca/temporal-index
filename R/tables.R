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
      str_detect(dataset_params, "n=10000000 ")
    )
}

filter_real <- function(data_batch) {
  data_batch %>%
    filter(dataset_name %in% c("Flights", "Webkit", "Tourism"))
}

table_best <- function() {
  data <- table_batch()
  synth <- filter_synthetic(data)
  real <- filter_real(data)
  bind_rows(synth, real) %>%
    group_by(algorithm_name, queryset_id, dataset_id) %>%
    slice_max(qps) %>%
    ungroup()
}

# Table about the parameter dependency change
table_param_dep <- function() {
  table_batch() %>%
    filter(algorithm_name == "period-index++") %>%
    select(dataset_name, queryset_name, algorithm_params, qps, time_queries)
}

# Extracts, out of all the batch experiments, the scalability ones
table_scalability <- function() {
  batch_data <- table_batch() %>%
    mutate(
      dataset_n = as.integer(str_match(dataset_params, "n=(\\d+)")[, 2]),
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[, 2])
    ) %>%
    filter(
      queryset_n == 5000,
      str_detect(queryset_params, "start_high=1000000000")
    )

  best_param_at_large_size <- batch_data %>%
    filter(dataset_n == 1000000000) %>%
    group_by(dataset_id, queryset_id, algorithm_name) %>%
    slice_max(qps) %>%
    ungroup() %>%
    select(dataset_name, queryset_name, algorithm_params)

  semi_join(batch_data, best_param_at_large_size)
}