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
  db_file <- here::here("temporal-index-results.sqlite")
  conn <- dbConnect(RSQLite::SQLite(), db_file)

  res <- dplyr::tbl(conn, "batch") %>%
    filter(algorithm_name != "period-index++") %>%
    collect() %>%
    # Filter some no longer considered configurations that might still be
    # lingering around
    filter(
      hostname %in% c("ironmaiden"),
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
      # index_size = as.double(index_size_bytes) / (1024 * 1024), # in megabytes. The `units` library does not support this use case well
      qps = set_units(qps, "1/s")
    ) %>%
    select(-time_query_ms, -time_index_ms) %>%
    # Tag workloads and distributions
    mutate(
      workload_type = case_when(
        queryset_name == "random-uniform-zipf-uniform" ~ "range-duration",
        queryset_name == "random-zipf-uniform-uniform" ~ "range-duration",
        queryset_name == "random-zipf-uniform-None" ~ "range",
        queryset_name == "random-clustered-zipf-uniform" ~ "range-duration",
        queryset_name == "random-None-uniform" ~ "duration",
        queryset_name == "random-uniform-zipf-None" ~ "range",
        queryset_name == "random-clustered-zipf-None" ~ "range",
        queryset_name == "Mixed" ~ "mixed",
        TRUE ~ "Unknown"
      ),
      workload_type = factor(workload_type, ordered = T, levels = c(
        "range-duration",
        "duration",
        "range"
      ))
    ) %>%
    mutate(
      start_times_distribution =
        case_when(
          dataset_name == "random-uniform-zipf" ~ "uniform",
          dataset_name == "random-clustered-zipf" ~ "clustered",
          dataset_name == "random-zipf-uniform" ~ "zipf",
        )
    ) %>%
    mutate(
      algorithm_name = case_when(
        str_detect(algorithm_params, "TimeDuration") ~ str_c(algorithm_name, "-td"),
        str_detect(algorithm_params, "DurationTime") ~ str_c(algorithm_name, "-dt"),
        TRUE ~ algorithm_name
      )
    ) %>%
    mutate(
      dataset_size = case_when(
        dataset_name == "Flight" ~ 684838,
        dataset_name == "Tourism" ~ 835071,
        dataset_name == "Webkit" ~ 1547419,
        dataset_name == "MimicIII" ~ 4134909,
        T ~ as.double(str_match(dataset_params, " n=(\\d+)")[, 2])
      ),
      bytes_per_interval = index_size_bytes / dataset_size
    )

  dbDisconnect(conn)
  res
}

filter_synthetic <- function(data_batch) {
  data_batch %>%
    filter(
      str_detect(dataset_name, "random"),
      # # Focus on experiments on 10 million intervals, without mixing
      # # in experiments about scalability
      str_detect(dataset_params, " n=10000000 "),
      (str_detect(queryset_params, " dur_n=10000000 ") | !str_detect(queryset_params, " dur_n=")),
      # # Filter out datasets used in scalability experiments
      !str_detect(dataset_params, "start_high=1000000000 "),
      str_detect(queryset_params, " n=5000 "),
      !(queryset_params %in% c(
        "start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1 dur_dist_low=1 dur_dist_high=10000",
        "seed=23512 n=20000 start_low=1 start_high=10000000 dur_n=10000000 dur_beta=1",
        "seed=23512 n=20000  dur_dist_low=1 dur_dist_high=10000"
      )),
      !(queryset_id %in% c(389, 408)),
      dataset_name != "random-zipf-uniform"
    )
}


filter_real <- function(data_batch) {
  data_batch %>%
    filter(dataset_name %in% c("Flight", "Webkit", "Tourism", "MimicIII")) %>%
    filter(case_when(
      dataset_name != "Flight" ~ TRUE,
      queryset_name == "random-None-uniform-scaled" ~ TRUE,
      str_detect(queryset_params, "dur_scale=60") ~ TRUE,
      TRUE ~ FALSE
    ))
  # filter((dataset_name != "Flight") |
  #         (str_detect(queryset_params, "dur_scale=60")))
}

table_best <- function() {
  data <- table_batch() %>%
    filter(index_size_bytes > 0)
  synth <- filter_synthetic(data)
  real <- filter_real(data)
  bind_rows(synth, real) %>%
    filter(!str_detect(queryset_name, "capped")) %>%
    filter(algorithm_name != "period-index++") %>%
    group_by(algorithm_name, queryset_id, dataset_id) %>%
    slice_max(qps) %>%
    # if there are multiple entries with the exact
    # same queries per second, just return the first
    slice(1) %>%
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
    filter(
      str_detect(queryset_name, "capped"),
      (!str_detect(dataset_name, "Flight")) | (str_detect(queryset_params, "dur_scale=60")),
      (!str_detect(dataset_name, "Tourism"))
    ) %>%
    mutate(
      # For synthetic datasets
      dataset_n = as.integer(str_match(dataset_params, " n=(\\d+)")[, 2]),
      # For real, scaled datasets
      scale = as.integer(str_match(dataset_params, "copies=(\\d+)")[, 2]),
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[, 2]),
      scale = case_when(
        dataset_name %in% c("Flight", "Webkit", "MimicIII") ~ as.integer(1),
        is.na(scale) ~ as.integer(dataset_n / 10000000),
        TRUE ~ scale
      )
    ) %>%
    # distinct(dataset_n, scale, queryset_params) %>% print(n=10000)
    filter(scale %in% c(1, 10, 20, 30, 40, 50)) %>%
    filter(
      # Filter synthetic datasets
      (dataset_name == "random-uniform-zipf" &
        queryset_n == 10000 &
        str_detect(queryset_params, "start_high=10000000")) |
        # Filter real datasets
        str_detect(dataset_name, "reiterated") |
        (dataset_name %in% c("Flight", "Webkit", "MimicIII"))
    ) %>%
    mutate(
      dataset_name = str_remove(dataset_name, "reiterated-")
    )

  batch_data %>%
    group_by(dataset_id, scale, algorithm_name) %>%
    slice_max(qps) %>%
    ungroup()
}

# Table about the dependency between period-index++ performance
# and page size
table_parameter_dependency <- function() {
  table_batch() %>%
    filter(!str_detect(queryset_name, "capped")) %>%
    filter(
      algorithm_name %in% c("rd-index-td", "rd-index-dt"),
      dataset_name %in% c("random-zipf-uniform", "random-uniform-zipf"),
      queryset_name %in% c("random-uniform-zipf-uniform", "random-None-uniform", "random-uniform-zipf-None"),
      # !(queryset_name %in% c("random-clustered-zipf-uniform", "random-clustered-zipf-None")),
      # !(dataset_name == "random-zipf-uniform" & str_detect(queryset_name, "random-uniform-zipf"))
    ) %>%
    mutate(
      algorithm_params = str_remove(algorithm_params, "dimension_order=.* "),
      dataset_n = as.integer(str_match(dataset_params, "n=(\\d+)")[, 2]),
      queryset_n = as.integer(str_match(queryset_params, "n=(\\d+)")[, 2])
    ) %>%
    get_params(queryset_params, "q_") %>%
    filter(
      dataset_n == 10000000,
      queryset_n == 10000,
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

table_insertions <- function() {
  db_file <- here::here("temporal-index-results.sqlite")
  conn <- DBI::dbConnect(RSQLite::SQLite(), db_file)

  res <- as_tibble(dbGetQuery(
    conn,
    "select * from insertions where batch_size = 50000"
  )) %>%
    filter(algorithm_name != "period-index++") %>%
    mutate(
      algorithm_name = case_when(
        str_detect(algorithm_params, "TimeDuration") ~ str_c(algorithm_name, "-td"),
        str_detect(algorithm_params, "DurationTime") ~ str_c(algorithm_name, "-dt"),
        TRUE ~ algorithm_name
      ),
      page_size = as.integer(str_match(algorithm_params, "page_size=(\\d+)")[,2])
    )

  DBI::dbDisconnect(conn)
  res
}


table_query_focus <- function() {
  db_file <- here::here("temporal-index-results.sqlite")
  conn <- dbConnect(RSQLite::SQLite(), db_file)

  as_tibble(dbGetQuery(
    conn,
    "select * from query_focus_w_stats natural join focus_configuration"
  )) %>%
    mutate(query_time = set_units(query_time_ns, "ns")) %>%
    select(-query_time_ns) %>%
    filter(algorithm_name != "period-index++") %>%
    filter(algorithm_params != "dimension_order=TimeDuration page_size=10") %>%
    mutate(
      algorithm_name = case_when(
        str_detect(algorithm_params, "TimeDuration") ~ str_c(algorithm_name, "-td"),
        str_detect(algorithm_params, "DurationTime") ~ str_c(algorithm_name, "-dt"),
        TRUE ~ algorithm_name
      )
    )
}

table_query_focus_inefficient <- function() {
  db_file <- here::here("temporal-index-results.sqlite")
  conn <- dbConnect(RSQLite::SQLite(), db_file)

  as_tibble(dbGetQuery(
    conn,
    "select * from query_focus_w_stats natural join focus_configuration"
  )) %>%
    mutate(query_time = set_units(query_time_ns, "ns")) %>%
    select(-query_time_ns) %>%
    filter(algorithm_name != "period-index++") %>%
    filter(algorithm_params == "dimension_order=TimeDuration page_size=10") %>%
    mutate(
      algorithm_name = case_when(
        str_detect(algorithm_params, "TimeDuration") ~ str_c(algorithm_name, "-td"),
        str_detect(algorithm_params, "DurationTime") ~ str_c(algorithm_name, "-dt"),
        TRUE ~ algorithm_name
      )
    )
}

table_running_example <- function(query_range, query_duration) {
  set.seed(1234)

  raw_flights <- read_csv(".datasets/flights.csv.gz")

  flights <- raw_flights %>%
    filter(fl_date == "2018-08-01") %>%
    sample_n(1000) %>%
    drop_na(dep_time, arr_time) %>%
    select(fl_date, dep_time, arr_time, tail_num) %>%
    mutate(
      dep_time = as.character(dep_time),
      dep_padding = case_when(
        str_length(dep_time) == 4 ~ "",
        str_length(dep_time) == 3 ~ "0",
        str_length(dep_time) == 2 ~ "00",
        str_length(dep_time) == 1 ~ "000"
      ),
      departure = ymd_hms(str_c(fl_date, "T", dep_padding, dep_time, "00")),
      arr_time = as.character(arr_time),
      arr_padding = case_when(
        str_length(arr_time) == 4 ~ "",
        str_length(arr_time) == 3 ~ "0",
        str_length(arr_time) == 2 ~ "00",
        str_length(arr_time) == 1 ~ "000"
      ),
      arrival = ymd_hms(str_c(fl_date, "T", arr_padding, arr_time, "00")),
      duration = as.numeric(arrival - departure, "hours")
    ) %>%
    select(flight = tail_num, departure, arrival, duration) %>%
    filter(date(departure) == "2018-08-01") %>%
    mutate(duration = as.double(duration)) %>%
    filter(duration > 0, duration < 30000) %>%
    mutate(
      highlighted = row_number() %in% c(
        100, 46, 16, 38, 667
      ),
      matches = (query_duration[1] <= duration & duration < query_duration[2]) &
        int_overlaps(interval(departure, arrival), query_range)
    )
}

table_running_mimic <- function(query_range, query_duration) {
  set.seed(1234)

  highlighted_data <- tribble(
    ~start, ~duration,
    "2016-06-10", 2,
    "2016-06-20", 15,
    "2016-06-24", 10,
    "2016-06-8", 6
  ) %>%
    mutate(
      start = ymd(start),
      highlighted = TRUE
    )

  # dataset <-
  read_csv(here::here("example_rdindex/example_dataset.csv")) %>%
    mutate(highlighted = FALSE) %>%
    bind_rows(highlighted_data) %>%
    mutate(
      time_range = interval(start, start + duration),
      matches = int_overlaps(time_range, query_range) & between(duration, query_duration[1], query_duration[2])
    ) %>%
    filter(highlighted)
}

get_best_wide <- function(best_batch) {
  best_batch %>%
    filter(
      dataset_name %in% c("UZ", "Flight", "Webkit", "MimicIII"),
      time_constraint %in% c("UZ", "UU", "-"),
      duration_constraint %in% c("U", "-")
    ) %>%
    ungroup() %>%
    select(
        dataset_name, time_constraint,
        duration_constraint, algorithm_name, qps
    ) %>%
    group_by(dataset_name, time_constraint, duration_constraint) %>%
    distinct(algorithm_name, qps) %>%
    mutate(
        qps = qps %>% units::drop_units(),
        rank = dense_rank(desc(qps)),
    ) %>%
    replace_na(list(qps = 0)) %>%
    mutate(
        algorithm_name = case_when(
            algorithm_name == "BTree" ~ "B-Tree",
            algorithm_name == "grid-file" ~ "Grid-File",
            algorithm_name == "period-index-*" ~ "Period-Index*",
            algorithm_name == "interval-tree" ~ "Interval-Tree",
            algorithm_name == "rd-index-td" ~ "RD-index-td",
            algorithm_name == "rd-index-dt" ~ "RD-index-dt",
            algorithm_name == "RTree" ~ "R-Tree",
            algorithm_name == "hint" ~ "HINT",
            TRUE ~ algorithm_name
        ),
        algorithm_name = factor(algorithm_name,
            levels = c(
                "RD-index-td",
                "RD-index-dt",
                "Grid-File",
                "HINT",
                "Period-Index*",
                "R-Tree",
                "Interval-Tree",
                "B-Tree"
            ),
            ordered = TRUE
        )
    ) %>%
    arrange(algorithm_name) %>%
    select(
        dataset = dataset_name,
        time = time_constraint,
        duration = duration_constraint,
        algorithm_name,
        qps
    ) %>%
    mutate(
        query_type = case_when(
            time == "UZ" && duration == "U" ~ "qrd",
            time == "UU" && duration == "U" ~ "qrd",
            time == "-" && duration == "U"  ~ "qdo",
            time == "UZ" && duration == "-" ~ "qro",
            time == "UU" && duration == "-" ~ "qro",
            T ~ "unknown"
        ),
        query_type = factor(query_type, levels = c("qro", "qdo", "qrd"), ordered = T)
    ) %>%
    ungroup() %>%
    select(
        dataset,
        query = query_type,
        algorithm = algorithm_name,
        qps
    ) %>%
    pivot_wider(
        names_from = "query",
        values_from = "qps"
    ) %>%
    mutate(
        dataset = case_when(
            dataset == "UZ" ~ "Random",
            TRUE ~ dataset
        ),
        dataset = factor(dataset,
            levels = c("Random", "Flight", "Webkit", "MimicIII"),
            ordered = TRUE
        )
    ) %>%
    arrange(dataset, algorithm)
}

# Mixes duration only and range only queries
get_simulated_tradeoff_do_ro <- function(best_wide) {
  best_wide %>% 
    mutate(frac_dur = list(0:100/100)) %>% 
    unnest(cols = frac_dur) %>% 
    mutate(qps = 1 / ((1-frac_dur) / qro + frac_dur / qdo))
}

# Mixes range-duration and duration-only queries
get_simulated_tradeoff_do_rd <- function(best_wide) {
  best_wide %>% 
    mutate(frac_dur = list(0:100/100)) %>% 
    unnest(cols = frac_dur) %>% 
    mutate(qps = 1 / ((1-frac_dur) / qrd + frac_dur / qdo))
}

# Mixes range-duration and range-only queries
get_simulated_tradeoff_ro_rd <- function(best_wide) {
  best_wide %>% 
    mutate(frac_ro = list(0:100/100)) %>% 
    unnest(cols = frac_ro) %>% 
    mutate(qps = 1 / ((1-frac_ro) / qrd + frac_ro / qro))
}

get_simulated_tradeoff_tern <- function(best_wide) {
  n <- 100
  best_wide %>%
    replace_na(list(qdo = 0)) %>%
    mutate(fracs = list(expand.grid(frac_ro =0:n/n, frac_do =0:n/n))) %>%
    unnest(cols = fracs) %>%
    mutate(frac_rd = 1 - frac_ro - frac_do) %>%
    mutate(frac_rd = if_else(abs(frac_rd - 0) <= 0.00001, 0, frac_rd)) %>%
    filter(frac_rd >= 0, abs(frac_rd + frac_ro + frac_do - 1) <= 0.0001) %>%
    mutate(
      qps = 1 / (frac_rd / qrd + frac_ro / qro + frac_do / qdo)
    )
}
