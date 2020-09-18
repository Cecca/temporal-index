## Define utility functions to load tables of results from the DB

table_main <- function(connection, path) {
  main <- tbl(connection, "main")
  stats <- tbl(connection, "query_stats") %>%
    group_by(sha) %>%
    summarise(output_throughput_ints_ns = sum(as.double(query_count), na.rm = T) / sum(as.double(query_time_ns), na.rm = T))
  inner_join(main, stats)
}

table_query_stats <- function(connection, path, dataset_val, dataset_params_val, queryset_val, queryset_params_val) {
  stats <- tbl(connection, "query_stats")
  main <- table_main(connection, path) %>%
    select(sha, dataset, dataset_params, queryset, queryset_params, algorithm, algorithm_params) %>%
    filter(dataset == dataset_val, dataset_params == dataset_params_val, 
           queryset == queryset_val, queryset_params == queryset_params_val)
  inner_join(main, stats) %>%
    collect() %>%
    mutate(query_time = set_units(as.numeric(query_time_ns), "ns"),
           normalized_query_time = query_time / set_units(query_count, "records"),
           precision = query_count / (query_examined)) %>%
    select(-query_time_ns)
}

table_period_index_buckets <- function(connection, path, dataset_val, dataset_params_val, queryset_val, queryset_params_val) {
  main <- table_main(connection, path) %>%
    select(sha, dataset, dataset_params, queryset, queryset_params, algorithm, algorithm_params) %>%
    filter(dataset == dataset_val, dataset_params == dataset_params_val, 
           queryset == queryset_val, queryset_params == queryset_params_val,
           algorithm %LIKE% "period-index%")
  print(main %>% select(sha) %>% arrange(sha))
  bucket_info <- tbl(connection, "period_index_buckets")
  print(bucket_info %>% distinct(sha) %>% arrange(sha))
  p <- inner_join(main, bucket_info) %>%
    collect()
  print(p)
  p
}