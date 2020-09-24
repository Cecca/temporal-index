workloads <- tribble(
  ~workload, ~queryset, ~queryset_params,
  "short dur. high selectivity", "random-uniform-zipf-uniform-uniform", "23512:5000_1:10000000_10000000:1_1:100_1:100",
  "long dur. high selectivity", "random-uniform-zipf-uniform-uniform", "23512:5000_1:10000000_10000000:1_1:10000_1:100",
  "long dur. low selectivity", "random-uniform-zipf-uniform-uniform", "23512:5000_1:10000000_10000000:1_1:10000_1:10000",
  "short dur. low selectivity", "random-uniform-zipf-uniform-uniform", "23512:5000_1:10000000_10000000:1_1:100_1:10000",
  "no duration", "random-uniform-zipf-None", "23512:5000_1:10000000_10000000:1_NA_NA",
  "only duration (short) high selectivity", "random-None-uniform-uniform", "seed=23512 n=5000  durmin_low=1 durmin_high=100 durmax_low=1 durmax_high=100",
  "only duration (long) high selectivity", "random-None-uniform-uniform", "23512:5000_NA_NA_1:10000_1:100",
  "only duration (short) low selectivity", "random-None-uniform-uniform", "23512:5000_NA_NA_1:100_1:10000",
  "only duration (long) low selectivity", "random-None-uniform-uniform", "23512:5000_NA_NA_1:10000_1:10000"
)