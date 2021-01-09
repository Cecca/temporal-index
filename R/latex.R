# This file collects functions related to the production of latex tables

latex_best <- function(data_best) {
    # TODO come up with a good naming scheme for workloads
    data_best %>%
        ungroup() %>%
        select(dataset_id, queryset_id, algorithm_name, qps) %>%
        group_by(dataset_id, queryset_id) %>%
        mutate(
            qps_num = qps,
            qps = drop_units(qps) %>% scales::number(big.mark = "\\\\,"),
            qps = if_else(qps_num == max(qps_num),
                str_c("\\underline{", qps, "}"),
                qps
            )
        ) %>%
        mutate(
            algorithm_name = case_when(
                algorithm_name == "period-index++" ~ "PI++",
                algorithm_name == "BTree" ~ "BT",
                algorithm_name == "grid-file" ~ "GF",
                algorithm_name == "period-index-*" ~ "PI*",
                algorithm_name == "interval-tree" ~ "IT",
                TRUE ~ algorithm_name
            ),
            algorithm_name = fct_reorder(algorithm_name, qps)
        ) %>%
        arrange(algorithm_name) %>%
        select(
            dataset = dataset_id, queryset = queryset_id,
            algorithm_name, qps
        ) %>%
        pivot_wider(names_from = "algorithm_name", values_from = "qps") %>%
        kbl(format = "latex", booktabs = T, escape = F)
}