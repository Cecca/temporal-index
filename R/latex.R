# This file collects functions related to the production of latex tables

latex_best <- function(data_best) {
    palette <- viridisLite::viridis(5, direction = -1)
    lineseps <- c(
        "", "", "", "", "\\addlinespace",
        "", "", "", "", "\\addlinespace",
        "", "", "\\addlinespace",
        "", "", "\\addlinespace",
        "", "", "\\addlinespace"
    )

    # TODO come up with a good naming scheme for workloads
    data_best %>%
        ungroup() %>%
        select(
            dataset_name, time_constraint,
            duration_constraint, algorithm_name, qps
        ) %>%
        group_by(dataset_name, time_constraint, duration_constraint) %>%
        distinct(algorithm_name, qps) %>%
        mutate(
            rank = row_number(desc(qps)),
            qps_num = qps %>% drop_units(),
            speedup = scales::number(qps_num / min(qps_num), accuracy = 1),
            speedup_str = if_else(qps_num == min(qps_num),
                "",
                str_c(" {\\footnotesize(x\\,", speedup, ")}")
            ),
            qps = drop_units(qps) %>% scales::number(big.mark = "\\\\,"),
            qps = if_else(qps_num == max(qps_num),
                str_c("\\underline{", qps, "}"),
                qps
            ),
            qps = str_c(qps, speedup_str),
            qps = cell_spec(qps,
                background = palette[rank],
                color = if_else(rank <= 2, "black", "white"),
                format = "latex",
                escape = FALSE
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
            # algorithm_name = fct_reorder(algorithm_name, rank, .fun = median)
            algorithm_name = factor(algorithm_name,
                levels = c("PI++", "GF", "BT", "PI*", "IT"),
                ordered = TRUE
            )
        ) %>%
        # (function(d) {
        #     print(group_by(d, algorithm_name) %>% summarise(m = median(rank)) %>% arrange(m))
        #     d
        # }) %>%
        arrange(algorithm_name) %>%
        select(
            dataset = dataset_name,
            time = time_constraint,
            duration = duration_constraint,
            algorithm_name,
            qps
        ) %>%
        pivot_wider(
            names_from = "algorithm_name",
            values_from = "qps"
        ) %>%
        mutate(dataset = factor(dataset,
            levels = c("UZ", "CZ", "Flight", "Webkit", "Tourism"),
            ordered = TRUE
        )) %>%
        arrange(dataset) %>%
        kbl(format = "latex", booktabs = T, escape = F, linesep = lineseps) %>%
        add_header_above(c(" " = 1, "constraint" = 2, " " = 5))
}