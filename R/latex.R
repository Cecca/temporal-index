# This file collects functions related to the production of latex tables

latex_best <- function(data_best) {
    palette <- viridisLite::viridis(6, direction = -1)
    # palette <- RColorBrewer::brewer.pal(6, "Greens")
    lineseps <- c(
        "", "", "", "", "\\midrule",
        "", "", "", "", "\\midrule",
        "", "", "\\midrule",
        "", "", "\\midrule",
        "", "", "\\midrule"
    )

    fgcolor <- c(
        "white",
        "black",
        "black",
        "black",
        "black",
        "black",
        "black",
        "black",
        "black",
        "black",
        "black"
    )
    bgcolors <- c(
        "3572B9",
        # "7FABD3",
        "C0DBEC",
        "FFFFFF",
        "FFFFFF",
        "FFFFFF",
        "FFFFFF",
        "FFFFFF",
        "FFFFFF",
        "FFFFFF"
    )

    data_best %>%
        ungroup() %>%
        select(
            dataset_name, time_constraint,
            duration_constraint, algorithm_name, qps, time_index, bytes_per_interval
        ) %>%
        group_by(dataset_name, time_constraint, duration_constraint) %>%
        distinct(algorithm_name, qps, time_index, bytes_per_interval) %>%
        replace_na(list(qps = 0)) %>%
        mutate(
            # rank = row_number(desc(qps)),
            rank = dense_rank(desc(round(qps))),
            rank_str = str_c(" {\\footnotesize(", rank, ")}"),
            qps_num = qps %>% drop_units() %>% round(),
            time_index_num = time_index %>% set_units("ms") %>% drop_units(),
            time_index = time_index_num %>% scales::number(big.mark = "\\\\,"),
            time_index = if_else(time_index_num == min(time_index_num),
                str_c("\\underline{", time_index, "}"),
                time_index
            ),
            bytes_per_interval = if_else(bytes_per_interval == min(bytes_per_interval),
                str_c("\\textbf{", scales::number(bytes_per_interval, accuracy = 0.1), "}"),
                scales::number(bytes_per_interval, accuracy = 0.1)
            ),
            # time_index_str = str_c(
            #     " {\\scriptsize$\\big|$\\stackanchor{",
            #     time_index,
            #     "}{",
            #     bytes_per_interval,
            #     "}}"
            # ),
            qps = drop_units(qps) %>% scales::number(big.mark = "\\\\,"),
            # qps = if_else(qps_num == max(qps_num),
            #     str_c("\\textbf{", qps, "}"),
            #     qps
            # ),
            qps = str_c("\\colorbox[HTML]{", bgcolors[rank], "}{\\color{", fgcolor[rank], "}", qps, "}"),
            qps = str_c(qps, time_index, bytes_per_interval, sep = "$~|~$"),
            # qps = str_c(qps, time_index_str),
            # qps = cell_spec(qps,
            #     background = if_else(qps_num == max(qps_num), "#e9e9e9", "white"),
            #     # color = if_else(rank <= 2, "black", "white"),
            #     format = "latex",
            #     escape = FALSE
            # )
        ) %>%
        mutate(
            algorithm_name = case_when(
                algorithm_name == "BTree" ~ "\\btree",
                algorithm_name == "grid-file" ~ "\\gfile",
                algorithm_name == "period-index-*" ~ "\\pindex",
                algorithm_name == "interval-tree" ~ "\\itree",
                algorithm_name == "rd-index-td" ~ "\\rdtd",
                algorithm_name == "rd-index-dt" ~ "\\rddt",
                algorithm_name == "RTree" ~ "\\rtree",
                TRUE ~ algorithm_name
            ),
            algorithm_name = factor(algorithm_name,
                levels = c(
                    "\\rdtd",
                    "\\rddt",
                    "\\gfile",
                    "\\pindex",
                    "\\rtree",
                    "\\itree",
                    "\\btree"
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
                time == "UZ" && duration == "U" ~ "\\qrd",
                time == "UU" && duration == "U" ~ "\\qrd",
                time == "-" && duration == "U" ~ "\\qdo",
                time == "UZ" && duration == "-" ~ "\\qro",
                time == "UU" && duration == "-" ~ "\\qro",
                T ~ "unknown"
            ),
            query_type = factor(query_type, levels = c("\\qro", "\\qdo", "\\qrd"), ordered = T)
        ) %>%
        ungroup() %>%
        select(
            dataset,
            query = query_type,
            algorithm_name,
            qps
        ) %>%
        pivot_wider(
            names_from = "algorithm_name",
            values_from = "qps"
        ) %>%
        mutate(
            dataset = case_when(
                dataset == "UZ" ~ "Synthetic",
                TRUE ~ dataset
            ),
            dataset = factor(dataset,
                levels = c("Synthetic", "Flight", "Webkit", "MimicIII"),
                ordered = TRUE
            )
        ) %>%
        arrange(dataset, query) %>%
        kbl(format = "latex", booktabs = T, escape = F, linesep = "", align = "llrrrrrrrr") %>%
        collapse_rows(columns = 1, latex_hline = "major", valign = "middle") %>%
        add_header_above(
            # c(" " = 2, "Queries per second$\\\\big|${\\\\scriptsize\\\\stackanchor{index build time}{index size}} " = 7),
            c(" " = 2, "Queries per second $~|~$ Index build time $~|~$ Bytes per interval" = 7),
            escape = F
        )
}

latex_example <- function(data_example) {
    data_example %>%
        filter(highlighted) %>%
        arrange(start) %>%
        mutate(
            tuple = str_c("$r_", row_number(start), "$"),
            end = start + duration
        ) %>%
        select(tuple, start, end, duration) %>%
        mutate(
            start = format(start, "%Y-%m-%d"),
            end = format(end, "%Y-%m-%d"),
            duration = scales::number(duration, suffix = " days")
        ) %>%
        kbl(
            format = "latex",
            booktabs = T,
            escape = F,
            linesep = "",
            align = "lrrr"
        )
}
