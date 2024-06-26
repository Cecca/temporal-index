# All the code to generate the plots

theme_paper <- function() {
    theme_tufte() +
    theme(
        plot.margin = unit(c(0,0,0,0), "cm")
        # axis.line.x = element_line(),
        # axis.line.y = element_line()
    )
}

scale_color_algorithm <- function() {
    scale_color_manual(values=c(
        "RD-index-td"    = "#e49444",
        "RD-index-dt"    = "#d1615d",
        "Grid-File"      = "#e7ca60",
        "Period-Index*"  = "#a87c9f",
        "R-Tree"         = "#6a9f58",
        "Interval-Tree"  = "#9c755f",
        "B-Tree"         = "#5778a4",
        "HINT"           = "#85b6b2"
    ))
}

scale_color_algorithm2 <- function() {
    scale_color_manual(values=c(
        "RD-index-td"    = "#e49444",
        "RD-index-dt"    = "#d1615d",
        "R-Tree"         = "#6a9f58",
        "Interval-Tree"  = "#9c755f",
        "B-Tree"         = "#5778a4",
        "HINT"           = "#85b6b2"
    ))
}

plot_scalability <- function(data_scalability) {
    data_scalability <- data_scalability %>%
        mutate(
            dataset_name = if_else(dataset_name == "random-uniform-zipf", "Random", dataset_name),
            dataset_name = factor(dataset_name, levels = c("Random", "Flight", "Webkit", "MimicIII"), ordered = T),
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
            algorithm_name = factor(algorithm_name, ordered = T, levels = c(
                "RD-index-td",
                "RD-index-dt",
                "Grid-File",
                "HINT",
                "Period-Index*",
                "R-Tree",
                "Interval-Tree",
                "B-Tree"
            ))
        )
    print(distinct(data_scalability, dataset_name))
    ggplot(data_scalability, aes(
        x = scale,
        y = drop_units(qps),
        color = algorithm_name,
        shape = algorithm_name
    )) +
        geom_point() +
        geom_line() +
        geom_rangeframe(color = "black") +
        scale_x_continuous(
            name = "dataset scale",
            labels = scales::number_format()
        ) +
        scale_y_continuous(
            name = "queries per second",
            labels = scales::number_format()
        ) +
        scale_color_algorithm() +
        scale_shape_discrete(name="") +
        # scale_color_tableau(name = "") +
        # scale_color_tableau(name = "", guide = guide_legend(ncol = 6)) +
        facet_wrap(vars(dataset_name), ncol = 2, scales = "free_y") +
        guides(
            color = guide_legend(nrow = 3, title.position="top"),
            shape = guide_legend(nrow = 3, title.position="top")
        ) +
        # guides(
        #     colour = guide_legend(nrow = 1),
        #     shape = guide_legend(name = "", nrow = 1)
        # ) +
        labs(shape = "", color="") +
        theme_paper() +
        theme(
            legend.position = "top",
            panel.grid.major.y = element_line(
                size = 0.5,
                color = "lightgray",
                linetype = "dotted"
            )
        )
}

plot_parameter_dependency <- function(data_parameter_dependency) {
    distinct_values <- tribble(
        ~dataset_name, ~distinct_values,
        "random-uniform-zipf", 6319382,
        "random-zipf-uniform", 1955006
    ) %>%
        mutate(
            threshold = sqrt(distinct_values)
        )

    plotdata <- data_parameter_dependency %>%
        mutate(start_times_distribution = case_when(
            start_times_distribution == "uniform" ~ "skewed durations",
            start_times_distribution == "zipf" ~ "skewed start times",
            T ~ start_times_distribution
        )) %>%
        group_by(start_times_distribution, workload_type) %>%
        mutate(ratio_to_best = qps / max(qps)) %>%
        inner_join(distinct_values) %>%
        mutate(
            one_column = page_size >= threshold
        )


    inner <- function(toplot) {
        ggplot(
            toplot,
            aes(
                x = page_size,
                y = drop_units(ratio_to_best),
                color = algorithm_name,
                shape = one_column
            )
        ) +
            geom_rangeframe(color = "black") +
            geom_line(aes(group = algorithm_name), linetype = "dotted") +
            geom_point() +
            geom_line() +
            scale_x_continuous(trans = "log10", limits = c(1, NA)) +
            scale_y_continuous(
                trans = "identity",
                labels = scales::number_format(accuracy = 0.1, limits = c(NA, NA))
            ) +
            scale_color_tableau() +
            facet_grid(
                vars(workload_type),
                vars(start_times_distribution),
                scales = "free"
            ) +
            guides(shape = F) +
            labs(
                x = "page size",
                y = "queries per second (ratio to best)",
                color = "algorithm"
            ) +
            theme_paper() +
            theme(
                legend.position = "bottom",
                legend.direction = "horizontal",
            )
    }

    inner(plotdata)
}

plot_real_distribution <- function(data_start, data_duration) {
    data_start <- filter(data_start, name != "Tourism")
    data_duration <- filter(data_duration, name != "Tourism")
    p1 <- ggplot(data_start, aes(x = start_time, weight = count)) +
        geom_histogram(aes(y = stat(count) / sum(count))) +
        facet_wrap(vars(name), ncol = 3, scales = "free") +
        labs(x = "start time", title = "Start time distributions") +
        scale_x_continuous(breaks = scales::pretty_breaks(3)) +
        scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
        theme_paper() +
        theme(
            plot.title = element_text(size = 12),
            axis.title = element_text(size = 10),
            axis.title.y = element_blank(),
            panel.border = element_rect(fill = NA)
        )
    p2 <- ggplot(data_duration, aes(x = duration, weight = count)) +
        geom_histogram(aes(y = stat(count) / sum(count))) +
        facet_wrap(vars(name), ncol = 3, scales = "free") +
        labs(x = "duration", title = "Duration distributions") +
        scale_x_continuous(breaks = scales::pretty_breaks(3)) +
        scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
        theme_paper() +
        theme(
            plot.title = element_text(size = 12),
            axis.title = element_text(size = 10),
            panel.border = element_rect(fill = NA),
            # strip.text = element_blank(),
            axis.title.y = element_blank()
        )
    # plot_grid(p1, p2, ncol = 1)
    p1 | p2
}

plot_query_focus_precision <- function(data_focus) {
    datasets <- data_focus %>% pull(dataset_name)
    assertthat::are_equal(length(datasets), 1)
    stops <- seq(0, 1.0, by = 1 / 32)
    plotdata <- data_focus %>%
        filter(
            matches > 0,
            dataset_name == "random-uniform-zipf",
            str_detect(dataset_params, "n=10000000 ")
        ) %>%
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
        ) %>%
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
            ),
            precision = examined / 10000000
        ) %>%
        group_by(
            algorithm_name,
            selectivity_time_group,
            selectivity_duration_group
        ) %>%
        summarise(precision = mean(precision)) %>%
        ungroup() %>%
        mutate(
            precision_tile =
                precision %>% ntile(20)
        )
    labels_data <- plotdata %>%
        group_by(as.integer((precision_tile - 1) / 4)) %>%
        summarise(
            precision_tile = min(precision_tile),
            max_tile = max(precision_tile),
            min_precision = min(precision),
            max_precision = max(precision)
        ) %>%
        mutate(
            label = scales::number(min_precision, accuracy = 0.1)
        )
    breaks <- c(
        pull(labels_data, precision_tile),
        summarise(labels_data, max(max_tile) + 3) %>% pull()
    )
    labels <- c(
        pull(labels_data, label),
        summarise(labels_data, max(max_precision) %>%
            scales::number(accuracy = 0.1)) %>% pull()
    )

    ggplot(plotdata, aes(
        x = selectivity_time_group,
        y = selectivity_duration_group,
        color = precision_tile,
        fill = precision_tile,
        tooltip = precision
    )) +
        geom_tile() +
        scale_fill_viridis_c(
            breaks = breaks,
            labels = labels,
            option = "inferno",
            direction = -1,
            aesthetics = c("fill", "color")
        ) +
        scale_y_discrete(breaks = c(.25, .5, .75, 1)) +
        scale_x_discrete(breaks = c(.25, .5, .75, 1)) +
        facet_wrap(vars(algorithm_name), ncol = 8) +
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


plot_query_focus <- function(data_focus) {
    datasets <- data_focus %>% pull(dataset_name)
    assertthat::are_equal(length(datasets), 1)
    stops <- seq(0, 1.0, by = 1 / 32)
    plotdata <- data_focus %>%
        filter(matches > 0, dataset_name == "random-uniform-zipf") %>%
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
        ) %>%
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
        ) %>%
        group_by(
            algorithm_name,
            selectivity_time_group,
            selectivity_duration_group
        ) %>%
        summarise(query_time = mean(query_time)) %>%
        ungroup() %>%
        mutate(
            query_time =
                set_units(query_time, "milliseconds"),
            query_time_tile =
                query_time %>% drop_units() %>% ntile(20)
        )
    labels_data <- plotdata %>%
        mutate(query_time = drop_units(query_time)) %>%
        group_by(as.integer((query_time_tile - 1) / 4)) %>%
        summarise(
            query_time_tile = min(query_time_tile),
            max_tile = max(query_time_tile),
            max_time = max(query_time),
            min_time = min(query_time)
        ) %>%
        mutate(
            label = str_c(
                scales::number(min_time, accuracy = 0.1),
                " ms"
            )
        )
    # breaks <- pull(labels_data, query_time_tile)
    # labels <- pull(labels_data, label)
    breaks <- c(
        pull(labels_data, query_time_tile),
        summarise(labels_data, max(max_tile) + 3) %>% pull()
    )
    labels <- c(
        pull(labels_data, label),
        summarise(labels_data, max(max_time) %>%
            scales::number(accuracy = 0.1, suffix = " ms")) %>% pull()
    )

    ggplot(plotdata, aes(
        x = selectivity_time_group,
        y = selectivity_duration_group,
        color = query_time_tile,
        fill = query_time_tile,
        tooltip = query_time
    )) +
        # geom_point_interactive() +
        geom_tile() +
        scale_fill_viridis_c(
            breaks = breaks,
            labels = labels,
            option = "viridis",
            direction = -1,
            aesthetics = c("fill", "color")
        ) +
        scale_y_discrete(breaks = c(.25, .5, .75, 1)) +
        scale_x_discrete(breaks = c(.25, .5, .75, 1)) +
        facet_wrap(vars(algorithm_name), ncol = 8) +
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

plot_selectivity_dependency <- function(data_selectivity, bare = FALSE, strip = TRUE, legend = TRUE) {
    plotdata <- data_selectivity %>%
        filter(matches > 0) %>%
        filter(!(selectivity_time >= 0.99 & selectivity_duration >= 0.99)) %>%
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
        ) %>%
        mutate(
            query_time =
                set_units(query_time, "milliseconds") %>% drop_units(),
            precision = examined / 10000000,
            category = case_when(
                selectivity_time >= 0.99 ~ "duration",
                selectivity_duration >= 0.99 ~ "range",
                TRUE ~ "range-duration"
            )
        )
    inner <- function(data) {
        ggplot(data, aes(
            x = selectivity,
            y = precision,
            color = category,
            alpha = category,
            tooltip = str_c(
                "sel duration: ",
                selectivity_duration,
                "\n",
                "sel time: ",
                selectivity_time
            )
        )) +
            geom_abline(slope = 1, yintercept = 0, inherit.aes = F) +
            geom_point_interactive(size = 0.5) +
            geom_rangeframe(show.legend = FALSE) +
            facet_wrap(vars(algorithm_name), ncol = 8, scales = "free_y") +
            scale_color_manual(values = c(
                "range-duration" = "#414141",
                "duration" = "steelblue",
                # "duration-only" = "#00ccff",
                "range" = "#ff5e00"
            )) +
            scale_alpha_manual(values = c(
                "range-duration" = 0.1,
                "duration" = 1,
                "range" = 1
            )) +
            scale_x_continuous(breaks = c(.25, .5, .75, 1)) +
            scale_y_continuous(limits = c(0, NA)) +
            labs(
                x = "selectivity",
                y = "fraction of intervals",
                color = "category of query"
            ) +
            guides(
                colour = guide_legend(override.aes = list(size = 2)),
                alpha = FALSE
            ) +
            theme_paper() +
            theme(legend.position = "none")
    }


    p <- inner(plotdata)
    if (bare) {
        cat("Bare plot\n")
        p <- p +
            theme(
                legend.position = "none",
                strip.text = element_blank(),
                plot.margin = margin(0, 0, 0, 0, "mm"),
                axis.line = element_line(),
                axis.text = element_text(size = 8),
                axis.title = element_blank()
            )
    }
    if (!strip) {
        p <- p + theme(strip.text = element_blank())
    }
    if (!legend) {
        p <- p + theme(legend.position = "none")
    }
    p
    # p1 <- plotdata %>%
    #     filter(algorithm_name != "period-index-*") %>%
    #     inner()
    # p2 <- plotdata %>%
    #     filter(algorithm_name == "period-index-*") %>%
    #     inner()
    # p1 | p2
}

plot_running_example <- function(data_running_example, query_time, query_duration) {
    limits <- c(0.5, 0.6 + nrow(filter(data_running_example, highlighted)))
    data_running_example <- data_running_example %>%
        filter(highlighted) %>%
        mutate(pos = row_number(start))

    ggplot(
        data_running_example,
        aes(
            x = start, xend = start + duration,
            y = pos, yend = pos
        )
    ) +
        geom_rect(
            xmin = as_date(int_start(query_time)),
            xmax = as_date(int_end(query_time)),
            ymin = limits[1],
            ymax = limits[2],
            inherit.aes = F,
            fill = "red",
            alpha = 0.02
        ) +
        geom_segment(
            aes(color = matches),
            size = 1.5,
            show.legend = F
        ) +
        # geom_vline(
        #     xintercept = c(
        #         as_date(int_start(query_time)),
        #         as_date(int_end(query_time))
        #     ),
        #     linetype = "dashed",
        #     color = "red"
        # ) +
        geom_text(
            aes(label = str_c("$r_", pos, "$")),
            size = 3,
            nudge_y = +0.3,
            hjust = 0.3,
            vjust = 0
            # label.padding = unit(0.1, "lines"),
            # label.size = 0
        ) +
        geom_point(
            aes(x = start, y = pos - 0.2),
            color = "red",
            alpha = 0.7,
            size = 1
        ) +
        geom_segment(
            aes(
                x = start,
                xend = start + days(query_duration[1]),
                y = pos - 0.2, yend = pos - 0.2
            ),
            alpha = 0.7,
            size = 0.6,
            color = "red"
        ) +
        geom_segment(
            aes(
                x = start + days(query_duration[1]),
                xend = start + days(query_duration[2]),
                y = pos - 0.2, yend = pos - 0.2
            ),
            alpha = 0.7,
            size = 0.8,
            color = "red",
            linetype = "dotted"
        ) +
        scale_x_date(limits = c(ymd("2016-06-05"), ymd("2016-07-20"))) +
        scale_y_continuous(limits = limits) +
        scale_color_manual(values = list(
            "black",
            # "#0ec487"
            "blue"
        )) +
        theme_minimal() +
        labs(x = "time") +
        theme(
            axis.line.x.bottom = element_line(color = "darkgray"),
            axis.text.y = element_blank(),
            axis.title.y = element_blank(),
            axis.title.x = element_blank(),
            panel.grid.major.y = element_blank(),
            panel.grid.minor.y = element_blank(),
            plot.margin = margin()
        )
}

plot_running_example_plane <- function(data_running_example, query_range, query_duration, grid = FALSE) {
    cell_size <- 30
    column_size <- cell_size * cell_size

    p <- data_running_example %>%
        mutate(
            duration = as.double(duration)
        ) %>%
        ggplot(aes(
            y = duration,
            x = start
        )) +
        geom_point(
            aes(color = highlighted, size = highlighted, alpha = highlighted),
            position = position_jitter(0.01),
            show.legend = F
        )
    # geom_point(
    #     aes(color = highlighted, size = highlighted),
    #     data = data_running_example %>% filter(highlighted),
    #     show.legend = F
    # )

    # add the grid, if requested
    if (grid) {
        algo_grid <- data_running_example %>%
            ungroup() %>%
            group_by(col_id = ntile(start, n() / column_size)) %>%
            mutate(
                time_min = min(start),
                time_max = max(start),
                cell_id = ntile(desc(duration), n() / cell_size)
            ) %>%
            group_by(col_id, cell_id) %>%
            mutate(
                duration_min = min(duration),
                duration_max = max(duration)
            ) %>%
            ungroup() %>%
            arrange(col_id, cell_id) %>%
            distinct(col_id, cell_id, time_min, time_max, duration_min, duration_max)
        p <- p +
            geom_vline(
                aes(xintercept = time_min),
                data = algo_grid,
                alpha = 1,
                # color = "#56B4E9",
                color = "black",
                size = 0.5
            ) +
            geom_segment(
                aes(
                    y = duration_min + 0.4, yend = duration_min + 0.4,
                    x = time_min, xend = time_max
                ),
                color = "black",
                data = algo_grid,
                size = 0.5,
                inherit.aes = F
            )
    }
    p <- p +
        # annotate(
        #     geom = "polygon",
        #     y = c(
        #         query_duration[1],
        #         query_duration[2],
        #         query_duration[2],
        #         query_duration[1]
        #     ),
        #     x = c(
        #         int_end(query_range),
        #         int_end(query_range),
        #         int_start(query_range) - 3600 * query_duration[2],
        #         int_start(query_range) - 3600 * query_duration[1]
        #     ),
        #     fill = "red",
        #     color = "red",
        #     size = 1,
        #     alpha = 0.0
        # ) +
        scale_color_manual(values = c("#56B4E9", "black")) +
        scale_size_manual(values = c(0.5, 2)) +
        scale_alpha_manual(values = c(0.5, 1)) +
        scale_x_date(
            # date_labels = "%H:%M",
            # date_breaks = "3 hours"
        ) +
        scale_y_continuous(
            # breaks = c(0, 2, 4, 6, 8, 10),
            # labels = scales::number_format()
        ) +
        labs(y = "duration (days)", x = "start time") +
        theme_minimal() +
        theme(
            text = element_text(size = 9),
            axis.ticks = element_line(color = "black"),
            axis.line.x.bottom = element_line(color = "black"),
            axis.line.y.left = element_line(color = "black"),
            panel.grid = element_blank()
        )

    p
}

plot_running_example_mimic <- function(query_range, query_duration, grid = FALSE) {
    dataset <- read_csv(here::here("example_rdindex/example_dataset.csv")) %>%
        filter(duration <= 50)

    maxduration <- dataset %>%
        pull(duration) %>%
        max()
    minduration <- dataset %>%
        pull(duration) %>%
        min()

    columns <- read_csv(here::here("example_rdindex/column_info.csv")) %>%
        filter(i != 4) %>%
        arrange(i) %>%
        mutate(column_end = lead(column_bound)) %>%
        replace_na(list("column_end" = ymd("2016-12-31")))
    cells <- read_csv(here::here("example_rdindex/cell_info.csv")) %>%
        inner_join(columns)

    p <- ggplot(dataset, aes(start, duration)) +
        geom_point(size = 0.2, shape = 16, color = "gray0", alpha = 0.7) +
        annotate(
            geom = "polygon",
            y = c(
                query_duration[1],
                query_duration[2],
                query_duration[2],
                query_duration[1]
            ),
            x = c(
                as_date(int_end(query_range)),
                as_date(int_end(query_range)),
                as_date(int_start(query_range)) - query_duration[2],
                as_date(int_start(query_range)) - query_duration[1]
            ),
            fill = "red",
            color = "red",
            size = 0.6,
            alpha = 0.0
        ) +
        scale_x_date(date_labels = "%b") +
        theme_minimal() +
        theme(
            text = element_text(size = 9),
            axis.ticks = element_line(color = "black"),
            axis.line.x.bottom = element_line(color = "black"),
            axis.line.y.left = element_line(color = "black"),
            panel.grid = element_blank()
        )

    if (grid) {
        p <- p +
            annotate(
                geom = "linerange",
                y = maxduration,
                xmin = ymd("2016-01-01"),
                xmax = ymd("2016-12-31"),
                size = 0.2,
                color = "forestgreen",
                linetype = "solid"
            ) +
            annotate(
                geom = "linerange",
                x = ymd("2016-12-31"),
                ymin = minduration - 0.4,
                ymax = maxduration,
                size = 0.2,
                color = "forestgreen",
                linetype = "solid"
            ) +
            geom_linerange(
                data = columns,
                mapping = aes(x = column_bound),
                ymin = minduration - 0.4,
                ymax = maxduration,
                inherit.aes = F,
                size = 0.2,
                color = "forestgreen",
                linetype = "solid"
            ) +
            # geom_text(
            #     data = columns,
            #     mapping = aes(
            #         x = column_bound,
            #         label = strftime(column_bound, "%b, %d")
            #     ),
            #     y = maxduration + 9,
            #     nudge_x = 3,
            #     vjust = 1,
            #     hjust = 0,
            #     size = 2.5,
            #     angle = 0,
            #     inherit.aes = F
            # ) +
            # geom_text(
            #     data = columns,
            #     mapping = aes(
            #         x = column_bound,
            #         label = strftime(latest_end_time, "%b, %d")
            #     ),
            #     y = maxduration + 5,
            #     nudge_x = 3,
            #     vjust = 1,
            #     hjust = 0,
            #     size = 2.5,
            #     color = "gray40",
            #     angle = 0,
            #     inherit.aes = F
            # ) +
            geom_linerange(
                data = cells,
                mapping = aes(
                    y = cell_bound - 0.4,
                    xmin = column_bound,
                    xmax = column_end
                ),
                size = 0.2,
                color = "forestgreen",
                linetype = "solid",
                inherit.aes = FALSE
            )

        arrays <- ggplot(dataset, aes(start)) +
            # geom_point(y = 0 +
            geom_text(
                data = columns,
                mapping = aes(
                    x = column_bound,
                    label = strftime(column_bound, "%b, %d")
                ),
                y = 2,
                nudge_x = 3,
                vjust = 1,
                hjust = 0,
                size = 2,
                angle = 0,
                inherit.aes = F
            ) +
            geom_text(
                data = columns,
                mapping = aes(
                    x = column_bound,
                    label = strftime(latest_end_time, "%b, %d")
                ),
                y = 0,
                nudge_x = 3,
                vjust = 1,
                hjust = 0,
                size = 2,
                color = "gray40",
                angle = 0,
                inherit.aes = F
            ) +
            geom_text(
                label = "col_minstart:",
                x = ymd('2016-01-01'),
                y = 2,
                data = function(d) {head(d, 1)},
                nudge_x = -9,
                vjust = 1,
                hjust = 1,
                size = 2,
            ) +
            geom_text(
                label = "col_maxend:",
                x = ymd('2016-01-01'),
                y = 0,
                data = function(d) {head(d, 1)},
                color = "gray40",
                nudge_x = -9,
                vjust = 1,
                hjust = 1,
                size = 2,
            ) +
            scale_y_continuous(limits=c(-1, 2)) +
            scale_x_date(date_labels = "%b", expand = expansion(mult=c(.05, .1))) +
            coord_cartesian(clip="off") +
            theme_void()
        p <- patchwork::wrap_plots(arrays, p, ncol=1, heights = c(1, 7))
    }
    p
}

plot_insertions <- function(data_insertions, sorted=F, legend=T) {
    plotdata <- data_insertions %>%
        filter(na_or_in(page_size, c(200))) %>%
        mutate(
            is_sorted = str_detect(dataset_name, "sorted"),
            dataset_name = str_remove(dataset_name, "sorted-"),
            dataset_name = if_else(dataset_name == "random-uniform-zipf", "Random", dataset_name),
            dataset_name = factor(dataset_name, levels = c("Random", "Flight", "Webkit", "MimicIII"), ordered = T),
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
        ) %>%
        group_by(dataset_name, algorithm_name, algorithm_params) %>%
        mutate(
            batch = percent_rank(batch)
        )

    legend_position = 'top'
    if (!legend) {
        legend_position = 'none'
    }

    doplot <- function(pdata) {
        ggplot(pdata, aes(
                x = batch, 
                y = batch_insertions_per_second,
                color = algorithm_name,
                group = interaction(algorithm_name, algorithm_params)
            )) +
            geom_line() +
            geom_hline(yintercept=0) +
            geom_vline(xintercept=0) +
            scale_y_log10(limits=c(500, 100000000)) +
            scale_x_continuous(labels = scales::percent_format()) +
            scale_color_algorithm2() +
            facet_wrap(vars(dataset_name), ncol = 1, scales="fixed") +
            labs(
                x = "dataset fraction",
                y = "insertions per second",
                color = ""
            ) +
            theme_paper() +
            theme(
                legend.position = legend_position,
                legend.margin=margin(c(0,0,0,0), unit='cm')
            )
    }
    # p1 <- (plotdata %>% filter(!is_sorted) %>% doplot()) + ggtitle("(a) Random order")
    # p2 <- (plotdata %>% filter(is_sorted) %>% doplot()) + ggtitle("(a) By increasing start time")
    # # guide_area() + p1 + p2 + plot_layout(ncol=1, guides="collect", heights=c(1,2,2))
    # guide_area() / (p1 | p2) + plot_layout(guides="collect", heights=c(1, 7))
    if (sorted) {
        plotdata %>% filter(is_sorted) %>% doplot()
    } else {
        plotdata %>% filter(!is_sorted) %>% doplot()
    }
}

plot_simulated_tradeoff <- function(simulated_tradeoff, col = frac_dur, xlab = "Fraction of duration queries") {
    breakpoints <- simulated_tradeoff %>%
        filter(algorithm %in% c("RD-index-dt", "B-Tree")) %>%
        select(dataset, algorithm, qps, {{ col }}) %>%
        pivot_wider(names_from=algorithm, values_from=qps) %>%
        group_by(dataset) %>%
        filter(`RD-index-dt` > `B-Tree`) %>%
        slice_max({{ col }})

    simulated_tradeoff %>% 
        ggplot(aes({{ col }}, qps, color=algorithm)) + 
        geom_line() +
        geom_text_repel(
            aes(x = {{ col }}, y=`RD-index-dt`, label = {{ col }}),
            data=breakpoints, 
            inherit.aes=F,
            box.padding = 0.5, 
            max.overlaps = 0,
            nudge_x = -0.15,
            nudge_y = 1000,
            size = 3,
            segment.size = 0.2
        ) +
        scale_y_log10() + 
        scale_color_algorithm() +
        labs(
            x = xlab,
            y = "Queries per second"
        ) +
        facet_wrap(vars(dataset), ncol=4, scales="free_y") +
        theme_paper() +
        theme(
            legend.position = "bottom",
            panel.border = element_rect(fill=NA)
        )
}

plot_tradeoff_tern_all <- function(simulated_tradeoff_tern) {
    simulated_tradeoff_tern %>%
        group_by(dataset) %>%
        mutate(
            rank = ntile(qps, n = 100)
        ) %>%
        ggtern(aes(x = frac_ro, y = frac_do, z = frac_rd, color = rank)) +
        geom_point(size = 0.2) +
        scale_color_viridis_c() +
        facet_grid(vars(algorithm), vars(dataset)) +
        labs(
            x = "R",
            y = "D",
            z = "RD",
            color = "Throughput percentile (dataset-wise)"
        ) +
        theme_paper() +
        theme(
            legend.position = "bottom",
            panel.spacing = unit(5, "mm"),
            axis.title = element_text(size = 8)
        )
}

plot_tradeoff_tern_algo <- function(simulated_tradeoff_tern) {
    simulated_tradeoff_tern %>%
        group_by(dataset, frac_ro, frac_do, frac_rd) %>%
        slice_max(qps, n=1) %>%
        ggtern(aes(x = frac_ro, y = frac_do, z = frac_rd, color = algorithm)) +
        geom_point(size = 0.1) +
        guides(colour = guide_legend(override.aes = list(size=3),
                                     title.position = "top")) +
        scale_color_algorithm() +
        facet_wrap(vars(dataset), ncol=2) +
        labs(
            x = "R",
            y = "D",
            z = "RD",
            color = "Fastest index"
        ) +
        theme_paper() +
        theme(
            legend.title = element_blank(),
            legend.position = "right",
            panel.spacing = unit(4, "mm"),
            axis.title = element_text(size = 7)
        )

}
