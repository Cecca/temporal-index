# All the code to generate the plots

theme_paper <- function() {
    theme_tufte()
}

plot_scalability <- function(data_scalability) {
    ggplot(data_scalability, aes(
        x = scale,
        y = drop_units(qps),
        color = algorithm_name
    )) +
        geom_point() +
        geom_line() +
        geom_rangeframe(color = "black") +
        scale_x_log10(
            name = "dataset scale",
            labels = scales::number_format()
        ) +
        scale_y_log10(
            name = "queries per second",
            labels = scales::number_format()
        ) +
        scale_color_tableau() +
        facet_wrap(vars(dataset_name), ncol = 4) +
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
    ggplot(
        data_parameter_dependency,
        aes(
            x = page_size,
            y = drop_units(qps),
            color = workload_type
        )
    ) +
        geom_point() +
        geom_line() +
        scale_x_continuous(trans = "log10", limits = c(10, NA)) +
        scale_y_continuous(trans = "log10", limits = c(NA, NA)) +
        scale_color_workload() +
        facet_grid(vars(algorithm_name), vars(start_times_distribution), scales = "fixed") +
        labs(
            x = "page size",
            y = "queries per second",
            color = "workload"
        ) +
        theme_bw() +
        theme(
            legend.position = "bottom",
            legend.direction = "horizontal"
        )
}

plot_real_distribution <- function(data_start, data_duration) {
    p1 <- ggplot(data_start, aes(x = start_time, weight = count)) +
        geom_histogram(aes(y = stat(count) / sum(count))) +
        facet_wrap(vars(name), scales = "free") +
        labs(x = "start time") +
        scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
        theme_paper() +
        theme(axis.title.y = element_blank())
    p2 <- ggplot(data_duration, aes(x = duration, weight = count)) +
        geom_histogram(aes(y = stat(count) / sum(count))) +
        facet_wrap(vars(name), scales = "free") +
        labs(x = "duration") +
        scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
        theme_paper() +
        theme(
            strip.text = element_blank(),
            axis.title.y = element_blank()
        )
    plot_grid(p1, p2, ncol = 1)
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
            aesthetics = c("fill", "color")
        ) +
        scale_y_discrete(breaks = c(.25, .5, .75, 1)) +
        scale_x_discrete(breaks = c(.25, .5, .75, 1)) +
        facet_wrap(vars(algorithm_name), ncol = 5) +
        labs(
            x = "time selectivity",
            y = "duration selectivity"
        ) +
        theme_paper() +
        theme(
            legend.title = element_blank(),
            legend.position = "bottom",
            legend.key.width = unit(30, "mm")
        )
}


plot_query_focus <- function(data_focus) {
    datasets <- data_focus %>% pull(dataset_name)
    assertthat::are_equal(length(datasets), 1)
    stops <- seq(0, 1.0, by = 1 / 32)
    plotdata <- data_focus %>%
        filter(matches > 0, dataset_name == "random-uniform-zipf") %>%
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
            aesthetics = c("fill", "color")
        ) +
        scale_y_discrete(breaks = c(.25, .5, .75, 1)) +
        scale_x_discrete(breaks = c(.25, .5, .75, 1)) +
        facet_wrap(vars(algorithm_name), ncol = 5) +
        labs(
            x = "time selectivity",
            y = "duration selectivity"
        ) +
        theme_paper() +
        theme(
            legend.title = element_blank(),
            legend.position = "bottom",
            legend.key.width = unit(30, "mm")
        )
}

plot_selectivity_dependency <- function(data_selectivity) {
    plotdata <- data_selectivity %>%
        filter(matches > 0) %>%
        filter(!(selectivity_time >= 0.99 & selectivity_duration >= 0.99)) %>%
        mutate(
            query_time =
                set_units(query_time, "milliseconds") %>% drop_units(),
            precision = examined / 10000000,
            category = case_when(
                selectivity_time >= 0.99 ~ "duration-only",
                selectivity_duration >= 0.99 ~ "time-only",
                TRUE ~ "both"
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
            facet_wrap(vars(algorithm_name), ncol = 5, scales = "free_y") +
            scale_color_manual(values = c(
                "both" = "#414141",
                "duration-only" = "steelblue",
                # "duration-only" = "#00ccff",
                "time-only" = "#ff5e00"
            )) +
            scale_alpha_manual(values = c(
                "both" = 0.1,
                "duration-only" = 1,
                "time-only" = 1
            )) +
            scale_x_continuous(breaks = c(.25, .5, .75, 1)) +
            scale_y_continuous(limits = c(0, NA)) +
            labs(
                x = "selectivity",
                y = "fraction of intervals examined",
                color = "category of query"
            ) +
            guides(
                colour = guide_legend(override.aes = list(size = 2)),
                alpha = FALSE
            ) +
            theme_paper() +
            theme(legend.position = "top")
    }


    inner(plotdata)
    # p1 <- plotdata %>%
    #     filter(algorithm_name != "period-index-*") %>%
    #     inner()
    # p2 <- plotdata %>%
    #     filter(algorithm_name == "period-index-*") %>%
    #     inner()
    # p1 | p2
}

plot_running_example <- function(data_running_example, query_time, query_duration) {
    limits <- c(-0.5, 0.5 + nrow(filter(data_running_example, highlighted)))
    data_running_example <- data_running_example %>%
        filter(highlighted) %>%
        mutate(pos = row_number(desc(departure)))

    ggplot(
        data_running_example,
        aes(
            x = departure, xend = arrival,
            y = pos, yend = pos
        )
    ) +
        geom_rect(
            xmin = int_start(query_time),
            xmax = int_end(query_time),
            ymin = limits[1],
            ymax = limits[2],
            inherit.aes = F,
            fill = "red",
            alpha = 0.01
        ) +
        geom_segment(
            aes(color = matches),
            size = 1.5,
            show.legend = F
        ) +
        geom_vline(
            xintercept = c(
                int_start(query_time),
                int_end(query_time)
            ),
            linetype = "dashed",
            color = "red"
        ) +
        geom_label(
            aes(label = flight),
            size = 3,
            nudge_y = -0.2,
            hjust = 0.3,
            vjust = 1,
            label.padding = unit(0.1, "lines"),
            label.size = 0
        ) +
        geom_point(
            aes(x = departure, y = pos + 0.2),
            color = "gray",
            alpha = 0.7,
            size = 1
        ) +
        geom_segment(
            aes(
                x = departure,
                xend = departure + hours(2),
                y = pos + 0.2, yend = pos + 0.2
            ),
            alpha = 0.7,
            size = 1,
            color = "gray"
        ) +
        geom_segment(
            aes(
                x = departure + hours(2),
                xend = departure + hours(4),
                y = pos + 0.2, yend = pos + 0.2
            ),
            alpha = 0.7,
            size = 1.5,
            color = "forestgreen"
        ) +
        scale_x_datetime() +
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
    p <- data_running_example %>%
        mutate(
            duration = as.double(duration)
        ) %>%
        ggplot(aes(
            y = duration,
            x = departure
        )) +
        geom_point(
            aes(color = highlighted, size = highlighted),
            show.legend = F
        ) +
        geom_point(
            aes(color = highlighted, size = highlighted),
            data = data_running_example %>% filter(highlighted),
            show.legend = F
        )

    # add the grid, if requested
    if (grid) {
        algo_grid <- data_running_example %>%
            ungroup() %>%
            group_by(
                col_id = ntile(departure, 5)
            ) %>%
            mutate(
                time_min = min(departure),
                time_max = max(departure),
                cell_id = ntile(duration, 5)
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
                color = "steelblue",
                size = 0.8
            ) +
            geom_segment(
                aes(
                    y = duration_min, yend = duration_min,
                    x = time_min, xend = time_max
                ),
                color = "steelblue",
                data = algo_grid,
                size = 0.8,
                inherit.aes = F
            )
    }
    p <- p + annotate(
        geom = "polygon",
        y = c(
            query_duration[1],
            query_duration[2],
            query_duration[2],
            query_duration[1]
        ),
        x = c(
            int_end(query_range),
            int_end(query_range),
            int_start(query_range) - 3600 * query_duration[2],
            int_start(query_range) - 3600 * query_duration[1]
        ),
        fill = "red",
        color = "red",
        size = 1,
        alpha = 0.0
    ) +
        scale_color_manual(values = c("darkgray", "black")) +
        scale_size_manual(values = c(0.5, 2)) +
        scale_x_datetime(
            date_labels = "%H:%M",
            date_breaks = "3 hours"
        ) +
        scale_y_continuous(
            breaks = c(0, 2, 4, 6, 8, 10),
            labels = scales::number_format()
        ) +
        labs(y = "duration (hours)", x = "departure time") +
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