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
        facet_wrap(vars(dataset_name)) +
        theme_paper()
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
        facet_wrap(vars(start_times_distribution), scales = "fixed") +
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
                query_time %>% drop_units() %>% ntile(8)
        )
    labels_data <- plotdata %>%
        mutate(query_time = drop_units(query_time)) %>%
        group_by(query_time_tile) %>%
        summarise(
            max_time = max(query_time),
            min_time = min(query_time)
        ) %>%
        mutate(
            label = str_c(
                scales::number(max_time, accuracy = 0.1),
                " ms"
            )
        ) # %>%
    # filter(query_time_tile %in% c(2, 4, 6, 8, 10))
    breaks <- pull(labels_data, query_time_tile)
    labels <- pull(labels_data, label)

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
            category = case_when(
                selectivity_time >= 0.99 ~ "duration-only",
                selectivity_duration >= 0.99 ~ "time-only",
                TRUE ~ "both"
            )
        )

    ggplot(plotdata, aes(
        x = selectivity,
        y = query_time,
        color = category,
        tooltip = str_c(
            "sel duration: ",
            selectivity_duration,
            "\n",
            "sel time: ",
            selectivity_time
        )
    )) +
        geom_point_interactive(size = 0.5) +
        geom_rangeframe(show.legend = FALSE) +
        facet_wrap(vars(algorithm_name), ncol = 5) +
        scale_color_manual(values = c(
            "both" = "#414141",
            "duration-only" = "#00ccff",
            "time-only" = "#ff5e00"
        )) +
        scale_x_continuous(breaks = c(.25, .5, .75, 1)) +
        labs(
            x = "selectivity",
            y = "query time (ms)"
        ) +
        guides(colour = guide_legend(override.aes = list(size = 2))) +
        theme_paper() +
        theme(legend.position = "top")
}

plot_running_example <- function(data_running_example) {
    limits <- c(-1, 3)
    ggplot(
        data_running_example,
        aes(
            x = departure, xend = arrival,
            y = pos, yend = pos
        )
    ) +
        geom_rect(
            xmin = ymd_hms("2021-01-01T10:00:00"),
            xmax = ymd_hms("2021-01-01T14:00:00"),
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
        geom_text(
            aes(label = flight),
            size = 3,
            nudge_y = 0.3
        ) +
        geom_vline(
            xintercept = c(
                ymd_hms("2021-01-01T10:00:00"),
                ymd_hms("2021-01-01T14:00:00")
            ),
            linetype = "dashed",
            color = "red"
        ) +
        annotate(
            geom = "text",
            label = "$d \\in [2, 4]$ hours",
            color = "darkred",
            x = ymd_hms("2021-01-01T12:00:00"),
            y = 2,
            size = 3,
            vjust = 0
        ) +
        scale_x_datetime() +
        scale_y_continuous(limits = limits) +
        scale_color_manual(values = list(
            "black",
            "#0ec487"
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
    set.seed(1234)
    span_time <- c(
        ymd_hms("2021-01-01T05:00:00"),
        ymd_hms("2021-01-01T18:00:00")
    )
    n_random <- 1000
    random_starts <- runif(n_random, span_time[1], span_time[2]) %>% as_datetime()
    random_durations <- rexp(n_random, rate = 0.5) + 0.75
    random_data <- tibble(
        departure = random_starts,
        duration = random_durations
    ) %>%
        filter(duration < 10)

    algo_grid <- random_data %>%
        ungroup() %>%
        group_by(
            col_id = ntile(duration, 5),
            cell_id = ntile(departure, 5)
        ) %>%
        mutate(
            time_min = min(departure),
            time_max = max(departure)
        ) %>%
        ungroup() %>%
        group_by(col_id) %>%
        mutate(
            duration_min = min(duration),
            duration_max = max(duration)
        ) %>%
        ungroup() %>%
        arrange(col_id, cell_id) %>%
        distinct(col_id, cell_id, time_min, time_max, duration_min, duration_max)

    p <- data_running_example %>%
        mutate(duration = as.double(duration)) %>%
        ggplot(aes(
            x = duration,
            y = departure
        )) +
        geom_point(
            data = random_data,
            size = .1,
            color = "darkgray"
        ) +
        geom_point(
            size = 1
        )
    if (grid) {
        p <- p +
            geom_vline(
                aes(xintercept = duration_min),
                data = algo_grid,
                alpha = 1,
                color = "black",
                size = 0.3
            ) +
            geom_segment(
                aes(
                    x = duration_min, xend = duration_max,
                    y = time_min, yend = time_min
                ),
                data = algo_grid,
                size = 0.3,
                inherit.aes = F
            )
    }
    p <- p + annotate(
        geom = "polygon",
        x = c(
            query_duration[1],
            query_duration[2],
            query_duration[2],
            query_duration[1]
        ),
        y = c(
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
        geom_label(
            aes(label = flight),
            size = 2,
            vjust = 0,
            hjust = 1,
            nudge_x = -0.1,
            nudge_y = 0.1
        ) +
        labs(x = "duration (hours)", y = "departure time") +
        theme_minimal() +
        theme(
            text = element_text(size = 9),
            axis.line.x.bottom = element_line(color = "black"),
            axis.line.y.left = element_line(color = "black"),
            panel.grid = element_blank()
        )
}