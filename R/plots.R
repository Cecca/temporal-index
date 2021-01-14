# All the code to generate the plots

theme_paper <- function() {
    theme_tufte()
}

plot_scalability <- function(data_scalability) {
    ggplot(data_scalability, aes(
        x = dataset_n,
        y = drop_units(qps),
        color = algorithm_name
    )) +
        geom_point() +
        geom_line() +
        geom_rangeframe(color = "black") +
        scale_x_log10(
            name = "dataset size",
            labels = scales::number_format()
        ) +
        scale_y_log10(
            name = "queries per second",
            labels = scales::number_format()
        ) +
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

table_query_focus() %>% plot_query_focus()