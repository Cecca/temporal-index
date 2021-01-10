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