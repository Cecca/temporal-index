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
    print(select(
        data_parameter_dependency,
        start_times_distribution, workload_type, page_size, qps
    ))
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

table_parameter_dependency() %>%
    plot_parameter_dependency()