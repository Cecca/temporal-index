
barchart_qps <- function(dataset) {
  # Assert that we are dealing with a single data and query configuration
  plot_label <- build_plot_label(dataset)

  breaks <- pretty(pull(dataset, qps))
  dataset %>%
    mutate(algorithm_wpar = fct_reorder(algorithm_wpar, qps)) %>%
    ggplot(aes(x=algorithm_wpar,
                y=qps,
                fill=algorithm)) +
        geom_col() +
        geom_hline(yintercept=breaks, col="white", lwd=.5) +
        geom_text(aes(label=scales::number(drop_units(qps), accuracy=1)),
                size=3,
                hjust=0,
                vjust=0.5,
                nudge_y=10) +
        scale_y_unit(breaks=breaks) +
        # scale_fill_discrete_qualitative() +
        scale_fill_algorithm() +
        coord_flip() +
        labs(caption=plot_label) +
        theme_tufte() +
        theme(
        panel.ontop = FALSE,
        panel.grid.minor.x = element_blank(),
        panel.grid.major.y = element_blank(),
        panel.grid.minor.y = element_blank(),
        legend.position = "none",
        axis.title.y = element_blank()
        )
}

distribution_normalized_latency <- function(dataset) {
  # Assert that we are dealing with a single data and query configuration
  plot_label <- build_plot_label(dataset)
  
  plotdata <- dataset %>%
    mutate(algo_wpar = str_c(algorithm, algorithm_params, sep=".")) %>%
    mutate(normalized_query_time = drop_units(normalized_query_time)) %>%
    filter(query_count > 0, algorithm != "linear-scan")

  support_data <- plotdata %>%
    group_by(algorithm, algo_wpar) %>%
    summarise(
      m_normalized_time = median(normalized_query_time),
      max_normalized_time = max(normalized_query_time),
      normalized_time_90 = quantile(normalized_query_time, probs=c(.9)),
      iqr_normalized_time = IQR(normalized_query_time),
      threshold = m_normalized_time + iqr_normalized_time*.75
    ) %>%
    ungroup() %>%
    group_by(algorithm) %>%
    slice(which.min(m_normalized_time)) %>%
    ungroup()

  outliers <- plotdata %>%
    inner_join(support_data) %>%
    filter(normalized_query_time > threshold)
  
  non_outliers <- plotdata %>%
    inner_join(support_data) %>%
    mutate(algorithm = fct_reorder(algorithm, desc(m_normalized_time))) %>%
    filter(normalized_query_time <= threshold)

  max_val <- non_outliers %>% pull(normalized_query_time) %>% max()

  non_outliers %>%
    ggplot(aes(x=normalized_query_time, y=algorithm, fill=algorithm)) +
    geom_density_ridges(scale=.8, size=0.1,
                        rel_min_height = 0.01) + 
    geom_point(aes(x=m_normalized_time, y=algorithm),
               shape=17,
               size=1,
               data=support_data) +
    geom_text(aes(label=scales::number(m_normalized_time), x=m_normalized_time, y=algorithm),
              nudge_y=-.1,
              size=3,
              data=support_data) +
    geom_text(aes(label=scales::number(normalized_time_90, prefix="→ "), y=algorithm),
              x=max_val,
              nudge_y=.2,
              size=3,
              hjust=1,
              data=support_data) +
    scale_x_continuous(limits=c(0, NA), labels=scales::number_format()) +
    scale_fill_algorithm() +
    labs(y="algorithm",
         x="normalized query time (ns/interval)",
         title="Distribution of normalized query times",
         caption=plot_label) +
    theme_tufte() +
    theme(legend.pos="none",
          panel.grid.major.y=element_line(color="lightgray", size=.2))

}

distribution_latency <- function(dataset) {
  # Assert that we are dealing with a single data and query configuration
  plot_label <- build_plot_label(dataset)
  
  plotdata <- dataset %>%
    mutate(algo_wpar = str_c(algorithm, algorithm_params, sep="."),
           query_time = drop_units(query_time)) %>%
    filter(query_count > 0, algorithm != "linear-scan")

  support_data <- plotdata %>%
    group_by(algorithm, algo_wpar) %>%
    summarise(
      m_time = median(query_time),
      max_time = max(query_time),
      time_90 = quantile(query_time, probs=c(.9)),
      threshold = m_time + .75 * IQR(query_time)
    ) %>%
    ungroup() %>%
    group_by(algorithm) %>%
    slice(which.min(m_time)) %>%
    ungroup()
  
  non_outliers <- plotdata %>%
    inner_join(support_data) %>%
    mutate(algorithm = fct_reorder(algorithm, desc(m_time))) %>%
    filter(query_time <= threshold)

  max_val <- non_outliers %>% pull(query_time) %>% max()

  non_outliers %>%
    ggplot(aes(x=query_time, y=algorithm, fill=algorithm)) +
    geom_density_ridges(scale=.8, size=0.1,
                        rel_min_height = 0.01) +
    geom_point(aes(x=m_time, y=algorithm),
               shape=17,
               size=1,
               data=support_data) +
    geom_text(aes(label=scales::number(m_time), x=m_time, y=algorithm),
              nudge_y=-.1,
              size=3,
              data=support_data) +
    geom_text(aes(label=scales::number(time_90, prefix="→ "), 
                  y=algorithm),
              x=max_val,
              hjust=1,
              nudge_y=.2,
              size=3,
              data=support_data) +
    scale_x_continuous(limits=c(0, NA), labels=scales::number_format()) +
    scale_fill_algorithm() +
    labs(y="algorithm",
         x="query time (ns)",
         title="Distribution of query times",
         caption=plot_label) +
    theme_tufte() +
    theme(legend.pos="none",
          panel.grid.major.y=element_line(color="lightgray", size=.2))

}

distribution_overhead <- function(dataset) {
  # Assert that we are dealing with a single data and query configuration
  plot_label <- build_plot_label(dataset)
  
  plotdata <- dataset %>%
    mutate(algo_wpar = str_c(algorithm, algorithm_params, sep="."),
           query_time = drop_units(query_time)) %>%
    filter(query_count > 0, algorithm != "linear-scan")

  support_data <- plotdata %>%
    group_by(algorithm, algo_wpar) %>%
    summarise(
      m_precision = median(precision),
      max_precision = max(precision),
      precision_90 = quantile(precision, probs=c(.9)),
      threshold = m_precision + .75 * IQR(precision)
    ) %>%
    ungroup() %>%
    group_by(algorithm) %>%
    slice(which.min(m_precision)) %>%
    ungroup()

  plotdata <- plotdata %>%
    inner_join(support_data) %>%
    mutate(algorithm = fct_reorder(algorithm, m_precision))

  plotdata %>%
    ggplot(aes(x=precision, y=algorithm, fill=algorithm)) +
    geom_density_ridges(scale=.8, size=0.1,
                        rel_min_height = 0.01) +
    geom_point(aes(x=m_precision, y=algorithm),
               shape=17,
               size=1,
               data=support_data) +
    geom_text(aes(label=scales::number(m_precision, accuracy=.001), x=m_precision, y=algorithm),
              nudge_y=-.5,
              size=3,
              data=support_data) +
    scale_x_continuous(limits=c(0, 1)) +
    scale_fill_algorithm() +
    labs(x="algorithm",
         y="query precision",
         title="Distribution of query precision",
         subtitle="Ratio between output intervals and examined intervals, per query",
         caption=plot_label) +
    theme_tufte() +
    theme(legend.pos="none",
          panel.grid.major.y=element_line(color="lightgray", size=.2))

}


scale_fill_algorithm <- function() {
  colors <- RColorBrewer::brewer.pal(n=8, name="Set2")
  colors <- ggthemes::tableau_color_pal(type="regular")
  algorithms <- c(
    "BTree",
    "period-index",
    "period-index-*",
    "period-index++",
    "grid",
    "grid3D",       
    "interval-tree", 
    "linear-scan",  
    "NestedBTree",  
    "NestedVecs"
  )
  colors <- colors(length(algorithms))
  names(colors) <- algorithms
  scale_fill_manual(values=colors)
}

build_plot_label <- function(dataset) {
  assert_that(distinct(dataset, dataset) %>% nrow() == 1)
  assert_that(distinct(dataset, dataset_params) %>% nrow() == 1)
  assert_that(distinct(dataset, queryset) %>% nrow() == 1)
  assert_that(distinct(dataset, queryset_params) %>% nrow() == 1)
  str_c(
    dataset %>% distinct(dataset) %>% pull(),
    " ",
    dataset %>% distinct(dataset_params) %>% pull(),
    "\n",
    dataset %>% distinct(queryset) %>% pull(),
    " ",
    dataset %>% distinct(queryset_params) %>% pull()
  )
}


