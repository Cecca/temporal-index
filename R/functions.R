get_dump <- function(what, conf_file) {
  tmp_file <- "/tmp/dump.csv"
  cmd <- str_c("target/release/temporal-index --dump", 
               what, 
               conf_file, 
               ">",
               tmp_file,
               sep=" ")
  system(cmd) 
  if (what == "dataset") {
    names <- c("name", "version", "parameters", "start", "end")
  } else {
    names <- c("name", "version", "parameters", "start", "end", "min_d", "max_d")
  }
  data <- read_csv(tmp_file, col_names=names)
  file.remove(tmp_file)
  data
}

draw_dataset <- function(intervals) {
  assert_that(distinct(intervals, name) %>% nrow() == 1)
  assert_that(distinct(intervals, parameters) %>% nrow() == 1)

  ggplot(intervals, aes(start, end)) +
    geom_point(shape=46, alpha=.01) +
    geom_rangeframe() +
    theme_tufte()
}

draw_queries <- function(queries, intervals) {
  max_end <- intervals %>% summarise(max(end)) %>% pull()
  extended <- queries %>% 
    filter(start < max_end) %>%
    select(start, end, min_d, max_d) %>%
    transmute(
      x_1 = end - max_d,
      y_1 = start,
      x_2 = end - min_d, 
      y_2 = start,
      x_3 = end, 
      y_3 = start + min_d,
      x_4 = end,
      y_4 = start + max_d,
      group = row_number()
    ) %>%
    gather("key", "val", x_1:y_4) %>%
    separate("key", into=c("dimension", "to_remove"), sep="_") %>%
    spread(dimension, val) %>%
    select(-to_remove)

  ggplot() +
    geom_point(shape=46, alpha=.01) +
    geom_point(data=extended,
               mapping=aes(x=x, y=y),
               color='orange',
               shape='.') +
    geom_polygon(mapping=aes(x, y, group=group),
                 data=extended,
                 fill='orange',
                 color="orange",
                 alpha=0.3)
    # theme_tufte()
}

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

get_histograms <- function(what, conf_file) {
  tmp_file <- "/tmp/histogram.csv"
  cmd <- str_c("target/release/temporal-index --histogram", 
               what, 
               conf_file, 
               ">",
               tmp_file,
               sep=" ")
  system(cmd) 
  data <- read_csv(tmp_file, col_names=c("name", "version", "parameters", "value", "count"))
  file.remove(tmp_file)
  data
}

plot_histogram <- function(plotdata, xlab) {
  ggplot(plotdata, aes(x=value, weight=count)) +
    geom_histogram(fill="lightgray") +
    labs(x=xlab) +
    theme_tufte()
}

plot_point_distribution <- function(plotdata, xlab) {
  plotdata <- plotdata %>% filter(count > 1)
  ggplot(plotdata, aes(x=value, y=count)) +
    geom_point() +
    geom_rangeframe() +
    labs(x=xlab) +
    scale_x_log10() +
    scale_y_log10() +
    theme_tufte()
}

