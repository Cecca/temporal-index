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

inline_print <- function(d) {
  print(d)
  d
}

get_params <- function(data, column, prefix) {
  out <- data %>% 
    separate({{ column }}, into=paste0("p", 1:20), sep=" ", remove=F) %>% 
    suppressWarnings() %>%
    pivot_longer(p1:p20, names_to="dummy__", values_to="pair__") %>% 
    select(-dummy__) %>%
    drop_na(pair__) %>% 
    separate(pair__, into=c("param__", "param_val__"), sep="=", remove=T, convert=T) %>% 
    mutate(param__ = str_c(prefix, param__)) %>%
    filter(str_length(param__) > 0) %>%
    pivot_wider(names_from=param__, values_from=param_val__)
  
  assert_that(nrow(out) == nrow(data))
  out
}

draw_dataset <- function(intervals) {
  assert_that(distinct(intervals, name) %>% nrow() == 1)
  assert_that(distinct(intervals, parameters) %>% nrow() == 1)

  intervals %>%
    select(start, end) %>%
    distinct() %>%
    ggplot(aes(start, end)) +
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
  n_workloads <- (distinct(dataset, queryset_params) %>% pull())[1]

  # breaks <- pretty(pull(dataset, qps))
  breaks <- dataset %>%
    group_by(workload) %>%
    summarise(breaks = list(pretty(qps))) %>%
    unnest(breaks) %>%
    ungroup()
  print(breaks)

  plotdata <- dataset %>%
    group_by(workload, algorithm) %>%
    slice(which.max(qps)) %>%
    ungroup()

  p <- ggplot(plotdata, aes(x=reorder_within(algorithm, qps, workload),
                            y=qps,
                            fill=algorithm)) +
    geom_col() +
    geom_hline(mapping=aes(yintercept=breaks),
               data=breaks,
               color="white",
               lwd=.5) +
    geom_text(aes(label=scales::number(drop_units(qps), accuracy=1)),
            size=3,
            hjust=0,
            vjust=0.5,
            nudge_y=10) +
    scale_x_reordered() +
    scale_y_unit(breaks=scales::pretty_breaks(),
                 expand = expansion(mult = c(0, .1))) +
    scale_fill_algorithm() +
    coord_flip()

    if (n_workloads == 1) {
      plot_label <- build_plot_label(dataset)
      p <- p + labs(caption=plot_label)
    } else {
      p <- p + facet_wrap(vars(workload, dataset_params), scales="free")
    }

    p +
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
    # "period-index",
    "period-index-*",
    "period-index-old-*",
    "period-index++",
    # "grid",
    # "grid3D",       
    "interval-tree", 
    # "linear-scan",  
    # "NestedBTree",  
    # "NestedVecs"
    "grid-file"
  )
  colors <- colors(length(algorithms))
  names(colors) <- algorithms
  scale_fill_manual(values=colors, aesthetics = c("fill", "color"))
}

scale_color_workload <- function() {
  colors <- RColorBrewer::brewer.pal(n=8, name="Set2")
  colors <- ggthemes::tableau_color_pal(type="regular")
  workloads <- c(
    "both",
    "time",
    "duration"
  )
  colors <- colors(length(workloads))
  names(colors) <- workloads
  scale_fill_manual(values=colors, aesthetics = c("fill", "color"))
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

# Snap points to tenths of logarithms
logsnap <- function(x, step=10) {
  as.integer(10^(ceiling(log10(x) * step) / step))
}

plot_overview2 <- function(data, metric, xlab, n_bins=60, annotations_selector=which.max) {
    best <- data %>% 
      group_by(dataset, dataset_params, queryset, queryset_params, algorithm) %>% 
      slice(which.max({{metric}})) %>%
      mutate(
        workload_type = case_when(
          queryset == "random-uniform-zipf-uniform-uniform" ~ "both",
          queryset == "random-clustered-zipf-uniform-uniform" ~ "both",
          queryset == "random-None-uniform-uniform" ~ "duration",
          queryset == "random-uniform-zipf-None" ~ "time",
          queryset == "random-clustered-zipf-None" ~ "time",
          queryset == "Mixed" ~ "mixed",
          TRUE ~ "Unknown"
        )
      ) %>%
      ungroup() %>%
      mutate(bin = logsnap(drop_units({{ metric }}), 10)) %>%
      mutate(data_id = interaction(dataset, dataset_params, queryset, queryset_params)) %>%
      mutate(labelshow = str_c(
        dataset, dataset_params, "\n",
        queryset, queryset_params,
        sep=" "
      ))

    y_breaks_data <- best %>%
      group_by(bin) %>%
      summarise(m = scales::number(drop_units(median({{metric}})))) %>%
      ungroup() %>%
      arrange(bin) %>%
      filter(bin %in% pretty(bin))
    y_breaks <- y_breaks_data %>% pull(bin)
    y_labels <- y_breaks_data %>% pull(m)
 
    algo_rank <- best %>%
      group_by(algorithm) %>%
      summarise(median_metric = median({{metric}})) %>%
      ungroup() %>%
      arrange(median_metric) %>%
      mutate(algo_id = row_number())

    breaks <- algo_rank %>% pull(algo_id)
    labels <- algo_rank %>% pull(algorithm)

    plotdata <- best %>%
      inner_join(algo_rank) %>%
      group_by(algorithm, bin) %>%
      mutate(offset = (row_number()) * 0.1) %>%
      ungroup()

    quartiles <- plotdata %>%
      group_by(algo_id) %>%
      summarise(
        min_bin = min(bin),
        max_bin = max(bin),
        bin75 = quantile(bin, .75),
        bin25 = quantile(bin, .25),
        median_bin = median(bin)
      ) %>%
      ungroup()

    annotation_positions <- quartiles %>%
      slice(annotations_selector(algo_id)) %>%
      gather(min_bin:median_bin, key="label", value="bin") %>%
      mutate(label = case_when(
        label == "min_bin" ~ "min",
        label == "max_bin" ~ "max",
        label == "bin25"   ~ "25%",
        label == "bin75"   ~ "75%",
        label == "median_bin"   ~ "median",
      )) %>%
      mutate(xpos = if_else(label == "median", algo_id - 0.25, algo_id - 0.15))

    labelshows <- plotdata %>%
      distinct(data_id)

    p <- plotdata %>%
      ggplot(aes(x=algo_id + offset,
                 y=bin,
                 color=workload_type)) + 
      geom_line_interactive(aes(x=algo_id, y=bin, 
                                group=data_id, 
                                # color=workload_type,
                                data_id=data_id),
                            show.legend=F,
                            color="black",
                            alpha=0) +
      geom_linerange(aes(x=algo_id, ymin=min_bin, ymax=max_bin),
                     data=quartiles,
                     inherit.aes=F,
                     size=.5,
                     color="black") +
      geom_linerange(aes(x=algo_id, ymin=bin25, ymax=bin75),
                     data=quartiles,
                     inherit.aes=F,
                     size=1,
                     color="black") +
      geom_point(aes(x=algo_id, y=median_bin),
                 data=quartiles,
                 inherit.aes=F,
                 size=1,
                 shape=15,
                 color="white") +
      geom_point_interactive(aes(data_id=data_id, 
                                #  shape=is_estimate, 
                                 tooltip=algorithm_params),
                             size=1.4) +
      geom_text_interactive(aes(x=algo_id - 0.1 , y=bin, data_id=data_id, 
                                label=scales::number(drop_units(qps))),
                            color="black",
                            alpha=0,
                            size=3) +
      geom_text(aes(x = xpos, y = bin, label=label),
                inherit.aes=F,
                data=annotation_positions,
                size=3) +
      scale_x_continuous(labels=labels, breaks=breaks,
                         expand = expansion(add=c(1, 1))) +
      scale_y_continuous(trans="log", breaks=c(1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000),
                         labels=scales::number_format(),
                         expand = expansion(add=.5)) +
      labs(y=xlab, # The plot is flipped, hence y -> x
           x="algorithm",
           color="workload") +
      coord_flip() +
      theme_tufte() +
      theme(legend.position='bottom',
            panel.grid.major.y=element_line(color="white"))
}


save_png <- function(ggobj, filename, width = 10, height = 6) {
  ggsave(filename, 
         ggobj,
         type="cairo",
         dpi=300,
         width = width,
         height = height)
  knitr::include_graphics(here(filename))
}

plot_distribution_all <- function(querystats) {
  plotdata <- querystats %>% 
    as_tibble() %>% 
    mutate(query_time_ns = as.double(query_time_ns))
  slowest <- plotdata %>%
    group_by(algorithm, workload_type, start_times_distribution) %>%
    slice_max(query_time_ns, prop=.05) %>%
    ungroup()

  ggplot(slowest, aes(x=query_time_ns, y=algorithm, fill=workload_type)) + 
    geom_density_ridges(scale=0.9, alpha=0.5, 
                        rel_min_height=0.01,
                        show.legend=FALSE) + 
    # geom_point(mapping=aes(color=workload_type),
    #            data=slowest,
    #            shape="|") +
    scale_x_continuous(labels=scales::number_format(scale=0.001, suffix="")) + 
    facet_grid(vars(start_times_distribution), vars(workload_type), 
               scales="free_x") + 
    labs(x="query time", y="algorithm") +
    theme_ridges() + 
    theme(legend.position="bottom")
}

plot_top_query_times <- function(querystats) {
  querystats %>% 
    filter(group_rank == max(group_rank)) %>% 
    arrange(algorithm) %>%
    ggplot(aes(x=workload_type, y=group_query_time, fill=algorithm)) + 
    geom_col(position="dodge") + 
    geom_text(aes(label=scales::percent(fraction_total_query_time),
                  y=group_query_time + 1000),
               hjust=0,
               position=position_dodge(.9)) +
    facet_wrap(vars(start_times_distribution)) +
    scale_fill_algorithm() +
    coord_flip() +
    theme_tufte()
}
