
barchart_qps <- function(dataset) {
  # Assert that we are dealing with a single data and query configuration
  assert_that(distinct(dataset, dataset) %>% nrow() == 1)
  assert_that(distinct(dataset, dataset_params) %>% nrow() == 1)
  assert_that(distinct(dataset, queryset) %>% nrow() == 1)
  assert_that(distinct(dataset, queryset_params) %>% nrow() == 1)
  
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
        scale_fill_discrete_qualitative() +
        coord_flip() +
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



