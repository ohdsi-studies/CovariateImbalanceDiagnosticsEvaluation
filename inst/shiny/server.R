function(input, output, session) {

  filteredBalancedResults <- reactive({
    balanceResults %>%
      filter(.data$targetName == input$targetName &
               .data$comparatorName == input$comparatorName &
               .data$databaseId == input$databaseId,
             .data$psAdjustment %in% input$psAdjustment)
  })

  filteredEseResults <- reactive({
    eseResults %>%
      filter(.data$targetName == input$targetName &
               .data$comparatorName == input$comparatorName &
               .data$databaseId == input$databaseId)
  })

  output$mainPlot <- renderPlot({

    if(is.null(input$balanceAggregateStatistic))
      return(NULL)

    combined <- getCombinedResults(filteredBalancedResults(),
                                   filteredEseResults())

    p <- filteredBalancedResults() %>%
      select(.data$psAdjustment, .data$samplePerc, input$balanceAggregateStatistic) %>%
      group_by(.data$psAdjustment, .data$samplePerc) %>%
      summarize_at(input$balanceAggregateStatistic, input$partitionAggregation) %>%
      tidyr::gather("statistic", "value", - c("psAdjustment", "samplePerc"))


    plot <- p %>%
      ggplot(aes(x = samplePerc, y = value)) +
      geom_line(aes(color = statistic, linetype = psAdjustment), plot = 0.6)

    return(plot)

  })

  output$balanceResultsTable <- renderReactable({
    reactable(filteredBalancedResults(),
              defaultColDef = colDef(align = "left"),
              filterable = TRUE,
              searchable = TRUE,
              pagination = TRUE,
              paginationType = "numbers",
              showPageSizeOptions = TRUE,
              showPageInfo = TRUE,
              bordered = TRUE,
              compact = FALSE,
              striped = TRUE,
              showSortable = TRUE,
              fullWidth = TRUE
              )
  })


  output$eseResultsTable <- renderReactable({
    reactable(filteredEseResults(),
              defaultColDef = colDef(align = "left"),
              filterable = TRUE,
              searchable = TRUE,
              pagination = TRUE,
              paginationType = "numbers",
              showPageSizeOptions = TRUE,
              showPageInfo = TRUE,
              bordered = TRUE,
              compact = FALSE,
              striped = TRUE,
              showSortable = TRUE,
              fullWidth = TRUE
    )
  })
}
