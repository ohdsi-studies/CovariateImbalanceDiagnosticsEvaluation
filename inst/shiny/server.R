function(input, output, session) {

  filteredBalancedResults <- reactive({
    balanceResults %>%
      filter(.data$targetName == input$targetName &
               .data$comparatorName == input$comparatorName &
               .data$databaseId == input$databaseId &
             .data$psAdjustment %in% input$psAdjustment)
  })

  filteredEseResults <- reactive({
    eseResults %>%
      filter(.data$targetName == input$targetName &
               .data$comparatorName == input$comparatorName &
               .data$databaseId == input$databaseId &
               .data$psAdjustment %in% input$psAdjustment)
  })


  filteredPopResults <- reactive({
    popResults %>%
      filter(.data$targetName == input$targetName &
               .data$comparatorName == input$comparatorName &
               .data$databaseId == input$databaseId)
  })


  output$mainPlot <- renderPlot({

    if(is.null(input$balanceAggregateStatistic))
      return(NULL)

    # combined <- getCombinedResults(filteredBalancedResults(),
    #                                filteredEseResults())

    balancePlotInput <- filteredBalancedResults() %>%
      select(.data$psAdjustment, .data$samplePerc, input$balanceAggregateStatistic) %>%
      group_by(.data$psAdjustment, .data$samplePerc) %>%
      summarize_at(input$balanceAggregateStatistic, input$partitionAggregation) %>%
      tidyr::gather("statistic", "value", - c("psAdjustment", "samplePerc"))

    esePlotInput <- filteredEseResults() %>%
      select(.data$psAdjustment, .data$samplePerc, .data$ease, .data$easeCiLb, .data$easeCiUb) %>%
      unique()

    balancePlot <- balancePlotInput %>%
      ggplot(aes(x = samplePerc, y = value, color = psAdjustment)) +
      geom_line(aes(linetype = statistic), alpha = 0.6) +
      geom_point()+
      scale_x_continuous(breaks = sort(unique(balancePlotInput$samplePerc))) +
      theme_bw() +
      labs(x = "Sub-Sample Proportion (%)",
           y = sprintf("%s Statistic Across Sub-Samples", str_to_title(input$partitionAggregation)),
           color = "Adjustment",
           linetype = "Statistic")

    pd <- position_dodge(.2)
    esePlot <- esePlotInput %>%
      ggplot(aes(x = samplePerc, y = ease, color = psAdjustment, group = psAdjustment)) +
      geom_errorbar(aes(ymin = easeCiLb, ymax = easeCiUb), colour="black", width = .15, position = pd) +
      geom_line(position = pd) +
      geom_point(position = pd) +
      scale_x_continuous(breaks = sort(unique(balancePlotInput$samplePerc))) +
      labs(x = "Sub-Sample Proportion (%)",
           y = "Expected Systematic Error (95% CI)") +
      theme_bw()


    plot_grid(balancePlot, esePlot, nrow = 2, labels = c("", ""), align = "v")

    return(plot_grid(balancePlot, esePlot, nrow = 2, labels = c("", ""), align = "v"))

  }, height = 800, res = 100)

  # output$mainTable <- renderDataTable({
  #
  #   c <- b %>%
  #     select(analysisId, samplePerc, psAdjustment, partitionId, targetId, comparatorId, numCovContinuous, numCovDichotomous, databaseId) %>%
  #     inner_join(p)
  #   c %>%
  #     select(-c(analysisId, databaseId, targetName, comparatorName, targetId, comparatorId, partitionId, numCovContinuous, mdrr, se, outcomeId)) %>%
  #     group_by(samplePerc, psAdjustment) %>%
  #     summarize_all(mean, na.rm = TRUE)
  # })

  output$popSummaryTable <- renderReactable({

    meanRound <- function(x) {
      return(
        round(mean(x, na.rm = TRUE))
      )
    }
    df <- filteredBalancedResults() %>%
      select(analysisId, samplePerc, psAdjustment, partitionId, targetId, comparatorId, numCovContinuous, numCovDichotomous, databaseId) %>%
      inner_join(filteredPopResults()) %>%
      select(-c(analysisId, databaseId, targetName, comparatorName, targetId, comparatorId, partitionId, numCovContinuous, mdrr, se, outcomeId)) %>%
      group_by(samplePerc, psAdjustment) %>%
      summarize_all(meanRound)

    colnames(df) <- SqlRender::camelCaseToTitleCase(colnames(df))

    reactable(df,
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

  output$balanceResultsTable <- renderReactable({
    df <- filteredBalancedResults()
    colnames(df) <- SqlRender::camelCaseToTitleCase(colnames(df))
    reactable(df,
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
    df <- filteredEseResults()
    colnames(df) <- SqlRender::camelCaseToTitleCase(colnames(df))
    reactable(df,
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
