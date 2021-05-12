function(input, output, session) {

  filteredResults <- reactive({
    results %>%
      filter(.data$sampleSize == input$sampleSize &
               .data$psAdjustment == input$psAdjustment &
               # .data$targetId == input$targetId &
               # .data$outcomeId == input$outcomeId &
               # .data$comparatorId == input$compartorId &
               .data$databaseId == input$databaseId)
  })

  output$resultsTable <- renderReactable({
    reactable(filteredResults(),
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
