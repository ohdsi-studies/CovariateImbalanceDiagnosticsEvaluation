fluidPage(
  titlePanel(""),
  sidebarLayout(
    sidebarPanel(
      selectInput(inputId = "sampleSize",
                  label = "Sample Size",
                  choices = unique(results$sampleSize)),
      selectInput(inputId = "psAdjustment",
                  label = "Adjustment",
                  choices = unique(results$psAdjustment)),
      selectInput(inputId = "targetId",
                  label = "Target",
                  choices = unique(results$targetId)),
      selectInput(inputId = "compartorId",
                  label = "Comparator",
                  choices = unique(results$comparatorId)),
      selectInput(inputId = "outcomeId",
                  label = "Outcome",
                  choices = unique(results$outcomeId)),
      selectInput(inputId = "databaseId",
                  label = "Database",
                  choices = unique(results$databaseId))
    ),
    mainPanel(
      reactableOutput(outputId = "resultsTable")

    )
  )
)
