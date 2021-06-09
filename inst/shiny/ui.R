fluidPage(
  titlePanel(""),
  sidebarLayout(
    sidebarPanel(
      selectInput(inputId = "targetName",
                  label = "Target Exposure",
                  choices = unique(balanceResults$targetName)),
      selectInput(inputId = "comparatorName",
                  label = "Comparator Exposure",
                  choices = unique(balanceResults$comparatorName)),
      selectInput(inputId = "databaseId",
                  label = "Database",
                  choices = unique(balanceResults$databaseId)),
      checkboxGroupInput(inputId = "psAdjustment",
                         label = "Adjustment Method",
                         choices = unique(balanceResults$psAdjustment),
                         selected = unique(balanceResults$psAdjustment)),
      selectInput(inputId = "balanceAggregateStatistic",
                  label = "Balance Statistic(s)",
                  choices = getBalanceFeasibleStatistics(balanceResults),
                  selected = NULL,
                  multiple = TRUE),
      selectInput(inputId = "partitionAggregation",
                  label = "Partition Aggregation Function",
                  choices = c("mean", "max", "min"),
                  selected = "mean")
    ),
    mainPanel(
      plotOutput(outputId = "mainPlot"),
      tabsetPanel(
        tabPanel(
          "Balance Results",
          reactableOutput(outputId = "balanceResultsTable")
        ),
        tabPanel(
          "CohortMethod/ESE Results",
          reactableOutput(outputId = "eseResultsTable")

        )
      )
    )
  )
)
