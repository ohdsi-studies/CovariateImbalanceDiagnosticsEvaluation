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
      plotOutput(outputId = "mainPlot", height = "auto"),
      tabsetPanel(
        tabPanel(
          "Population Summary",
          style = "margin-top:10px",
          span("** Averaged and rounded across TCO(s), partitions***",
               style = "font-weight:bold;"),
          reactableOutput(outputId = "popSummaryTable")
        ),
        tabPanel(
          "Balance Results",
          style = "margin-top:10px",
          reactableOutput(outputId = "balanceResultsTable")
        ),
        tabPanel(
          "CohortMethod/ESE Results",
          style = "margin-top:10px",
          reactableOutput(outputId = "eseResultsTable")

        )
      )
    )
  )
)
