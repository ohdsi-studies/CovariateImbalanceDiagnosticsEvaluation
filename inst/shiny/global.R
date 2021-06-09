library(dplyr)
library(reactable)
library(shiny)

source('helper.R')


balanceResults <- readr::read_csv(file.path("data", "balance_results.csv"), col_types = readr::cols(), guess_max = 1e5)
eseResults <- readr::read_csv(file.path("data", "ese_results.csv"), col_types = readr::cols(), guess_max = 1e5)
colnames(balanceResults) <- SqlRender::snakeCaseToCamelCase(colnames(balanceResults))
colnames(eseResults) <- SqlRender::snakeCaseToCamelCase(colnames(eseResults))


