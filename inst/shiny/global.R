library(dplyr)
library(reactable)
library(shiny)


results <- readr::read_csv(file.path("data", "all_results.csv"), col_types = readr::cols())
colnames(results) <- SqlRender::snakeCaseToCamelCase(colnames(results))


