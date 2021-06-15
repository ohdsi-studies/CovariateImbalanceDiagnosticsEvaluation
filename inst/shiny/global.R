library(cowplot)
library(dplyr)
library(ggplot2)
library(reactable)
library(shiny)
library(stringr)

source('helper.R')


balanceResults <- readr::read_csv(file.path("data", "balance_results.csv"), col_types = readr::cols(), guess_max = 1e5)
eseResults <- readr::read_csv(file.path("data", "ese_results.csv"), col_types = readr::cols(), guess_max = 1e5)
popResults <- readr::read_csv(file.path("data", "pop_summary.csv"), col_types = readr::cols(), guess_max = 1e5)
colnames(balanceResults) <- SqlRender::snakeCaseToCamelCase(colnames(balanceResults))
colnames(eseResults) <- SqlRender::snakeCaseToCamelCase(colnames(eseResults))
colnames(popResults) <- SqlRender::snakeCaseToCamelCase(colnames(popResults))


