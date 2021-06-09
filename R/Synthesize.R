# Copyright 2021 Observational Health Data Sciences and Informatics
#
# This file is part of CovarBalDiagEval
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#' Combines results across databases.
#'
#' @details
#' Combines results across databases.
#'
#' @param baseOutputFolder    The base output folder of results
#' @param dbIds               A vector of database IDs
#' @param packageName         Name of package
#' @param finalOtputDir       Final directory where the combined results should be saved
#'
#' @export
synthesizeResults <- function(baseOutputFolder,
                              dbIds,
                              packageName,
                              finalOutputFolder = getResultsFolderPath(baseOutputFolder)) {


  tcosList <- createTcos(baseOutputFolder, packageName)
  outcomesOfInterest <- getOutcomesOfInterest(packageName)

  tcos <- data.frame(
    targetId = character(),
    comparatorId = character(),
    outcomeId = character()
  )
  for (i in 1:length(tcosList)) {
    tco <- tcosList[[i]]
    t <-
      expand.grid(tco$targetId,
                  tco$comparatorId,
                  tco$outcomeIds)
    colnames(t) <- colnames(tcos)
    tcos <- rbind(tcos, t)
  }

  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = packageName)
  cohortsToCreate <- read.csv(pathToCsv)


  pathToCsv <- system.file("settings", "NegativeControls.csv", package = packageName)
  negativeControls <- read.csv(pathToCsv)

  tcos <- tcos %>%
    left_join(select(cohortsToCreate, cohortId, name), by = c("targetId" = "cohortId")) %>%
    rename("targetName" = "name") %>%
    left_join(select(cohortsToCreate, cohortId, name), by = c("comparatorId" = "cohortId")) %>%
    rename("comparatorName" = "name") %>%
    left_join(unique(select(negativeControls, outcomeId, outcomeName)))

  tcos$outcomeName[is.na(tcos$outcomeName)] <- "Angioedema"


  balanceResults <- NULL
  eseResults <- NULL
  popResults <- NULL
  for(databaseId in dbIds) {
    outputFolder <- file.path(baseOutputFolder, databaseId)
    cmOutputFolder <- getCmFolderPath(outputFolder)

    resultsFolder <- getResultsFolderPath(outputFolder)
    dbBalanceFile <- file.path(resultsFolder, getCombinedBalanceFileName(databaseId))
    dbEseFile <- file.path(resultsFolder, getCombinedEseFileName(databaseId))
    dbPopFile <- file.path(resultsFolder, getCombinedPopFileName(databaseId))

    dbBalanceResults <- readr::read_csv(dbBalanceFile, col_types = readr::cols(), guess_max = 1e5)
    dbEseResults <- readr::read_csv(dbEseFile, col_types = readr::cols(), guess_max = 1e5)
    dbPopResults <- readr::read_csv(dbPopFile, col_types = readr::cols(), guess_max = 1e5)

    colnames(dbBalanceResults) <- SqlRender::snakeCaseToCamelCase(colnames(dbBalanceResults))
    colnames(dbEseResults) <- SqlRender::snakeCaseToCamelCase(colnames(dbEseResults))
    colnames(dbPopResults) <- SqlRender::snakeCaseToCamelCase(colnames(dbPopResults))

    dbBalanceResults[, "databaseId"] <- databaseId
    dbEseResults[, "databaseId"] <- databaseId
    dbPopResults[, "databaseId"] <- databaseId

    if(is.null(balanceResults)) {
      balanceResults <- dbBalanceResults
      eseResults <- dbEseResults
      popResults <- dbPopResults

    } else {
      balanceResults <- rbind(balanceResults, dbBalanceResults)
      eseResults <- rbind(eseResults, dbEseResults)
      popResults <- rbind(popResults, dbPopResults)
    }
  }

  combinedResults <- balanceResults %>%
    left_join(popResults) %>%
    left_join(unique(select(tcos, -c(outcomeId, outcomeName)))) %>%
    left_join(unique(select(eseResults, c("databaseId", "analysisId", "targetId", "comparatorId", "ease", "easeCiLb", "easeCiUb"))))

  balanceResults <- balanceResults %>%
    left_join(unique(select(tcos, -c(outcomeId, outcomeName))))

  eseResults <- eseResults %>%
    left_join(unique(select(tcos, -c(outcomeId, outcomeName))))

  popResults <- popResults %>%
    left_join(unique(select(tcos, -c(outcomeId, outcomeName))))

  balanceOutFile <- file.path(finalOutputFolder, "balance_results.csv")
  eseOutFile <- file.path(finalOutputFolder, "ese_results.csv")
  popSummaryOutFile <- file.path(finalOutputFolder, "pop_summary.csv")
  combinedOutFile <- file.path(finalOutputFolder, "combined_results.csv")

  colnames(balanceResults) <- SqlRender::camelCaseToSnakeCase(colnames(balanceResults))
  colnames(eseResults) <- SqlRender::camelCaseToSnakeCase(colnames(eseResults))
  colnames(popResults) <- SqlRender::camelCaseToSnakeCase(colnames(popResults))
  colnames(combinedResults) <- SqlRender::camelCaseToSnakeCase(colnames(combinedResults))

  readr::write_csv(balanceResults, file = balanceOutFile)
  readr::write_csv(eseResults, file = eseOutFile)
  readr::write_csv(popResults, file = popSummaryOutFile)
  readr::write_csv(combinedResults, file = combinedOutFile)

}
