
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
#'
#'
packageResults <- function(baseOutputFolder, dbs) {
  allResults <- NULL
  for (db in dbs) {
    outputFolder <- file.path(baseOutputFolder, db)
    cmoutputFolder <- getCmFolderPath(outputFolder)
    analysisFolders <- list.dirs(cmoutputFolder, recursive = FALSE, full.names = FALSE)
    dbResults <- NULL
    for(analysisFolder in analysisFolders) {
      analysisId <- gsub("Analysis_", "", analysisFolder) %>% as.numeric()
      resultsFile <- getAnalysisBalanceSummaryFileName(analysisId = analysisId)
      resultsFile <- file.path(cmoutputFolder, analysisFolder, resultsFile)
      if(!file.exists(resultsFile))
        next
      aResult <- readr::read_csv(resultsFile, col_types = readr::cols())
      colnames(aResult) <- SqlRender::snakeCaseToCamelCase(colnames(aResult))
      aResult[, "databaseId"] <- db
      if(is.null(dbResults)) {
        dbResults <- aResult
        next
      }
      dbResults <- rbind(dbResults, aResult)
    }

    outFile <- file.path(outputFolder, sprintf("%s_results.csv", db))
    if(!is.null(dbResults))
      readr::write_csv(formatCols(dbResults), outFile)

    if(is.null(allResults)) {
      allResults <- dbResults
      next
    }
    allResults <- rbind(allResults, dbResults)
  }
  outFile <- file.path(baseOputFolder, "all_results.csv")
  readr::write_csv(formatCols(allResults), outFile)
}

formatCols <- function(df) {
  colnames(df) <- SqlRender::camelCaseToSnakeCase(colnames(df))
  return(df)
}
