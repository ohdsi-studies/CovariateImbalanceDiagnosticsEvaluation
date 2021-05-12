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

#' Synthesizes and exports all results to tables
#'
#' @description
#' Outputs all results to a results folder.
#'
#' @param resultsFolder         Path to folder where results should be persistsed
#' @param cmOutputFolder        Name of local folder where CM objects are saved
#' @param databaseId            A short string for identifying the database (e.g. 'Synpuf').
#' @param maxCores              How many parallel cores should be used? If more cores are made
#'                              available this can speed up the analyses.
#'
#' @export
synthesizeAndExportResults <- function(resultsFolder,
                          cmOutputFolder,
                          databaseId,
                          maxCores,
                          packageName,
                          absoluteSdm = TRUE) {

  maybeMakeDir(resultsFolder)
  analyses <- readr::read_csv(
      system.file("settings", "analysisList.csv", package = packageName),
      col_types = readr::cols()
    )
  colnames(analyses) <- SqlRender::snakeCaseToCamelCase(colnames(analyses))


  balanceStatsToCompute <- list(
    "smdMin" = list("func" = "min",
                    "args" = list(na.rm = TRUE)),
    "smdMax" = list("func" = "max",
                    "args" = list(na.rm = TRUE)),
    "smdAvg" = list("func" = "mean",
                    "args" = list(na.rm = TRUE)),
    "smdP10" = list("func" = "quantile",
                    "args" = list(probs = .10, na.rm = TRUE)),
    "smdP25" = list("func" = "quantile",
                    "args" = list(probs = .25, na.rm = TRUE)),
    "smdP50" = list("func" = "quantile",
                    "args" = list(probs = .5, na.rm = TRUE)),
    "smdP75" = list("func" = "quantile",
                    "args" = list(probs = .75, na.rm = TRUE)),
    "smdP90" = list("func" = "quantile",
                    "args" = list(probs = .90, na.rm = TRUE)),
    "smdStd" = list("func" = "sd",
                    "args" = list(na.rm = TRUE)),
    "smdIqr" = list("func" = "IQR",
                    "args" = list(na.rm = TRUE)),
    "propGtThresh" = list("func" = "getPropGtThreshold",
                          "args" = list(threshold = 0.1,
                                        na.rm = TRUE))
  )

  tcosList <- createTcos(outputFolder, packageName)
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

  createSummaryTask <- function(i) {
    analysis <- analyses[i, ]
    args <- list()
    args$analysisId <- analysis$analysisId
    args$samplePerc <- analysis$samplePerc
    args$analysisSummaryFile <- getAnalysisSummaryFileName(analysis$analysisId)
    return(args)
  }


  summaryTasks <- lapply(1:length(unique(analyses$analysisId)), createSummaryTask)
  params <- summaryTasks[[1]]
  if (length(summaryTasks) != 0) {
    cluster <- ParallelLogger::makeCluster(min(14, maxCores))
    ParallelLogger::clusterRequire(cluster, packageName)
    dummy <- ParallelLogger::clusterApply(cluster, summaryTasks,
                                          summarizeAnalysis,
                                          cmOutputFolder, tcos,
                                          balanceStatsToCompute,
                                          analyses,
                                          outcomesOfInterest,
                                          absoluteSdm)
    ParallelLogger::stopCluster(cluster)
  }


}


getPropGtThreshold <- function(x, threshold, na.rm) {
  if(na.rm)
    return(length(x[x > threshold]) / length(x[!is.na(x)]))
  return(length(x[x > threshold]) / length(x))
}

summarizeAnalysis <- function(params, cmOutputFolder, tcos, balanceStatsToCompute, analyses, outcomesOfInterest,
                              absolute = TRUE) {

  analysisFolder <- file.path(cmOutputFolder, sprintf("Analysis_%d", params$analysisId))
  outputFile <- file.path(analysisFolder, getAnalysisSummaryFileName(params$analysisId))
  nullDistFile <- file.path(analysisFolder, getMcmcNullFileName(params$analysisId))

  # if(file.exists(outputFile))
  #   return(NULL)


  colNames <- c("analysisId", "partitionId", "targetId", "comparatorId", "outcomeId")
  colNames <- c(colNames, sapply(names(balanceStatsToCompute), function(x) {paste(x, "Dichotomous", sep = "")}))
  colNames <- c(colNames, sapply(names(balanceStatsToCompute), function(x) {paste(x, "Continuous", sep = "")}))
  # colNames <- c(colNames, names(balanceStatsToCompute))
  treatmentEstimates <- c("logRr", "logLb95", "logUb95", "seLogRr")
  colNames <- c(colNames, treatmentEstimates)

  numPartitions <- getNumPartitions(params$samplePerc)
  results <- matrix(, nrow = 0, ncol = length(colNames))
  for(i in 1:nrow(tcos)) {
    tco <- tcos[i, ]
    outcomeModelFile <- getOutcomeModelFileName(targetId = tco$targetId, comparatorId = tco$comparatorId, outcomeId = tco$outcomeId)
    outcomeModelFile <- file.path(getOutcomeModelFolderPath(cmOutputFolder), outcomeModelFile)
    om <- readRDS(outcomeModelFile)
    for(partitionId in 1:numPartitions){
      balanceFile <- getBalanceFileName(analysisId = params$analysisId, targetId = tco$targetId, comparatorId = tco$comparatorId,
                                        outcomeId = tco$outcomeId, partitionId = partitionId)
      balanceFile <- file.path(getBalanceFolderPath(analysisFolder),  balanceFile)

      bal <- readRDS(balanceFile)


      aResult <- c(params$analysisId, partitionId, tco$targetId, tco$comparatorId, tco$outcomeId)

      x <- bal %>%
        filter(isBinary == "Y") %>%
        pull(stdDiff)
      if(absolute)
        x <- abs(x)

      a <- sapply(names(balanceStatsToCompute), function(stat) {
        args <- list(x = x)
        args <- append(args, balanceStatsToCompute[[stat]][["args"]])
        return(do.call(balanceStatsToCompute[[stat]][["func"]], args))
        })
      aResult <- c(aResult, a)

      x <- bal %>%
        filter(isBinary == "N") %>%
        pull(stdDiff)
      if(absolute)
        x <- abs(x)

      a <- sapply(names(balanceStatsToCompute), function(stat) {
        args <- list(x = x)
        args <- append(args, balanceStatsToCompute[[stat]][["args"]])
        return(do.call(balanceStatsToCompute[[stat]][["func"]], args))
      })
      aResult <- c(aResult, a)


      if(!("outcomeModelTreatmentEstimate" %in% names(om))) {
        aResult <- c(aResult, rep(NA, length(treatmentEstimates)))
      } else {
        a <- sapply(treatmentEstimates, function(x) {
          return(om[["outcomeModelTreatmentEstimate"]][[x]])
        })
        aResult <- c(aResult, a)
      }
      results <- rbind(results, aResult)
    }
  }

  results <- as.data.frame(results)
  names(results) <- colNames
  results <- merge(analyses, results)
# for particular outcome, combine stratapop files acr
  ncs <- results[!(results$outcomeId %in% outcomesOfInterest), ] %>%
    select(c(.data$targetId, .data$outcomeId, .data$logRr, .data$seLogRr)) %>%
    unique()

  #TODO: question - fitmcmcnull run on NCs across all T-C pairs or one at a time?
  null <- EmpiricalCalibration::fitMcmcNull(logRr = results$logRr, seLogRr = results$seLogRr)
  saveRDS(null, nullDistFile)

  ease <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
  names(ease) <- c("ease", "easeCiLb", "easeCiUb")
  results <- cbind(results, ease)

  names(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  readr::write_csv(results, file = outputFile)

  return(NULL)
}


