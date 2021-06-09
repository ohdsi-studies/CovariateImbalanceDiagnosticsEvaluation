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


synthesizeAndExportResults <- function(resultsFolder,
                          cmOutputFolder,
                          databaseId,
                          maxCores,
                          packageName,
                          absoluteSdm = TRUE,
                          combineAnalyses = TRUE) {

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
                                        na.rm = TRUE)),
    "numCov" = list("func" = "getNumCov",
                    "args" = list(na.rm = TRUE))
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
    args$balanceApproach <- analysis$balanceApproach
    return(args)
  }


  summaryTasks <- lapply(1:length(unique(analyses$analysisId)), createSummaryTask)
  # params <- summaryTasks[[7]]
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

  if(combineAnalyses) {
    combinedStudyPops <- NULL
    combinedBalanceResults <- NULL
    combinedEseResults <- NULL
    for(i in 1:nrow(analyses)) {
      analysis <- analyses[i, ]
      analysisFolder <- getAnalysisFolderPath(cmOutputFolder, analysis$analysisId)
      analysisBalanceFile <- file.path(analysisFolder, getAnalysisBalanceSummaryFileName(analysis$analysisId))
      analysisEseFile <- file.path(analysisFolder, getAnalysisEseSummaryFileName(analysis$analysisId))
      analysisPopFile <- file.path(analysisFolder, getAnalysisPopSummarySummaryFileName(analysis$analysisId))

      analysisBalanceResults <- readr::read_csv(analysisBalanceFile, col_types = readr::cols())
      analysisEseResults <- readr::read_csv(analysisEseFile, col_types = readr::cols())
      analysisPopResults <- readr::read_csv(analysisPopFile, col_types = readr::cols())
      if(is.null(combinedBalanceResults)) {
        combinedBalanceResults <- analysisBalanceResults
        combinedEseResults <- analysisEseResults
        combinedStudyPops <- analysisPopResults
      } else {
        combinedBalanceResults<- rbind(combinedBalanceResults, analysisBalanceResults)
        combinedEseResults<- rbind(combinedEseResults, analysisEseResults)
        combinedStudyPops<- rbind(combinedStudyPops, analysisPopResults)
      }
    }

    balanceOutputFile <- file.path(resultsFolder, getCombinedBalanceFileName(databaseId))
    eseOutputFile <- file.path(resultsFolder, getCombinedEseFileName(databaseId))
    popOutputFile <- file.path(resultsFolder, getCombinedPopFileName(databaseId))
    readr::write_csv(combinedBalanceResults, file = balanceOutputFile)
    readr::write_csv(combinedEseResults, file = eseOutputFile)
    readr::write_csv(combinedStudyPops, file = popOutputFile)
    ParallelLogger::logInfo(sprintf("Results for database serialized to %s", resultsFolder))
  }


}


getPropGtThreshold <- function(x, threshold, na.rm) {
  if(na.rm)
    return(length(x[x > threshold]) / length(x[!is.na(x)]))
  return(length(x[x > threshold]) / length(x))
}


getNumCov <- function(x, na.rm) {
  if(na.rm)
    return(length(x[!is.na(x)]))
  return(length(x))
}

summarizeAnalysis <- function(params, cmOutputFolder, tcos, balanceStatsToCompute, analyses, outcomesOfInterest,
                              absolute = TRUE) {

  analysisFolder <- file.path(cmOutputFolder, sprintf("Analysis_%d", params$analysisId))
  balanceOutputFile <- file.path(analysisFolder, getAnalysisBalanceSummaryFileName(params$analysisId))
  eseOutputFile <- file.path(analysisFolder, getAnalysisEseSummaryFileName(params$analysisId))
  popSummaryOutputFile <- file.path(analysisFolder, getAnalysisPopSummarySummaryFileName(params$analysisId))


  # end balance summarization
  colNames <- c("analysisId", "partitionId", "targetId", "comparatorId")
  if(params$balanceApproach == "tco")
    colNames <- c(colNames, "outcomeId")
  colNames <- c(colNames, sapply(names(balanceStatsToCompute), function(x) {paste(x, "Dichotomous", sep = "")}))
  colNames <- c(colNames, sapply(names(balanceStatsToCompute), function(x) {paste(x, "Continuous", sep = "")}))
  # colNames <- c(colNames, names(balanceStatsToCompute))

  numPartitions <- getNumPartitions(params$samplePerc)
  results <- matrix(, nrow = 0, ncol = length(colNames))

  if (params$balanceApproach == "tco") {
    loopingObject <- tcos
  } else {
    loopingObject <- tcos %>%
      select(.data$targetId, .data$comparatorId) %>%
      unique()
  }

  for(i in 1:nrow(loopingObject)) {
    row <- loopingObject[i, ]

    for(partitionId in 1:numPartitions) {
      balanceFile <- getBalanceFileName(analysisId = params$analysisId,
                                        targetId = row$targetId,
                                        comparatorId = row$comparatorId,
                                        outcomeId = if(params$balanceApproach == "tco") row$outcomeId else NULL,
                                        partitionId = partitionId)
      balanceFile <- file.path(getBalanceFolderPath(analysisFolder),  balanceFile)

      bal <- readRDS(balanceFile)


      aResult <- c(params$analysisId, partitionId, row$targetId, row$comparatorId)
      if(params$balanceApproach == "tco")
        aResult <- c(aResult, row$outcomeId)

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

      results <- rbind(results, aResult)
    }
  }

  results <- as.data.frame(results)
  names(results) <- colNames
  results <- merge(analyses, results)
  names(results) <- SqlRender::camelCaseToSnakeCase(colnames(results))
  readr::write_csv(results, file = balanceOutputFile)
  # end balance summarization


  # begin ESE generation results
  results <- NULL
  colNames <- c("analysisId", "targetId", "comparatorId", "outcomeId")
  treatmentEstimates <- c("logRr", "logLb95", "logUb95", "seLogRr")
  colNames <- c(colNames, treatmentEstimates)
  tcs <- unique(tcos[, c("targetId", "comparatorId")])
  allResults <-  vector(mode = "list", length = nrow(tcs))
  for(i in 1:nrow(tcs)) {
    tc <- tcs[i, ]
    tcResults <- matrix(, nrow = 0, ncol = length(colNames))

    tcoSubset <- tcos[tcos$targetId == tc$targetId & tcos$comparatorId == tc$comparatorId, ]
    for(j in 1:nrow(tcoSubset)) {
      tco <- tcoSubset[j, ]
      outcomeModelFile <- getOutcomeModelFileName(analysisId = params$analysisId, targetId = tco$targetId, comparatorId = tco$comparatorId, outcomeId = tco$outcomeId)
      outcomeModelFile <- file.path(getOutcomeModelFolderPath(analysisFolder), outcomeModelFile)
      om <- readRDS(outcomeModelFile)

      aResult <- c(params$analysisId, tco$targetId, tco$comparatorId, tco$outcomeId)

      if(!("outcomeModelTreatmentEstimate" %in% names(om))) {
        a <- rep(NA, length(treatmentEstimates))
      } else {
        a <- sapply(treatmentEstimates, function(x) {
          return(om[["outcomeModelTreatmentEstimate"]][[x]])
        })
      }
      aResult <- c(aResult, a)
      tcResults <- rbind(tcResults, aResult)
    }

    tcResults <- as.data.frame(tcResults)
    names(tcResults) <- colNames

    ncs <- tcResults[!(tcResults$outcomeId %in% outcomesOfInterest), ] %>%
      select(c(.data$targetId, .data$outcomeId, .data$logRr, .data$seLogRr)) %>%
      unique()
    null <- EmpiricalCalibration::fitMcmcNull(logRr = ncs$logRr, seLogRr = ncs$seLogRr)
    ease <- EmpiricalCalibration::computeExpectedAbsoluteSystematicError(null)
    names(ease) <- c("ease", "easeCiLb", "easeCiUb")

    nullDistFile <- file.path(analysisFolder, getMcmcNullFileName(params$analysisId, tc$targetId, tc$comparatorId))
    saveRDS(null, nullDistFile)

    tcResults <- cbind(tcResults, ease)
    allResults[[i]] <- tcResults
  }

  allResults <- do.call("rbind", allResults)
  allResults <- merge(analyses, allResults)

  names(allResults) <- SqlRender::camelCaseToSnakeCase(colnames(allResults))
  readr::write_csv(allResults, file = eseOutputFile)

  # end ESE generation results



  # begin pop summary generation

  popSummaryFiles <- list.files(path = analysisFolder, pattern = "PopSummary_.*.csv", full.names = TRUE)

  parseAndReadFile <- function(file, includeParams = TRUE) {
    data <- readr::read_csv(file,  col_types = readr::cols())
    if (includeParams) {
      specs <- basename(file) %>%
        gsub(pattern = "PopSummary_a|t|c|o|\\.csv", replacement = "")
      specs <- strsplit(specs, "_")[[1]]

      data[, "targetId"] <- specs[[2]]
      data[, "comparatorId"] <- specs[[3]]
      # data[, "outcomeId"] <- specs[[4]]
    }
    return(data)
  }

  popResults <- lapply(popSummaryFiles, parseAndReadFile, TRUE)
  popResults <- do.call("rbind", popResults)
  t <- popResults %>%
    group_by(targetId, comparatorId, partition) %>%
    summarise_at(vars(-group_cols()), mean, na.rm = TRUE) %>%
    rename(partitionId = partition)

  # t <- popResults %>%
  #   group_by(partition) %>%
  #   mutate_all(mean, na.rm = TRUE)

  t[, "analysisId"] <- params$analysisId[[1]]

  names(t) <- SqlRender::camelCaseToSnakeCase(colnames(t))
  readr::write_csv(t, file = popSummaryOutputFile)


  # end pop summary generation

  return(NULL)
}


