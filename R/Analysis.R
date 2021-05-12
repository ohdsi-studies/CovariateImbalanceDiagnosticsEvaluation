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



generateAnalysisObjects <- function(cmOutputFolder,
                                    packageName,
                                    randomSeed,
                                    maxCores) {
  ParallelLogger::logInfo("Getting analysis settings")
  analyses <-
    readr::read_csv(
      system.file("settings", "analysisList.csv", package = packageName),
      col_types = readr::cols()
    )
  colnames(analyses) <-
    SqlRender::snakeCaseToCamelCase(colnames(analyses))


  tcosList <- createTcos(outputFolder, packageName)


  analysisCombos <- data.frame(
    analysisId = character(),
    targetId = character(),
    comparatorId = character(),
    outcomeId = character()
  )

  for (i in 1:length(tcosList)) {
    tco <- tcosList[[i]]
    t <-
      expand.grid(unique(analyses$analysisId),
                  tco$targetId,
                  tco$comparatorId,
                  tco$outcomeIds)
    colnames(t) <- colnames(analysisCombos)
    analysisCombos <- rbind(analysisCombos, t)
  }

  analyses <- merge(analyses, analysisCombos)

  analyses <- addFileNames(analyses)

  subset <- analyses[analyses$outcomeFile != "", ]

  createAnalysisTask <- function(i) {
    analysis <- subset[i,]
    args <- list()
    args$analysisId <- analysis$analysisId
    args$psFile <-
      file.path(
        cmOutputFolder,
        getPsFileName(
          analysis$targetId,
          analysis$comparatorId,
          analysis$outcomeId
        )
      )

    isStratified <- FALSE #(analysis$psAdjustment == "matchOnPs")
    args$targetId <- analysis$targetId
    args$comparatorId <- analysis$comparatorId
    args$outcomeId <- analysis$outcomeId
    args$outcomeFile <- analysis$outcomeFile
    args$balanceFile <- analysis$balanceFile
    args$strataPopFile <- analysis$strataPopFile
    args$analysisPopFile <- analysis$analysisPopFile
    args$popSummaryFile <- analysis$popSummaryFile
    args$samplePerc <- analysis$samplePerc
    args$psAdjustment <- analysis$psAdjustment
    args$balanceApproach <- analysis$balanceApproach
    args$matchOnPsArgs <- CohortMethod::createMatchOnPsArgs()
    args$outcomeModelArgs <- CohortMethod::createFitOutcomeModelArgs(modelType = "cox",
                                                                     stratified = isStratified)
    args$fitOutcomeModel <- TRUE
    args$computeBalance <- TRUE
    args$cohortMethodDataFile <- file.path(cmOutputFolder,
                                           sprintf("CmData_l1_t%s_c%s.zip", analysis$targetId, analysis$comparatorId))
    args$sharedPsFile <- file.path(cmOutputFolder, getPsFileName(targetId = analysis$targetId, comparatorId = analysis$comparatorId,
                                                                 shared = TRUE))
    return(args)
  }

  analysisTasks <- lapply(1:nrow(subset), createAnalysisTask)
  # params <- analysisTasks[[3000]]


  ParallelLogger::logInfo("Generating analysis objects")
  if (length(analysisTasks) != 0) {
    cluster <- ParallelLogger::makeCluster(min(15, maxCores))
    ParallelLogger::clusterRequire(cluster, packageName)
    ParallelLogger::clusterRequire(cluster, "CohortMethod")
    dummy <- ParallelLogger::clusterApply(cluster, analysisTasks, createAnalysisObjects, cmOutputFolder, randomSeed)
    ParallelLogger::stopCluster(cluster)
  }

}

addFileNames <- function(analyses) {
  getStrataName <- function(i) {
    x <- analyses[i, ]
    return(getStrataFileName(targetId = x$targetId, comparatorId = x$comparatorId, outcomeId = x$outcomeId))
  }
  getBalanceName <- function(i) {
    x <- analyses[i, ]
    return(getBalanceFileName(x$analysisId, x$targetId, x$comparatorId, x$outcomeId))
  }
  getOutcomeName <- function(i, shared=FALSE) {
    x <- analyses[i, ]
    return(getOutcomeModelFileName(x$analysisId, x$targetId, x$comparatorId, x$outcomeId))
  }
  getPopSummaryName <- function(i) {
    x <- analyses[i, ]
    return(getPopSummaryFileName(x$analysisId, x$targetId, x$comparatorId, x$outcomeId))
  }
  getAnalysisPopName <- function(i) {
    x <- analyses[i, ]
    return(getAnalysisPopFileName(x$analysisId, x$targetId, x$comparatorId, x$outcomeId))
  }
  analyses[, "strataPopFile"] <- sapply(1:nrow(analyses), getStrataName)
  # analyses[, "balanceFile"] <- sapply(1:nrow(analyses), getBalanceName)
  analyses[, "outcomeFile"] <- sapply(1:nrow(analyses), getOutcomeName)
  analyses[, "popSummaryFile"] <- sapply(1:nrow(analyses), getPopSummaryName)
  analyses[, "analysisPopFile"] <- sapply(1:nrow(analyses), getAnalysisPopName)
  return(analyses)
}


createAnalysisObjects <- function(params, cmOutputFolder, randomSeed) {

  analysisFolder <- file.path(cmOutputFolder, sprintf("Analysis_%d", params$analysisId))
  outcomeModelFolder <- getOutcomeModelFolderPath(analysisFolder) # change back to analysisFolder if no longer shared
  balanceFolder <- getBalanceFolderPath(analysisFolder)

  strataPopFile <- file.path(cmOutputFolder, params$strataPopFile)
  balanceFile <- file.path(balanceFolder, params$balanceFile)
  outcomeModelFile <- file.path(outcomeModelFolder, params$outcomeFile)
  popSummaryFile <- file.path(analysisFolder, params$popSummaryFile)
  analysisPopFile <- file.path(analysisFolder, params$analysisPopFile)

  # calculate number of partitions for sampling scheme
  numPartitions <- getNumPartitions(params$samplePerc)

  # construct all the balance file names (for continuing if exist)
  getABalanceFile <- function(partitionId) {
    file.path(balanceFolder,
              getBalanceFileName(analysisId = params$analysisId,
                                 partitionId = partitionId,
                                 targetId = params$targetId,
                                 outcomeId = if(params$balanceApproach == "tco") params$outcomeId else NULL,
                                 comparatorId = params$comparatorId))
  }
  balanceFiles <- sapply(1:numPartitions, getABalanceFile)

  # change if we start serializing more objects
  outFiles <- c(balanceFiles, outcomeModelFile)
  if(all(sapply(outFiles, file.exists)))
    return(NULL)

  # read shared propensity score file and sample population
  sharedPs <- readRDS(params$sharedPsFile)
  sharedPs <- partitionPropensityPop(ps = sharedPs, numPartitions = numPartitions, randomSeed = randomSeed)
  # sampleIds <- sample(1:100, size = params$sampleSize, replace = FALSE)
  # ps <- ps[ps$partition %in% sampleIds, ]


  popSummary <- NULL
  wholePop <- NULL

  cohortMethodData <- getCohortMethodData(params$cohortMethodDataFile)
  # allCov <- cohortMethodData$covariates %>%
  #   collect()

  partitionId <- 1
  for(partitionId in 1:numPartitions) {
    ps <- sharedPs %>%
      filter(.data$partition == partitionId)

    #TODO: does outcome matter?  For crude/random, no, but for matchonPs, yes since study pop is being manipulated
    balanceFile <- balanceFiles[[partitionId]]

    # will always evaluate to TRUE at the moment since not serializing the object
    if(!file.exists(strataPopFile)) {
      args <- list(population = ps)
      args$cohortMethodData <- cohortMethodData
      args$outcomeId <- params$outcomeId
      ps <- do.call("createStudyPopulation", args)
      # saveRDS(ps, file = strataPopFile)
    } else {
      # ps <- readRDS(strataPopFile)
    }

    l <- CohortMethod::computeMdrr(ps)
    l$partition <- partitionId
    if(is.null(popSummary))
      popSummary <- l
    else
      popSummary <- rbind(popSummary, l)


    if(params$psAdjustment == "crude") {
      # saveRDS(ps, file = strataPopFile)
    } else if (params$psAdjustment == "matchOnPs") {
      args <- list(population = ps)
      args <- append(args, params$matchOnPsArgs)
      ps <- do.call("matchOnPs", args)
      # saveRDS(ps, file = strataPopFile)

    } else if(params$psAdjustment == "random") {
      ps[, "treatment_orig"] <- ps$treatment # no reason to capture, since no longer persisting
      # weights <- if(FALSE) c(0.5, 0.5) else ps$treatment %>% table() %>% nrow(ps) %>% as.vector()
      # ps$treatment <- sample(c(0, 1), size = nrow(ps), replace = TRUE, prob = weights)
      ps$treatment <- sample(ps$treatment, size = nrow(ps), replace = FALSE)
      # saveRDS(ps, file = strataPopFile)
    }

    if(params$computeBalance && !file.exists(balanceFile)) {
      args <- list(population = ps)
      args$cohortMethodData <- cohortMethodData
      # args$allCov <- allCov
      # args$includeBefore <- FALSE #params$psAdjustment == "matchOnPs"
      balance <- do.call('computeCovariateBalanceFeatureExtraction', args)
      saveRDS(balance, balanceFile)
    }
    if(is.null(wholePop))
      wholePop <- ps
    else
      wholePop <- rbind(wholePop, ps)

  }

  readr::write_csv(popSummary, popSummaryFile)
  saveRDS(wholePop, analysisPopFile)

  if(params$fitOutcomeModel && !file.exists(outcomeModelFile)) {
    # args <- list(population = sharedPs)
    # args$cohortMethodData <- cohortMethodData
    # args$outcomeId <- params$outcomeId
    # ps <- do.call("createStudyPopulation", args)

    #TODO: when fitting outcome model, should we concatenate all samples
    #     after individual study Pops, adjusting? Or take original pop, create
    #     study pop, then adjust? (Only makes a difference for matching analysis)

    args <- list(population = wholePop)
    args$cohortMethodData <- cohortMethodData
    args <- append(args, params$outcomeModelArgs)
    outcomeModel <- do.call('fitOutcomeModel', args)
    saveRDS(outcomeModel, outcomeModelFile)
  }

  return(NULL)
}




getCohortMethodData <- function(cohortMethodDataFile) {
  if (exists("cohortMethodData", envir = globalenv())) {
    cohortMethodData <- get("cohortMethodData", envir = globalenv())
  }
  if (!mget("cohortMethodDataFile", envir = globalenv(), ifnotfound = "") == cohortMethodDataFile) {
    if (exists("cohortMethodData", envir = globalenv())) {
      Andromeda::close(cohortMethodData)
    }
    cohortMethodData <- CohortMethod::loadCohortMethodData(cohortMethodDataFile)
    assign("cohortMethodData", cohortMethodData, envir = globalenv())
    assign("cohortMethodDataFile", cohortMethodDataFile, envir = globalenv())
  }
  return(cohortMethodData)
}
