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

loadCohortsToCreate <- function(packageName) {
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = packageName)
  cohortsToCreate <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(cohortsToCreate)
}



loadNegativeControls <- function(packageName) {
  pathToCsv <- system.file("settings", "NegativeControls.csv", package = packageName)
  negativeControls <- readr::read_csv(pathToCsv, col_types = readr::cols())
  return(negativeControls)
}

maybeMakeDir <- function(path, warnings = FALSE, recusrive = TRUE) {
  if(!file.exists(path))
    dir.create(path, showWarnings = warnings, recursive = recusrive)
}

getCmFolderPath <- function(outputFolder, makeIfNotFound = TRUE) {
  path <- file.path(outputFolder, "cmOutput")
  if(makeIfNotFound)
    maybeMakeDir(path)
  return(path)
}

getOutcomeModelFolderPath <- function(anAnalysisDir, makeIfNotFound = TRUE) {
  path <- file.path(anAnalysisDir, "oms")
  if(makeIfNotFound)
    maybeMakeDir(path)
  return(path)
}

getBalanceFolderPath <- function(anAnalysisDir, makeIfNotFound = TRUE) {
  path <- file.path(anAnalysisDir, "balance")
  if(makeIfNotFound)
    maybeMakeDir(path)
  return(path)
}


getSampleFolderPath <- function(outputFolder, makeIfNotFound = TRUE) {
  path <- file.path(outputFolder, "cmSample")
  if(makeIfNotFound)
    maybeMakeDir(path)
  return(path)
}

getResultsFolderPath <- function(outoutFolder, makeIfNotFound = TRUE) {
  path <- file.path(outputFolder, "results")
  if(makeIfNotFound)
    maybeMakeDir(path)
  return(path)
}

getCohortCountsFile <- function(outputFolder) {
  return(file.path(outputFolder, "CohortCounts.csv"))
}


getDrugConceptsForCohort <-function(aCohortId, packageName, includedOnly = TRUE, domainId = "Drug") {
  cohortName <- loadCohortsToCreate(packageName) %>%
    dplyr::filter(.data$cohortId == aCohortId) %>%
    dplyr::pull(.data$name) %>%
    unique()

  jsonFileName <- paste0(cohortName, ".json")
  jsonFilePath <- system.file("cohorts", paste0(cohortName, ".json"), package=packageName)
  if (!file.exists(jsonFilePath)) {
    return(NULL)
  }
  json <- RJSONIO::fromJSON(readr::read_file(jsonFilePath))

  concepts <- c()
  for(conceptSet in json$ConceptSets) {
    for(item in conceptSet$expression$items) {
      if (!item$isExcluded && item$concept$DOMAIN_ID == domainId)
        concepts <- c(concepts, item$concept$CONCEPT_ID)
    }
  }

  return(unique(concepts))

}

getStudyPopFileName <- function(targetId, comparatorId, outcomeId) {
  return(
    sprintf("StudyPop_l1_s1_t%s_c%s_o%s.rds", targetId, comparatorId, outcomeId)
  )
}

getPsFileName <- function(targetId, comparatorId, outcomeId = NULL, shared = FALSE) {
  if(shared) {
    return(
      sprintf("Ps_l1_s1_p1_t%s_c%s.rds", targetId, comparatorId)
    )
  }
  if(is.null(outcomeId)) {
    stop("Must specify an outcomeId to construct TCO PS file name.")
  }
  return(
    sprintf("Ps_l1_s1_p1_t%s_c%s_o%s.rds", targetId, comparatorId, outcomeId)
  )
}

getPsFiles <- function(cmOutputFolder, sharedOnly = TRUE) {
  if(sharedOnly) {
    return(
      list.files(path = cmOutputFolder, pattern = "Ps_l\\d+_s\\d+_p\\d+_t\\d+_c\\d+.rds")
    )
  } else {
    return(
      list.files(path = cmOutputFolder, pattern = "Ps_l\\d+_s\\d+_p\\d+_t\\d+_c\\d+.*rds")
    )
  }
}

getStudyPopFiles <- function(cmOutputFolder) {
  return(
    list.files(path = cmOutputFolder, pattern = "StudyPop\\w+t\\d+_c\\d+_o\\d+.rds")
  )
}



getStrataFileName <- function(targetId, comparatorId, outcomeId, analysisId=NULL) {
  if(is.null(analysisId)) {
    return(
      sprintf("StratPop_t%d_c%d_o%d.rds", targetId, comparatorId, outcomeId)
    )
  }
  return(
    sprintf("StratPop_a%d_t%d_c%d_o%d.rds", analysisId, targetId, comparatorId, outcomeId)
  )
}

getBalanceFileName <- function(analysisId, targetId, comparatorId, outcomeId, partitionId = NULL) {
  if(is.null(partitionId)) {
    if(is.null(outcomeId))
      return(sprintf("bal_a%d_t%d_c%d.rds", analysisId, targetId, comparatorId))
    return(sprintf("bal_a%d_t%d_c%d_o%d.rds", analysisId, targetId, comparatorId, outcomeId))
  } else {
    if(is.null(outcomeId))
      return(sprintf("bal_a%d_p%d_t%d_c%d.rds", analysisId, partitionId, targetId, comparatorId))
    return(sprintf("bal_a%d_p%d_t%d_c%d_o%d.rds", analysisId, partitionId, targetId, comparatorId, outcomeId))
  }
}

getAnalysisPopFileName <- function(analysisId, targetId, comparatorId, outcomeId) {
  return(
    sprintf("AnalysisPop_a%d_t%d_c%d_o%d.rds", analysisId, targetId, comparatorId, outcomeId)
  )
}

getOutcomeModelFileName <- function(analysisId, targetId, comparatorId, outcomeId) {
  return(
    sprintf("om_a%d_t%d_c%d_o%d.rds", analysisId, targetId, comparatorId, outcomeId)
  )
}

getAnalysisSummaryFileName <- function(analysisId) {
  return(
    sprintf("analysis_%d_results.csv", analysisId)
  )
}

getPopSummaryFileName <- function(analysisId, targetId, comparatorId, outcomeId) {
  return(
    sprintf("PopSummary_a%d_t%d_c%d_o%d.csv", analysisId, targetId, comparatorId, outcomeId)
  )
}

getMcmcNullFileName <- function(analysisId) {
  return(
    sprintf("analysis_%d_mcmc_null.rds", analysisId)
  )
}



getNumPartitions <- function(samplePerc) {
  numPartitions <- 100 / samplePerc
  if(numPartitions %% 1 == 0) {
    #TODO: give warning?
    numPartitions <- floor(numPartitions)
  }
  return(numPartitions)
}

createTcos <- function(outputFolder, packageName) {
  pathToCsv <- system.file("settings", "TcosOfInterest.csv", package = packageName)
  tcosOfInterest <- read.csv(pathToCsv, stringsAsFactors = FALSE)
  allControls <- getAllControls(outputFolder, packageName)
  tcs <- unique(rbind(tcosOfInterest[, c("targetId", "comparatorId")],
                      allControls[, c("targetId", "comparatorId")]))
  createTco <- function(i) {
    targetId <- tcs$targetId[i]
    comparatorId <- tcs$comparatorId[i]
    outcomeIds <- as.character(tcosOfInterest$outcomeIds[tcosOfInterest$targetId == targetId & tcosOfInterest$comparatorId == comparatorId])
    outcomeIds <- as.numeric(strsplit(outcomeIds, split = ";")[[1]])
    outcomeIds <- c(outcomeIds, allControls$outcomeId[allControls$targetId == targetId & allControls$comparatorId == comparatorId])
    excludeConceptIds <- as.character(tcosOfInterest$excludedCovariateConceptIds[tcosOfInterest$targetId == targetId & tcosOfInterest$comparatorId == comparatorId])
    if (length(excludeConceptIds) == 1 && is.na(excludeConceptIds)) {
      excludeConceptIds <- c()
    } else if (length(excludeConceptIds) > 0) {
      excludeConceptIds <- as.numeric(strsplit(excludeConceptIds, split = ";")[[1]])
    }
    includeConceptIds <- as.character(tcosOfInterest$includedCovariateConceptIds[tcosOfInterest$targetId == targetId & tcosOfInterest$comparatorId == comparatorId])
    if (length(includeConceptIds) == 1 && is.na(includeConceptIds)) {
      includeConceptIds <- c()
    } else if (length(includeConceptIds) > 0) {
      includeConceptIds <- as.numeric(strsplit(includeConceptIds, split = ";")[[1]])
    }
    tco <- CohortMethod::createTargetComparatorOutcomes(targetId = targetId,
                                                        comparatorId = comparatorId,
                                                        outcomeIds = outcomeIds,
                                                        excludedCovariateConceptIds = excludeConceptIds,
                                                        includedCovariateConceptIds = includeConceptIds)
    return(tco)
  }
  tcosList <- lapply(1:nrow(tcs), createTco)
  return(tcosList)
}

getOutcomesOfInterest <- function(packageName) {
  pathToCsv <- system.file("settings", "TcosOfInterest.csv", package = packageName)
  tcosOfInterest <- read.csv(pathToCsv, stringsAsFactors = FALSE)
  outcomeIds <- as.character(tcosOfInterest$outcomeIds)
  outcomeIds <- do.call("c", (strsplit(outcomeIds, split = ";")))
  outcomeIds <- unique(as.numeric(outcomeIds))
  return(outcomeIds)
}

getAllControls <- function(outputFolder, packageName) {
  allControlsFile <- file.path(outputFolder, "AllControls.csv")
  if (file.exists(allControlsFile)) {
    # Positive controls must have been synthesized. Include both positive and negative controls.
    allControls <- read.csv(allControlsFile)
  } else {
    # Include only negative controls
    allControls <- loadNegativeControls(packageName)
    allControls$oldOutcomeId <- allControls$outcomeId
    allControls$targetEffectSize <- rep(1, nrow(allControls))
  }
  return(allControls)
}
