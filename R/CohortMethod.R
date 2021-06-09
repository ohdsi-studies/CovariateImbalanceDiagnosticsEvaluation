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

createCohortMethodObjects <- function(connectionDetails,
                            cdmDatabaseSchema,
                            cohortDatabaseSchema,
                            cohortTable,
                            oracleTempSchema,
                            outputFolder,
                            packageName,
                            maxCores,
                            maxCohortSize,
                            createStudyPops = FALSE,
                            serializeObjects = TRUE,
                            verbose) {

  cmOutputFolder <- getCmFolderPath(outputFolder)
  if (!file.exists(cmOutputFolder)) {
    dir.create(cmOutputFolder)
  }

  # get TCOs
  targetComparatorOutcomesList <- createTcos(outputFolder = outputFolder, packageName = packageName)
  outcomeIdsOfInterest <- getOutcomesOfInterest(packageName = packageName)

  # list of target IDs
  targetIds <- unique(sapply(targetComparatorOutcomesList, function(x) {x$targetId}))
  # list of comparator IDs
  comparatorIds <- unique(sapply(targetComparatorOutcomesList, function(x) {x$comparatorId}))
  # parse target concept IDs
  targetConcepts <- do.call(c, sapply(targetIds, FUN = getDrugConceptsForCohort, packageName, simplify=FALSE))
  # parse comparator concept IDs
  comparatorConcepts <- do.call(c, sapply(comparatorIds, FUN = getDrugConceptsForCohort, packageName, simplify=FALSE))


  # exclude target and comparator concept IDs, plus descendants
  covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(excludedCovariateConceptIds = c(targetConcepts, comparatorConcepts),
                                       addDescendantsToExclude = TRUE)


  getDbCohortMethodDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(washoutPeriod = 365,
                                                     restrictToCommonPeriod = FALSE,
                                                     firstExposureOnly = TRUE,
                                                     removeDuplicateSubjects = "remove all",
                                                     studyStartDate = "",
                                                     studyEndDate = "",
                                                     covariateSettings = covariateSettings,
                                                     maxCohortSize = maxCohortSize)


  createStudyPopArgs <- CohortMethod::createCreateStudyPopulationArgs()
  psArgs <- CohortMethod::createCreatePsArgs()

  # just needed to generate cohort method data object and fit shared propensity score
  cmAnalysis1 <- CohortMethod::createCmAnalysis(
    analysisId = 1,
    description = "CM / Study Pop Construction",
    getDbCohortMethodDataArgs = getDbCohortMethodDataArgs,
    createStudyPopArgs = createStudyPopArgs,
    createPs = TRUE,
    createPsArgs = psArgs,
    trimByPs = FALSE,
    matchOnPs = FALSE,
    stratifyByPs = FALSE,
    fitOutcomeModel = FALSE)

  cmAnalysisList <- list(cmAnalysis1)


  # geenerates cohortmethod data, trains shared PS models, and gets studypop data for outcome of interest
  results <- CohortMethod::runCmAnalyses(connectionDetails = connectionDetails,
                                         cdmDatabaseSchema = cdmDatabaseSchema,
                                         exposureDatabaseSchema = cohortDatabaseSchema,
                                         exposureTable = cohortTable,
                                         outcomeDatabaseSchema = cohortDatabaseSchema,
                                         outcomeTable = cohortTable,
                                         outputFolder = cmOutputFolder,
                                         oracleTempSchema = oracleTempSchema,
                                         cmAnalysisList = cmAnalysisList,
                                         targetComparatorOutcomesList = targetComparatorOutcomesList,
                                         getDbCohortMethodDataThreads = min(3, maxCores),
                                         createStudyPopThreads = min(3, maxCores),
                                         createPsThreads = max(1, round(maxCores/10)),
                                         psCvThreads = min(10, maxCores),
                                         trimMatchStratifyThreads = min(10, maxCores),
                                         fitOutcomeModelThreads = max(1, round(maxCores/4)),
                                         outcomeCvThreads = min(4, maxCores),
                                         refitPsForEveryOutcome = FALSE,
                                         prefilterCovariates = FALSE,
                                         outcomeIdsOfInterest = outcomeIdsOfInterest)

  # create study population for all control outcomes?
  if(createStudyPops) {


    # annotate studyPop file names and PS file names (ps file simply study pop with PS added)
    results <- addNonOutcomeFilenames(results)

    # only run for those that have not had results generated
    subset <- results[!results$outcomeOfInterest &
                        results$studyPopFile != "" &
                               !file.exists(file.path(cmOutputFolder, results$studyPopFile)) , ]
    createStudyPopTask <- function(i) {
      row <- subset[i, ]
      args = list(studyPopFile = file.path(cmOutputFolder, row$studyPopFile),
                  outcomeId = row$outcomeId,
                  cohortMethodDataFile = file.path(cmOutputFolder, row$cohortMethodDataFile),
                  cmOutputFolder = cmOutputFolder,
                  psModel = TRUE,
                  sharedPsFile = file.path(cmOutputFolder, row$sharedPsFile),
                  psFile = file.path(cmOutputFolder, row$psFile))

      return(args)
    }

    if (nrow(subset) == 0) {
      studyPopTasks <- list()
    } else {
      studyPopTasks <- lapply(1:nrow(subset), createStudyPopTask)
    }

    if (length(studyPopTasks) != 0) {
      cluster <- ParallelLogger::makeCluster(min(3, maxCores))
      ParallelLogger::clusterRequire(cluster, packageName)
      ParallelLogger::clusterRequire(cluster, "CohortMethod")
      dummy <- ParallelLogger::clusterApply(cluster, studyPopTasks, createStudyPopObject)
      ParallelLogger::stopCluster(cluster)
    }
  }
}

addNonOutcomeFilenames <- function(referenceTable) {
  maybeAddStudyPopFile <- function(i) {
    row <- referenceTable[i, ]
    if (row$studyPopFile !=  "") {
      return(row$studyPopFile)
    }
    return(getStudyPopFileName(row$targetId, row$comparatorId, row$outcomeId))
  }
  maybeAddPsFile <- function(i) {
    row <- referenceTable[i, ]
    if (row$psFile !=  "") {
      return(row$psFile)
    }
    return(sprintf("Ps_l1_s1_p1_t%s_c%s_o%s.rds", row$targetId, row$comparatorId, row$outcomeId))
  }
  referenceTable[, "studyPopFile"] <- sapply(1:nrow(referenceTable), maybeAddStudyPopFile)
  referenceTable[, "psFile"] <- sapply(1:nrow(referenceTable), maybeAddPsFile)
  return(referenceTable)
}


createStudyPopObject <- function(params) {
  cohortMethodData <- loadCohortMethodData(params$cohortMethodDataFile)
  args <- list()
  args$cohortMethodData <- cohortMethodData
  args$outcomeId <- params$outcomeId
  studyPop <- do.call("createStudyPopulation", args)
  saveRDS(studyPop, params$studyPopFile)
  if(params$psModel) {
    ps <- readRDS(params$sharedPsFile)
    idx <- match(studyPop$rowId, ps$rowId)
    studyPop$propensityScore <- ps$propensityScore[idx]
    ps <- studyPop
    saveRDS(ps, file = params$psFile)
  }
  return(NULL)
}

