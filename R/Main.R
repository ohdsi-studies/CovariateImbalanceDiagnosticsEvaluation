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



#' Execute the study
#'
#' @details
#' This function executes the study.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param cohortDatabaseSchema Schema name where outcome data can be stored. You will need to have
#'                             write priviliges in this schema. Note that for SQL Server, this should
#'                             include both the database and schema name, for example 'cdm_data.dbo'.
#' @param cohortTable          The name of the table that will be created in the \code{cohortDatabaseSchema}.
#'                             This table will hold the exposure and outcome cohorts used in this
#'                             study.
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param databaseId           A short string for identifying the database (e.g. 'Synpuf').
#' @param packageName          The name of the package, added for flexibility in package naming convention with find/replace.
#' @param randomSeed           The seed to use for random samples - for greater reproducibility.
#' @param databaseName          The full name of the database.
#' @param databaseDescription   A short description (several sentences) of the database.
#' @param maxCores             How many parallel cores should be used? If more cores are made available
#'                             this can speed up the analyses.
#' @param maxCohortSize        The largest cohort size to be used when creating \code{CohortMethod} data objects.
#' @param createCohorts        Create the exposure and outcome cohorts?
#' @param synthesizePositiveControls Create synthetic positive controls using \code{MethodEvaluation} package?
#' @param createCohortMethodObjects      Create the CohortMethod data objects?
#' @param generateAnalysisObjects Computes the balance objects, fits outcome models, and generates population meta-data.
#' @param synthesizeAndExportResults Synthesize results across analyses?
#' @param verbose Should verbose logging be used?
#'
#' @export
execute <- function(connectionDetails,
                    cdmDatabaseSchema,
                    cohortDatabaseSchema,
                    cohortTable,
                    outputFolder,
                    databaseId,
                    packageName,
                    randomSeed = 123,
                    databaseName = databaseId,
                    databaseDescription = databaseId,
                    maxCores = parallel::detectCores() - 1,
                    maxCohortSize = 100000,
                    createCohorts = FALSE,
                    synthesizePositiveControls = FALSE,
                    createCohortMethodObjects = FALSE,
                    generateAnalysisObjects  = FALSE,
                    synthesizeAndExportResults = FALSE,
                    verbose = TRUE) {


  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "covBalanceLog.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE), add = TRUE)

  startTime <- Sys.time()
  if (verbose)
    ParallelLogger::logInfo(sprintf("Starting database %s at %s", databaseId, startTime))

  if (!file.exists(outputFolder)) {
    dir.create(outputFolder, recursive = TRUE, showWarnings = FALSE)
  }



  # create cohorts (exposure, outcome, negative controls)
  if (createCohorts && !file.exists(getCohortCountsFile(outputFolder))) {
    if (verbose)
      ParallelLogger::logInfo("Creating exposure, outcome, and negative control cohorts")
    createCohorts(connectionDetails = connectionDetails,
                  cdmDatabaseSchema = cdmDatabaseSchema,
                  cohortDatabaseSchema = cohortDatabaseSchema,
                  cohortTable = cohortTable,
                  outputFolder = outputFolder,
                  packageName = packageName)

  }


  # injection signal in negative controls
  if (synthesizePositiveControls) {
    if(verbose)
      ParallelLogger::logInfo("Synthesizing positive controls")
    synthesizePositiveControls(connectionDetails = connectionDetails,
                               cdmDatabaseSchema = cdmDatabaseSchema,
                               cohortDatabaseSchema = cohortDatabaseSchema,
                               cohortTable = cohortTable,
                               oracleTempSchema = oracleTempSchema,
                               outputFolder = outputFolder,
                               packageName = packageName,
                               maxCores = maxCores,
                               verbose = verbose)
  }

  # create cohortMethod data object and studyPop
  if(createCohortMethodObjects) {
    if(verbose)
      ParallelLogger::logInfo("Generating CohortMethod data")
    createCohortMethodObjects(connectionDetails = connectionDetails,
                              cdmDatabaseSchema = cdmDatabaseSchema,
                              cohortDatabaseSchema = cohortDatabaseSchema,
                              cohortTable = cohortTable,
                              oracleTempSchema = oracleTempSchema,
                              outputFolder = outputFolder,
                              packageName = packageName,
                              maxCores = maxCores,
                              createStudyPops = FALSE,
                              maxCohortSize = maxCohortSize,
                              serializeObjects = TRUE,
                              verbose = verbose)
  }




  # fit outcome models and compute balance
  if(generateAnalysisObjects) {
    if(verbose)
      ParallelLogger::logInfo("Fitting outcome models and computing balance")
    generateAnalysisObjects(cmOutputFolder = getCmFolderPath(outputFolder),
                    packageName = packageName,
                    maxCores = maxCores,
                    randomSeed = randomSeed)
  }

  if(synthesizeResults) {
    if(verbose)
      ParallelLogger::logInfo("Exporting results")
    synthesizeAndExportResults(resultsFolder = getResultsFolderPath(outputFolder = outputFolder),
                               cmOutputFolder = getCmFolderPath(outputFolder = outputFolder),
                               databaseId = databaseId,
                               maxCores = maxCores,
                               packageName = packageName)
  }



  endTime <- Sys.time()
  delta <- endTime - startTime
  if (verbose)
    ParallelLogger::logInfo(sprintf("Database %s took %f %s", databaseId, signif(delta, 3), attr(delta, "units")))


}
