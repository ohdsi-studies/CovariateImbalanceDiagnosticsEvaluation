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


createCohorts <- function(connectionDetails,
                          cdmDatabaseSchema,
                          cohortDatabaseSchema,
                          cohortTable,
                          outputFolder,
                          packageName) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  ParallelLogger::logInfo("Creating base exposure cohorts")
  .createCohorts(connection = connection,
                 cdmDatabaseSchema = cdmDatabaseSchema,
                 cohortDatabaseSchema = cohortDatabaseSchema,
                 cohortTable = cohortTable,
                 outputFolder = outputFolder,
                 packageName = packageName)


  ParallelLogger::logInfo("Creating negative control outcome cohorts")
  negativeControls <- loadNegativeControls(packageName)
  sql <- SqlRender::loadRenderTranslateSql("NegativeControlOutcomes.sql",
                                           packageName = packageName,
                                           dbms = connectionDetails$dbms,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable,
                                           outcome_ids = unique(negativeControls$outcomeId))
  DatabaseConnector::executeSql(connection, sql)

  # Check number of subjects per cohort:
  ParallelLogger::logInfo("Counting cohorts")
  sql <- "SELECT cohort_definition_id,
    COUNT(*) AS entry_count,
    COUNT(DISTINCT subject_id) AS subject_count
  FROM @cohort_database_schema.@cohort_table
  GROUP BY cohort_definition_id"
  sql <- SqlRender::render(sql,
                           cohort_database_schema = cohortDatabaseSchema,
                           cohort_table = cohortTable)
  sql <- SqlRender::translate(sql, targetDialect = attr(connection, "dbms"))
  counts <- DatabaseConnector::querySql(connection, sql, snakeCaseToCamelCase = TRUE)
  # <- addCohortNames(counts, outputFolder = outputFolder)
  readr::write_csv(x = counts, file = getCohortCountsFile(outputFolder))

}




.createCohorts <- function(connection,
                           cdmDatabaseSchema,
                           vocabularyDatabaseSchema = cdmDatabaseSchema,
                           cohortDatabaseSchema,
                           cohortTable,
                           outputFolder,
                           packageName) {

  # Create study cohort table structure:
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateCohortTable.sql",
                                           packageName = packageName,
                                           dbms = connection@dbms,
                                           cohort_database_schema = cohortDatabaseSchema,
                                           cohort_table = cohortTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)



  # Instantiate cohorts:
  cohortsToCreate <- loadCohortsToCreate(packageName)
  for (i in 1:nrow(cohortsToCreate)) {
    ParallelLogger::logInfo(paste("Creating cohort:", cohortsToCreate$name[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate$name[i], ".sql"),
                                             packageName = packageName,
                                             dbms = connection@dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             vocabulary_database_schema = vocabularyDatabaseSchema,

                                             target_database_schema = cohortDatabaseSchema,
                                             target_cohort_table = cohortTable,
                                             target_cohort_id = cohortsToCreate$cohortId[i])
    DatabaseConnector::executeSql(connection, sql)
  }


}


addCohortNames <- function(data, IdColumnName = "cohortDefinitionId", nameColumnName = "cohortName", packageName) {
  pathToCsv <- system.file("settings", "CohortsToCreate.csv", package = packageName)
  cohortsToCreate <- read.csv(pathToCsv)
  pathToCsv <- system.file("settings", "NegativeControls.csv", package = packageName)
  negativeControls <- read.csv(pathToCsv)

  idToName <- data.frame(cohortId = c(cohortsToCreate$cohortId,
                                      negativeControls$outcomeId),
                         cohortName = c(as.character(cohortsToCreate$atlasName),
                                        as.character(negativeControls$outcomeName)))
  idToName <- idToName[order(idToName$cohortId), ]
  idToName <- idToName[!duplicated(idToName$cohortId), ]
  names(idToName)[1] <- IdColumnName
  names(idToName)[2] <- nameColumnName
  data <- merge(data, idToName, all.x = TRUE)
  # Change order of columns:
  idCol <- which(colnames(data) == IdColumnName)
  if (idCol < ncol(data) - 1) {
    data <- data[, c(1:idCol, ncol(data) , (idCol+1):(ncol(data)-1))]
  }
  return(data)
}
