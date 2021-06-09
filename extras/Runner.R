
if(length(ls()) > 0) {
  writeLines("********* Consider clearing environment before running... *********")
  # invisible(readline(prompt = "Press any key to continue."))
}
library(CovarBalDiagEval)

if(packageVersion("FeatureExtraction") < "3.1.1")
  stop("Need to update FeatureExtraction to >= 3.1.1!")

packageName <- "CovarBalDiagEval"
baseOutputFolder <- "d:/studies/CovarBalDiagEval"
randomSeed <- 123



cdmDatabaseSchema <- "cdm"
cohortTable <- "covimbdiag"
cdmVersion <- 5
maxCohortSize <- 100000

oracleTempSchema <- NULL



maxCores <- parallel::detectCores() - 1


optumDodConnectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  server = paste(keyring::key_get("OPTUM_DOD_URL"),
                 keyring::key_get("OPTUM_DOD_DATABASE"),
                 sep = "/"),
  user = keyring::key_get("REDSHIFT_USER"),
  password = keyring::key_get("REDSHIFT_PASSWORD"),
  port = keyring::key_get("OPTUM_DOD_PORT")
)

ibmCcaeConnectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  server = paste(keyring::key_get("IBM_CCAE_URL"),
                 keyring::key_get("IBM_CCAE_DATABASE"),
                 sep = "/"),
  user = keyring::key_get("REDSHIFT_USER"),
  password = keyring::key_get("REDSHIFT_PASSWORD"),
  port = keyring::key_get("IBM_CCAE_PORT")
)


optumPantherConnectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  server = paste(keyring::key_get("OPTUM_PANTHER_URL"),
                 keyring::key_get("OPTUM_PANTHER_DATABASE"),
                 sep = "/"),
  user = keyring::key_get("REDSHIFT_USER"),
  password = keyring::key_get("REDSHIFT_PASSWORD"),
  port = keyring::key_get("OPTUM_PANTHER_PORT")
)

ibmMdcrConnectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "redshift",
  server = paste(keyring::key_get("IBM_MDCR_URL"),
                 keyring::key_get("IBM_MDCR_DATABASE"),
                 sep = "/"),
  user = keyring::key_get("REDSHIFT_USER"),
  password = keyring::key_get("REDSHIFT_PASSWORD"),
  port = keyring::key_get("IBM_MDCR_PORT")
)


dbIds <- c("OPTUM_DOD", "IBM_CCAE", "OPTUM_PANTHER", "IBM_MDCR")
dbConnectionDetails <- list(optumDodConnectionDetails,
                         ibmCcaeConnectionDetails,
                         optumPantherConnectionDetails,
                         ibmMdcrConnectionDetails)
dbNames <- c(keyring::key_get("OPTUM_DOD_DB_NAME"),
             keyring::key_get("IBM_CCAE_DB_NAME"),
             keyring::key_get("OPTUM_PANTHER_DB_NAME"),
             keyring::key_get("IBM_MDCR_DB_NAME"))
dbDescriptions <- c(keyring::key_get("OPTUM_DOD_DB_DESCRIPTION"),
                    keyring::key_get("IBM_CCAE_DB_DESCRIPTION"),
                    keyring::key_get("OPTUM_PANTHER_DB_DESCRIPTION"),
                    keyring::key_get("IBM_MDCR_DB_DESCRIPTION"))
dbScratchSpaces <- c(keyring::key_get("OPTUM_DOD_SCRATCH_SCHEMA"),
                     keyring::key_get("IBM_CCAE_SCRATCH_SCHEMA"),
                     keyring::key_get("OPTUM_PANTHER_SCRATCH_SCHEMA"),
                     keyring::key_get("IBM_MDCR_SCRATCH_SCHEMA"))


# i <- 2
for (i in 1:length(dbIds)) {


  databaseId <- dbIds[[i]]

  outputFolder <- file.path(baseOutputFolder, databaseId)

  connectionDetails <- dbConnectionDetails[[i]]
  databaseName <- dbNames[[i]]
  databaseDescription <- dbDescriptions[[i]]
  cohortDatabaseSchema <- dbScratchSpaces[[i]]


  execute(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    outputFolder = outputFolder,
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    packageName = packageName,
    maxCores = maxCores,
    maxCohortSize = maxCohortSize,
    createCohorts = FALSE,
    synthesizePositiveControls = FALSE,
    createCohortMethodObjects = FALSE,
    generateAnalysisObjects = FALSE,
    synthesizeAndExportResults = FALSE,
    verbose = TRUE,
    randomSeed = randomSeed
  )

}


synthesizeResults(baseOutputFolder = baseOutputFolder,
                  packageName = packageName,
                  dbIds = dbIds)

