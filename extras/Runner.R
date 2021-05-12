

library(CovarBalDiagEval)

if(packageVersion("FeatureExtraction") < "3.1.1")
  stop("Need to update FE!")

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


outFolders <- c(file.path(baseOutputFolder, "OPTUM_DOD"),
                file.path(baseOutputFolder, "IBM_CCAE"),
                file.path(baseOutputFolder, "OPTUM_PANTHER"),
                file.path(baseOutputFolder, "IBM_MDCR"))

i <- 1
for (i in 1:length(dbIds)) {


  databaseId <- dbIds[[i]]
  outputFolder <- file.path(baseOutputFolder, databaseId)

  connectionDetails <- dbConnectionDetails[[i]]
  databaseName <- dbNames[[i]]
  databaseDescription <- dbDescriptions[[i]]
  cohortDatabaseSchema <- dbScratchSpaces[[i]]

  # connectionDetails <-
  #   DatabaseConnector::createConnectionDetails(
  #     dbms = "redshift",
  #     server = paste(keyring::key_get(sprintf("%s_URL", db)),
  #                     keyring::key_get(sprintf("%s_DATABASE", db)),
  #                     sep = "/"),
  #     user = keyring::key_get("REDSHIFT_USER"),
  #     password = keyring::key_get("REDSHIFT_PASSWORD"),
  #     port = keyring::key_get(sprintf("%s_PORT", db))
  #   )
  #
  #
  # cohortDatabaseSchema <- keyring::key_get(sprintf("%s_SCRATCH_SCHEMA", db))
  # databaseId <- db
  # databaseName <- keyring::key_get(sprintf("%s_DB_NAME", db))
  # databaseDescription <- keyring::key_get(sprintf("%s_DB_DESCRIPTION", db))


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
    # samplePercentage = samplePercentage,
    createCohorts = FALSE,
    synthesizePositiveControls = FALSE,
    createCohortMethodObjects = TRUE,
    # partitionPop = TRUE,
    generateAnalysisObjects = TRUE,
    synthesizeResults = FALSE,
    verbose = TRUE,
    randomSeed = randomSeed
  )


}
